from .torrentfile import TorrentFile
from .filestate import FileState
from .tracker import RemoteTracker
from .peers import PeerConnections, MIN_CONNECTED_PEERS
from socket import socket
from os.path import join
from random import choices
from string import ascii_letters, digits
import socket
from select import select
import threading	
from time import sleep
import logging
from concurrent.futures import ThreadPoolExecutor

class TorrentClient:
	tfile: TorrentFile
	fstate: FileState
	tracker: RemoteTracker
	conns: PeerConnections

	peer_id: bytes
	local_port: int
	local_sock: socket.socket

	stop_event: threading.Event

	def __init__(self, port: int):
		self.create_peer_id()
		self.local_port = port
		self.open_sock()
		self.stop_event = threading.Event()

	''' 
		Create 20-byte peer id.
		"Azureus-style uses the following encoding: '-', two 
		characters for client id, four ascii digits for version 
		number, '-', followed by random numbers."
	'''
	def create_peer_id(self):
		logging.debug("Creating peer id")
		prefix = '-ZZ0000-'
		suffix = ''.join(choices(ascii_letters + digits, k=12))
		self.peer_id = (prefix + suffix).encode()
		assert(len(self.peer_id) == 20)

	def open_sock(self):
		logging.debug(f"Opening client socket on port {self.local_port}")
		self.local_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		self.local_sock.bind(('127.0.0.1', self.local_port))
		self.local_sock.listen()
	
	def open_tfile(self, path):
		logging.debug("Parsing torrent file")
		self.tfile = TorrentFile(path)
		self.tfile.parse_tfile()

	def init_filestate(self, output_dir: str):
		self.fstate = self.tfile.to_filestate(output_dir)
		logging.debug("Opening output file(s)")
		self.fstate.open()

	def begin_torrent(self, targ_peers: int):
		self.tracker = RemoteTracker(self.tfile.announce_url, 
						 self.tfile.info_hash, 
						 self.peer_id, 
						 self.local_port, 
						 self.fstate,
						 targ_peers)
		logging.debug('Sending initial announce to tracker')
		try:
			self.tracker.send_init_req()
		except Exception as e:
			logging.error('Failed to initiate communication with the tracker:', e)
			exit(-1)
		self.conns = PeerConnections(self.fstate, self.tfile.info_hash, self.peer_id)
		init_peers = self.tracker.get_peer_addrs()
		logging.debug(f'Connecting to initial {len(init_peers)} peers')
		self.conns.bulk_add_conn(init_peers)
		#self.conns.bulk_add_conn([('127.0.0.1', 6881)])
		self.begin_loops()

	def begin_loops(self):
		self.begin_tracker_loop(self.stop_event)
		self.conns.begin_keepalive_loop(self.stop_event)
		self.conns.begin_choke_loop(self.stop_event)
		self.begin_socket_loop(self.stop_event)

	def begin_socket_loop(self, stop_event: threading.Event):
		logging.info('Beginning socket loop')
		while not stop_event.is_set():
			socks = [self.local_sock, *self.conns.get_all_socks()]
			ready_socks, _, _ = select(socks, [], [])

			def handle_sock(sock):
				if sock == self.local_sock:
					self.handle_local_read()
				else:
					self.conns.handle_remote_read(sock)

			for sock in ready_socks:
				handle_sock(sock)
			#with ThreadPoolExecutor() as executor:
			#	futures = [executor.submit(handle_sock, sock) for sock in ready_socks]
				# No need to wait for futures to complete, they are all handled in the loop
	
	def tracker_loop(self, stop_event: threading.Event):
		while not stop_event.is_set():
			sleep(self.tracker.get_interval())
			try:
				if self.fstate.is_finished() and not self.tracker.sent_completed():
					logging.debug('Sending completed message to the tracker')
					self.tracker.request('completed')
				else:
					self.tracker.request()
				if self.conns.num_conn_peers() < MIN_CONNECTED_PEERS and not self.fstate.is_finished():
					new_peers = self.tracker.get_peer_addrs()
					logging.info(f'Connecting to another {len(new_peers)} peers')
					self.conns.bulk_add_conn(new_peers)
			except Exception as e:
				logging.error('Failed regular tracker request:', e)
				# self.end_torrent()

	def begin_tracker_loop(self, stop_event: threading.Event):
		thread = threading.Thread(target=self.tracker_loop, args=[stop_event,])
		thread.start()

	def handle_local_read(self):
		logging.debug('local read')
		peer_sock, _ = self.local_sock.accept()
		self.conns.accept_conn(peer_sock)
	
	def end_torrent(self):
		logging.info('\nEnding torrent')
		self.tracker.send_stop_req()
		logging.info('Closing sockets')
		self.local_sock.close()
		self.conns.sever_all_conns()
		logging.info('Closing file descriptor(s)')
		self.fstate.close()
		logging.info('Stopping threads')
		self.stop_event.set()

	def init_tracker(self):
		pass

'''
	Thread that loops tracker requests
	loop that checks all sockets:
		tracker socket
		client socket for new connections
		current socket connections
	Do we occasionaly send messages to current socket connections unprompted?
'''