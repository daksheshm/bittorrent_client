from .filestate import FileState
from urllib.parse import urlparse, urlencode, ParseResult
from . import bencoder
from .lib import create_sock, Addr, AddrList
from typing import Literal
import socket
import struct
from random import randint
import logging

UDP_MAGIC = 0x41727101980
UDP_TIMEOUT_FACTOR = 15
UDP_CONNECT = 0
UDP_ANNOUNCE = 1
UDP_ERROR = 3

''' Handles relationship with the tracker '''
class RemoteTracker:
	url: str
	parse: ParseResult
	udp_sock: socket.socket
	udp_timeouts: int

	''' Request fields '''
	info_hash: bytes
	client_peer_id: bytes
	local_port: str
	fstate: FileState
	targ_peers: int

	''' Response fields '''
	interval: int						# Interval in seconds between regular requests
	peer_addrs: list[Addr]				# Dynamic set of peer addresses
	tracker_id: str

	completed: bool						# Whether or not we have sent a completed request already

	def __init__(self, 
			  url: str, 
			  info_hash: bytes, 
			  client_peer_id: bytes, 
			  local_port: int,
			  fstate: FileState,
			  targ_peers: int):
		self.url = url
		self.parse = urlparse(self.url)
		if self.parse.scheme == 'udp':
			self.udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
			self.udp_timeouts = 0
			self.udp_sock.settimeout(UDP_TIMEOUT_FACTOR)
		self.info_hash = info_hash
		self.client_peer_id = client_peer_id
		self.local_port = local_port
		self.fstate = fstate
		self.peer_addrs = []
		self.tracker_id = None
		self.targ_peers = targ_peers
		self.completed = False

	''' Send a request to the tracker and parse its response '''
	def request(self,
			 event: Literal['started', 'completed', 'stopped'] | None = None, 
			 compact: bool = True, 
			 no_peer_id: bool = True):
		if self.parse.scheme == 'udp':
			self.request_udp(event)
		elif self.parse.scheme == 'http':
			self.request_http(event, compact, no_peer_id)
		if event == 'completed':
			self.completed = True

	def request_http(self,
			 event: Literal['started', 'completed', 'stopped'] | None = None, 
			 compact: bool = True, 
			 no_peer_id: bool = True):
		addr = (self.parse.hostname, self.parse.port)
		sock = create_sock(addr, 10)
		if not sock:
			raise Exception('Failed to connect to tracker')

		query = { 
			'info_hash': self.info_hash,
			'peer_id': self.client_peer_id,
			'port': self.local_port,
			'uploaded': self.fstate.uploaded,
			'downloaded': self.fstate.downloaded,
			'left': self.fstate.left,
			'compact': int(compact),
			'no_peer_id': int(no_peer_id),
			'numwant': self.targ_peers}
		if event:
			query['event'] = event
		elif self.tracker_id:
			query['trackerid'] = self.tracker_id
		query_encoded = urlencode(query)
		path = self.parse.path if not self.parse.path == '' else '/'
		req_target = path + '?' + query_encoded
		req = f'GET {req_target} HTTP/1.1\r\n\r\n'

		sock.send(req.encode())
		raw_resp = sock.recv(10000)
		sock.close()
		if not event == 'stopped':
			try:
				split_resp = raw_resp.split(b'\r\n')
				bdecoded_resp = bencoder.decode(split_resp[-1])
			except:
				logging.debug(split_resp)
				raise Exception('Unexpected response format from tracker')
			if not b'peers' in bdecoded_resp:
				raise Exception('Peers not included in tracker response')
			if not b'interval' in bdecoded_resp:
				raise Exception('Interval not included in tracker response')
			self.interval = bdecoded_resp[b'interval']
			pb = bdecoded_resp[b'peers']
			self.peer_addrs = RemoteTracker.parse_peerbytes(pb)

	def request_udp(self,
			 event: Literal['started', 'completed', 'stopped'] | None = None):
		addr = (self.parse.hostname, self.parse.port)
		transaction_id = randint(0, 2**32 - 1)
		connect_req = struct.pack('!QII', UDP_MAGIC, UDP_CONNECT, transaction_id)
		self.udp_sock.sendto(connect_req, addr)

		resp, addr = self.recv_udp()
		while resp == None and addr == None:
			resp, addr = self.recv_udp()
		if not len(resp) >= 16:
			raise Exception('Unexpected response from tracker')
		action, transaction_id_resp, connection_id = struct.unpack("!IIQ", resp[:16])
		if not action == UDP_CONNECT or not transaction_id_resp == transaction_id:
			raise Exception('Unexpected response from tracker')
		
		announce_req = struct.pack('!QII', connection_id, UDP_ANNOUNCE, transaction_id)
		announce_req += self.info_hash
		announce_req += self.client_peer_id
		event_int = 0
		if event == 'completed':
			event_int = 1
		elif event == 'started':
			event_int = 2
		elif event == 'stopped':
			event_int = 3
		announce_req += struct.pack('!QQQIIIIH', 
							  self.fstate.downloaded, 
							  self.fstate.left, 
							  self.fstate.uploaded, 
							  event_int, 0, 0,  # IP address and key
							  self.targ_peers, 
							  self.local_port)
		self.udp_sock.sendto(announce_req, addr)

		resp, addr = self.recv_udp()
		while resp == None and addr == None:
			resp, addr = self.recv_udp()
		if not len(resp) >= 20:
			raise Exception('Unexpected response from tracker')
		action, transaction_id_resp, interval, leechers, seeders = struct.unpack('!IIIII', resp[:20])
		if not action == UDP_ANNOUNCE or not transaction_id_resp == transaction_id:
			raise Exception('Unexpected response from tracker')
		
		self.peer_addrs = RemoteTracker.parse_peerbytes(resp[20:])
		self.interval = interval
		

	def recv_udp(self) -> tuple[bytes, Addr]:
		try:
			data, addr = self.udp_sock.recvfrom(1000)
			return (data, addr)
		except socket.timeout:
			if self.udp_timeouts < 8:
				self.udp_timeouts += 1
			self.udp_sock.settimeout(UDP_TIMEOUT_FACTOR*(2**self.udp_timeouts))
			return (None, None)
	
	def parse_peerbytes(peerbytes: bytes) -> AddrList:
		addrs = []
		for i in range(0, len(peerbytes), 6):
			p = peerbytes[i:i+6]
			addr = f'{p[0]}.{p[1]}.{p[2]}.{p[3]}'
			port = int.from_bytes(p[4:], 'big', signed=False)
			addrs.append((addr, port))
		return addrs
		
	def send_init_req(self):
		self.request('started')

	def send_stop_req(self):
		try:
			self.request('stopped', numwant=0)
		except:
			logging.error('Failure sending final request to tracker')

	def sent_completed(self):
		return self.completed

	def get_peer_addrs(self) -> list[Addr]:
		return self.peer_addrs
	
	def get_interval(self) -> int:
		return self.interval

