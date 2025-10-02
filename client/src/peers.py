from .filestate import FileState
import socket
from math import ceil
from .lib import *
import threading
from time import time, sleep
from .bitfield import BitField
from random import choice, choices
from .request import Request
import logging
from typing import Literal
from concurrent.futures import ThreadPoolExecutor, as_completed

MAX_ASSIGNED_REQS = 50
MIN_CONNECTED_PEERS = 25

KEEPALIVE_RATE = 30.0
KEEPALIVE_TIMEOUT = 120.0
REQ_TIMEOUT = 30.0
CHOKE_RATE = 5.0
OPT_UNCHOKE_RATE = 10.0

ENDGAME_THRESHOLD = 0.9

UNSPECIFIED = 'Unspecified'
READ_FAILED = 'Read failed'
CONN_CLOSED = 'Connection closed'
SEND_FAILED = 'Send failed'
UNEXPECTED = 'Unexpected message from peer'
TIMEOUT = 'Peer timed out'

ConnectionType = Literal['inbound', 'outbound']

''' Represents a peer connection '''
class PeerConnection:
	addr: Addr
	peer_id: bytes
	sock: socket.socket
	sock_lock: threading.Lock
	hs_status: int 				# 0 if no handshakes have been sent, 1 if one, 2 if handshake is complete
	conn_type: ConnectionType
	alive: bool
	
	am_choking: bool
	am_interested: bool
	peer_choking: bool
	peer_interested: bool

	bitfield: BitField		# Pieces that the peer has

	request: Request | None

	''' Fields for download and upload rates '''
	dl_rates: list[float]
	dl_rate: float | None		# Average download rate from this peer
	unchoke_stamp: float		# Timestamp of the last time we unchoked this peer
	ul_bytes: int				# Total number of bytes we've uploaded to this peer
	unchoke_time: float			# Total amount of time we've had this peer unchoked
	last_msg_stamp: float

	def __init__(self, addr: Addr, sock: socket.socket, num_pieces: int, conn_type: ConnectionType):
		self.addr = addr
		self.sock = sock
		self.sock_lock = threading.Lock()
		self.peer_id = None
		self.hs_status = 0
		self.conn_type = conn_type
		self.alive = True

		self.am_choking = True
		self.am_interested = False
		self.peer_choking = True
		self.peer_interested = False

		self.bitfield = BitField(num_pieces)

		self.request = None

		self.dl_rates = []
		self.dl_rate = 0				# Avg download rate in pieces / second
		self.unchoke_stamp = 0			# Timestamp of when we last unchoked this peer
		self.ul_bytes = 0				# Total number of bytes uploaded to this peer
		self.unchoke_time = 0			# Total amount of time this peer has been unchoked
		self.last_msg_stamp = time()

	def send(self, msg: bytes) -> int:
		with self.sock_lock:
			try:
				self.sock.send(msg)
				return 0
			except:
				return -1

	def send_handshake(self, info_hash: bytes, client_peer_id: bytes) -> int:
		pstrlen = int.to_bytes(19, 1, byteorder='big')
		pstr = 'BitTorrent protocol'.encode()
		reserved = bytes(8)
		handshake = pstrlen + pstr + reserved + info_hash + client_peer_id
		assert len(handshake) == HANDSHAKE_LEN
		self.hs_status += 1
		return self.send(handshake)

	def handle_handshake(self, peer_id: bytes, info_hash: bytes, client_peer_id: bytes):
		self.peer_id = peer_id
		self.hs_status += 1
		if self.hs_status < 2:
			return self.send_handshake(info_hash, client_peer_id)
		return 0
	
	def send_keepalive(self):
		return self.send(bytes(LEN_LEN))
	
	def send_have(self, piece: int):
		return self.send(build_msg(HAVE, [int.to_bytes(piece, LEN_LEN, 'big')]))
	
	def send_bitfield(self, bitfield: BitField) -> int:
		msg = build_msg(BITFIELD, [bitfield.to_bytes()])
		return self.send(msg)

	def set_bitfield(self, bitfield_bytes: bytes):
		if not len(bitfield_bytes) == self.bitfield.len_bytes():
			return -1
		else:
			self.bitfield = BitField(self.bitfield.len_bits(), bitfield_bytes)
			return 0
		
	def update_bitfield(self, piece: int):
		self.bitfield.set_bit(piece)

	def clear_bitfield(self):
		self.bitfield.clear()

	def get_bitfield(self) -> BitField:
		return self.bitfield
	
	def self_interested(self) -> int:
		if not self.am_interested:
			self.am_interested = True
			return self.send(build_msg(INTERESTED))
		return 0
	
	def assign_request(self, piece: int, num_pieces: int, piece_len: int, flen: int) -> int:
		if self.has_assigned_req():
			return -1
		else:
			self.request = Request(piece, num_pieces, piece_len, flen)
			status = self.self_interested()
			if not status == 0: return status
			status = self.dispatch_queued_req()
			return status

	def dispatch_queued_req(self) -> int:
		if not self.peer_choking and self.has_assigned_req() and not self.has_outstanding_req():
			logging.debug(f'Dispatching requests for piece {self.request.get_piece()} for {self.to_str()}')
			reqs = self.request.build_reqs()
			for req in reqs:
				status = self.send(req)
				if not status == 0: return status
			self.request.set_outstanding()
		return 0
	
	def unchoke_self(self):
		self.peer_choking = False
		return self.dispatch_queued_req()
	
	def choke_self(self):
		self.peer_choking = True
		if self.has_assigned_req():
			self.request.cancel_outstanding()

	def choke(self):
		if not self.am_choking:
			logging.debug(f'Choking peer {self.to_str()}')
			self.am_choking = True
			self.unchoke_time += time() - self.unchoke_stamp
			self.send(build_msg(CHOKE))

	def unchoke(self):
		if self.am_choking:
			logging.debug(f'Unchoking peer {self.to_str()}')
			self.am_choking = False
			self.unchoke_stamp = time()
			self.send(build_msg(UNCHOKE))

	def handle_block_resp(self, piece: int, offset: int, data: bytes):
		if self.has_outstanding_req():
			if not piece == self.request.get_piece():
				#logging.error(f'bad piece, expected {self.request.get_piece()}, got {piece} on {self.to_str()}')
				return
			self.request.add_to_buffer(offset, data)

	def finish_request(self) -> bytes:
		if self.has_outstanding_req() and self.finished_request():
			buffer = self.request.get_buffer()
			# TODO: Calculate and update download rate
			dl_time = time() - self.request.get_send_stamp()
			self.update_dl_rate(dl_time)
			self.request = None
			return buffer
		raise Exception('No outstanding request')
	
	def cancel_request(self):
		if self.has_outstanding_req():
			reqs = self.request.build_cancel_reqs()
			for req in reqs:
				self.send(req)
		if self.has_assigned_req():
			self.request = None
		else:
			raise Exception('No assigned request')
		
	def send_block(self, piece: int, offset: int, data: bytes):
		msg = build_msg(PIECE, [int.to_bytes(piece, LEN_LEN, 'big'),
						  int.to_bytes(offset, LEN_LEN, 'big'),
						  data])
		return self.send(msg)
		
	def update_dl_rate(self, dl_time: float):
		rate = 1/dl_time
		self.dl_rates.append(rate)
		self.dl_rate = sum(self.dl_rates) / len(self.dl_rates)

	def get_dl_rate(self) -> float:
		return self.dl_rate
	
	def get_ul_rate(self) -> float:
		if self.unchoke_time == 0 and self.am_choking:
			return 0
		unchoke_time = self.unchoke_time
		if not self.am_choking:
			unchoke_time += time() - self.unchoke_stamp
		return self.ul_bytes / unchoke_time

	def refresh(self):
		self.last_msg_stamp = time()
		
	def kill(self):
		self.close_sock()
		self.choke_self()
		self.alive = False
		
	def close_sock(self) -> socket.socket:
		try:
			self.sock.close()
		except:
			pass
		self.sock = None

	def is_alive(self) -> bool:
		return self.alive
	
	def is_choking(self) -> bool:
		return self.peer_choking
	
	def is_interested(self) -> bool:
		return self.peer_interested
	
	def has_piece(self, piece: int) -> bool:
		return self.bitfield.is_set(piece)
	
	def has_assigned_req(self) -> bool:
		return not self.request == None
	
	def has_outstanding_req(self) -> bool:
		return not self.request == None and self.request.is_outstanding()
	
	def finished_request(self) -> bool:
		if not self.has_assigned_req():
			raise Exception('No assigned request')
		return self.has_outstanding_req() and self.request.is_complete()

	def get_sock(self) -> socket.socket:
		return self.sock
	
	def get_peer_id(self) -> bytes | None:
		return self.peer_id
	
	def get_addr(self) -> Addr:
		return self.addr
	
	def get_hs_status(self) -> int:
		return self.hs_status
	
	def get_last_msg_stamp(self) -> float:
		return self.last_msg_stamp
	
	def get_request_stamp(self) -> float:
		if self.has_outstanding_req():
			return self.request.get_send_stamp()
		raise Exception('No outstanding request')
	
	def get_assigned_request(self) -> int:
		if self.has_assigned_req():
			return self.request.get_piece()
		raise Exception('No assigned request')
	
	def get_outstanding_request(self) -> int:
		if self.has_outstanding_req():
			return self.request.get_piece()
		raise Exception('No outstanding request')
	
	def to_str(self) -> str:
		return f'Addr: {self.addr}, Id: {self.peer_id}'

''' Handles all peer connections '''
class PeerConnections:
	peers: dict[socket.socket, PeerConnection]	# Maps sockets to a peer connection
	conn_addrs: set[Addr]						# Set of addrs we are connected to. Helpful for identifying repeat connections
	conn_ids: set[bytes]						# Set of peer ids we are connected to. Helpful for identifying repeat connections
	peers_lock: threading.Lock					# Used when changing peer sets / dictionaries

	fstate: FileState
	bitfield_sum: bytearray						# Cumulative bitfield. Each index holds the total num of clients that have that piece
	assigned_reqs: set[int]							# Set of pieces that are either outstanding or queued among all peers
	req_lock : threading.Lock					# Used when changing all_out_reqs

	''' Static fields needed for each handshake '''
	info_hash: bytes
	client_peer_id: bytes

	''' Choking fields '''
	recent_conns: set[socket.socket]			# Set of recently connected peers. Used for optimistic unchoking
	downloaders: set[PeerConnection]
	unchoked: set[PeerConnection]
	opt_unchoke: PeerConnection | None
	choke_lock: threading.Lock

	def __init__(self, fstate: FileState, info_hash: bytes, client_peer_id: bytes):
		self.peers = {}
		self.conn_addrs = set()
		self.conn_ids = set()
		self.peers_lock = threading.Lock()

		self.fstate = fstate
		self.bitfield_sum = bytearray(self.fstate.num_pieces)
		self.assigned_reqs = set()
		self.req_lock = threading.Lock()

		self.info_hash = info_hash
		self.client_peer_id = client_peer_id

		self.recent_conns = set()
		self.downloaders = set()
		self.unchoked = set()
		self.opt_unchoke = None
		self.choke_lock = threading.Lock()


	''' Start a connection with a new peer and send handshake '''
	def add_conn(self, addr: Addr) -> int:
		if not addr in self.conn_addrs:
			sock = create_sock(addr)
			if sock:
				sock.setblocking(False)
				peer = PeerConnection(addr, sock, self.fstate.num_pieces, 'outbound')
				logging.debug(f'Added outbound connection to {peer.to_str()}')
				self.peers_lock.acquire()
				self.peers[sock] = peer
				self.conn_addrs.add(addr)
				self.recent_conns.add(sock)
				self.peers_lock.release()
				status = peer.send_handshake(self.info_hash, self.client_peer_id)
				if not status == 0: self.sever_conn(peer, SEND_FAILED)
				return status
			else:
				return -1

	def bulk_add_conn(self, addrs: AddrList):
		self.recent_conns.clear()
		num_added = 0

		def add_connection(addr: Addr):
			if self.add_conn(addr) == 0:
				return 1
			return 0

		# Add connections in parallel
		with ThreadPoolExecutor() as executor:
			futures = {executor.submit(add_connection, addr): addr for addr in addrs}
			for future in as_completed(futures):
				try:
					result = future.result()
					num_added += result
				except Exception as e:
					logging.error(f'Failed to add outgoing connection to {futures[future]}: {e}')
		logging.info(f'Connected to {num_added} peers')

	''' Add a new peer connection after a peer has initiated with us '''
	def accept_conn(self, peer_sock: socket.socket):
		addr = peer_sock.getpeername()
		if not addr in self.conn_addrs:
			peer_sock.setblocking(False)
			peer = PeerConnection(addr, peer_sock, self.fstate.num_pieces, 'inbound')
			logging.debug(f'Accepted inbound connection to {peer.to_str()}')
			self.peers_lock.acquire()
			self.peers[peer_sock] = peer
			self.conn_addrs.add(addr)
			self.recent_conns.add(peer_sock)
			self.peers_lock.release()

	def sever_conn(self, peer: PeerConnection, reason: str = UNSPECIFIED):
		logging.warn(f'Severing connection with {peer.to_str()}. Reason: {reason}. {len(self.peers)-1} connections remaining')
		self.peers_lock.acquire()
		if peer.is_alive():
			peer_id = peer.get_peer_id()
			addr = peer.get_addr()
			sock = peer.get_sock()
			if peer_id and peer_id in self.conn_ids:
				try:
					self.conn_ids.remove(peer_id)
				except:
					pass
			if addr in self.conn_addrs:
				try:
					self.conn_addrs.remove(addr)
				except:
					pass
			if sock in self.peers:
				try:
					del self.peers[peer.get_sock()]
				except:
					pass
			if sock in self.recent_conns:
				try:
					self.recent_conns.remove(sock)
				except:
					pass
			self.clear_peer_bitfield(peer)
			self.req_lock.acquire()
			self.cancel_peer_request(peer)
			self.req_lock.release()
			peer.kill()
		self.peers_lock.release()

	def sever_all_conns(self):
		# will change this later
		for conn in self.peers.values():
			conn.sock.close()

	def handle_handshake(self, peer: PeerConnection, peer_sock: socket.socket):
		"""Handles the handshake from the peer **if applicable**. Returns 1 if the 
		handshake is still in progress, 0 if it is complete, and -1 if the 
		connection has been severed."""
		if peer.get_hs_status() >= 2:
			return 0

		try:
			handshake = peer_sock.recv(HANDSHAKE_LEN)
		except:
			self.sever_conn(peer, READ_FAILED)
			return -1

		if HANDSHAKE_LEN >= len(handshake) > 0:
			self.handle_handshake_msg(peer, handshake)
			return 1
		else:
			self.sever_conn(peer, CONN_CLOSED)
			return -1

	''' Implement a number of request and response handlers '''
	def handle_remote_read(self, peer_sock: socket.socket):
		peer = self.peers.get(peer_sock)
		if not peer or not peer.is_alive():
			# Connection has been severed by another thread before we got here
			# Bad idea to read from the socket
			return  

		if self.handle_handshake(peer, peer_sock) != 0:
			# Handshake is either in progress or has been severed
			return

		# Handshake is complete and this is a regular message from the peer

		try:
			len_bytes = peer_sock.recv(LEN_LEN, socket.MSG_PEEK)
		except:
			self.sever_conn(peer, READ_FAILED)
			return
		if len(len_bytes) == 4:
			msg_len = int.from_bytes(len_bytes, 'big')
			msg = b''
			if msg_len > 0:
				try:
					# TODO: Maybe instead of discarding, we can flag that this socket has len(msg) bytes already read so that we can read the rest of the message in the next iteration
					# Large messages may take a while to come in
					msg = peer_sock.recv(LEN_LEN + msg_len, socket.MSG_PEEK)[LEN_LEN:]
					if len(msg) < msg_len:
						return  # Come back later when all the data is ready
					else:
						peer_sock.recv(LEN_LEN + msg_len)  # Flush the socket buffer
				except BlockingIOError:
					return  # Come back later when all the data is ready
				except Exception as e:
					logging.error("Error reading message from peer:", e)
					self.sever_conn(peer, READ_FAILED)
					return
			self.handle_regular_msg(peer, msg)
		elif len(len_bytes) > 0:
			return
		else:
			self.sever_conn(peer, CONN_CLOSED)

	def handle_handshake_msg(self, peer: PeerConnection, handshake: bytes):
		ptstrlen = int.from_bytes(handshake[:1], 'big')
		try:
			pstr = handshake[1:1+ptstrlen].decode()
		except:
			self.sever_conn(peer, 'Peer did not send the right pstr')
			return
		info_hash = handshake[1+ptstrlen+8: 1+ptstrlen+8+20]
		peer_id = handshake[1+ptstrlen+8+20:]
		if not ptstrlen == 19 or not pstr == 'BitTorrent protocol':
			self.sever_conn(peer, 'Peer did not send the right pstr')
		if not info_hash == self.info_hash:
			self.sever_conn(peer, 'Peer does not have matching info hash')
		if peer_id in self.conn_ids:
			self.sever_conn(peer, 'We are already connected to this peer')
		self.conn_ids.add(peer_id)
		status = peer.handle_handshake(peer_id, self.info_hash, self.client_peer_id)
		if not status == 0:
			self.sever_conn(peer, SEND_FAILED)
			return
		logging.debug(f'Handshake completed with {peer.to_str()}. Sending bitfield...')
		if peer.conn_type == 'outbound':
			# We know the handshake is done at this point, so send our bitfield
			status = peer.send_bitfield(self.fstate.get_bitfield())
			if not status == 0:
				self.sever_conn(peer, SEND_FAILED)
		else:
			# We are inbound, so we wait for the peer to send their bitfield
			pass

	def handle_regular_msg(self, peer: PeerConnection, msg: bytes):
		peer.refresh()
		if len(msg) == 0:
			return  # Keepalive message
		id = int.from_bytes(msg[:1], 'big')
		payload = msg[ID_LEN:]
		status = 0
		if id == CHOKE:
			self.handle_choke_self(peer)
		elif id == UNCHOKE:
			self.handle_unchoke_self(peer)
		elif id == INTERESTED:
			self.handle_interested_peer(peer)
		elif id == UNINTERESTED:
			self.handle_uninterested_peer(peer)
		elif id == HAVE:
			piece = int.from_bytes(payload, 'big')
			self.handle_have_msg(peer, piece)
		elif id == BITFIELD:
			self.handle_bitfield_msg(peer, payload)
		elif id == REQUEST:
			self.handle_peer_request(peer, payload)
		elif id == PIECE:
			piece = int.from_bytes(payload[:LEN_LEN], 'big')
			block = int.from_bytes(payload[LEN_LEN:LEN_LEN*2], 'big')
			data = payload[LEN_LEN*2:]
			self.handle_block_resp(peer, piece, block, data)
		elif id == CANCEL:
			pass

	def handle_bitfield_msg(self, peer: PeerConnection, new_bitfield: bytes):
		'''
			The connection could have possibly been severed by another thread before we get here.
			If we add to the total bitfield after the connection has been severed,
			those new values won't ever be removed
		'''
		if peer.conn_type == 'outbound':
			self.peers_lock.acquire()
			if peer.is_alive():
				logging.debug(f'Received bitfield from {peer.to_str()}')
				self.clear_peer_bitfield(peer)
				status = peer.set_bitfield(new_bitfield)
				if not status == 0:
					self.peers_lock.release()
					self.sever_conn(peer, 'Unexpected bitfield length')
					return
				peer.get_bitfield().add_to(self.bitfield_sum)
				self.peers_lock.release()
				self.request_pieces()
			else:
				self.peers_lock.release()
		else:
			# We are inbound, so we have to send our bitfield back
			self.peers_lock.acquire()
			if peer.is_alive():
				logging.debug(f'Receiving bitfield from {peer.to_str()}')
				logging.debug(f'Sending back bitfield to {peer.to_str()}')
				if not peer.set_bitfield(new_bitfield) == 0:
					self.peers_lock.release()
					self.sever_conn(peer, 'Unexpected bitfield length')
					return
				if not peer.send_bitfield(self.fstate.get_bitfield()) == 0:
					self.peers_lock.release()
					self.sever_conn(peer, SEND_FAILED)
					return
			self.peers_lock.release()

	def handle_have_msg(self, peer: PeerConnection, piece: int):
		''' Same rules as handle_bitfield_msg '''
		self.peers_lock.acquire()
		if peer.is_alive():
			logging.debug(f'Received have message from {peer.to_str()}')
			if piece < self.fstate.num_pieces:
				peer.update_bitfield(piece)
				self.bitfield_sum[piece] += peer.get_bitfield().get_bit(piece)
				self.peers_lock.release()
				self.request_pieces()
			else:
				self.peers_lock.release()
				self.sever_conn(peer, 'Piece index out of bounds')
		else:
			self.peers_lock.release()

	''' Must be called with peer_lock acquired '''
	def clear_peer_bitfield(self, peer: PeerConnection):
		peer.get_bitfield().sub_from(self.bitfield_sum)
		peer.clear_bitfield()

	def handle_choke_self(self, peer: PeerConnection):
		logging.debug(f'Choking self on {peer.to_str()}')
		peer.choke_self()

	def handle_unchoke_self(self, peer: PeerConnection):
		logging.debug(f'Unchoking self from {peer.to_str()}')
		status = peer.unchoke_self()
		if not status == 0:
			self.sever_conn(peer, SEND_FAILED)
			return
		self.request_pieces()

	def handle_interested_peer(self, peer: PeerConnection):
		logging.debug(f"{peer.to_str()} is interested")
		"""
		PSUEODOCODE
		Set a flag on the peer that they are interested
		"""
		logging.debug(f'{peer.to_str()} is interested')
		peer.peer_interested = True
		self.choke_lock.acquire()
		if peer in self.unchoked and not peer in self.downloaders:
			if len(self.downloaders) == 4:
				self.choke_slowest_downloader()
			self.downloaders.add(peer)
		self.choke_lock.release()
		# TODO: Implement some mechanism to determine if we should unchoke the peer
		#peer.am_choking = False
		#if not peer.send(build_msg(UNCHOKE)) == 0:
		#	logging.error("Failed to unchoke peer")
		#	self.sever_conn(peer, SEND_FAILED)
		#	return
		# logging.debug(f'Unchoked peer {peer.to_str()}')

	def handle_uninterested_peer(self, peer: PeerConnection):
		logging.debug(f"{peer.to_str()} is uninterested")
		peer.peer_interested = False

	def handle_peer_request(self, peer: PeerConnection, payload: bytes):
		"""
		request: <len=0013><id=6><index><begin><length>
			The request message is fixed length, and is used to request a block. The payload contains the following information:

			index: integer specifying the zero-based piece index
			begin: integer specifying the zero-based byte offset within the piece
			length: integer specifying the requested length.
		"""
		logging.debug(f"Received request from {peer.to_str()}")
		if not peer.is_interested() or peer.am_choking:
			logging.debug(f'Peer {peer.to_str()} is either not interested or we are choking them. Ignoring request')
			return
		piece = int.from_bytes(payload[:LEN_LEN], 'big')
		offset = int.from_bytes(payload[LEN_LEN:LEN_LEN*2], 'big')
		length = int.from_bytes(payload[LEN_LEN*2:], 'big')
		logging.debug(f"request: <len=0013><id=6><index={piece}><begin={offset}><length={length}>")

		if piece >= self.fstate.num_pieces:
			logging.debug(f'Piece index out of range')
			return
		if piece * self.fstate.piece_len + offset + length > self.fstate.flen:
			logging.debug(f'Block out of range')
			return
		if not self.fstate.has(piece):
			logging.debug(f'We do not have this piece')
			return
		piece_data = self.fstate.read(piece, offset, length)
		status = peer.send_block(piece, offset, piece_data)
		if not status == 0:
			self.sever_conn(peer, SEND_FAILED)


	def handle_block_resp(self, peer: PeerConnection, piece: int, block: int, data: bytes):
		self.req_lock.acquire()
		if peer.has_outstanding_req():
			peer.handle_block_resp(piece, block, data)
			if peer.finished_request():
				logging.debug(f'Piece {piece} finished on {peer.to_str()}')
				status = self.finish_peer_request(peer)
				self.req_lock.release()
				if status == 0: self.send_have_msgs(piece)
				self.request_pieces()
				return
		self.req_lock.release()

	''' Must be called with req_lock acquired '''
	def finish_peer_request(self, peer: PeerConnection) -> int:
		assert peer.finished_request()
		piece = peer.get_assigned_request()
		piece_data = peer.finish_request()
		if piece in self.assigned_reqs:
			self.assigned_reqs.remove(piece)
		if self.fstate.get_progress() > ENDGAME_THRESHOLD:
			peers = list(self.peers.values())
			for p in peers:
				self.cancel_peer_request(p)
		return self.fstate.write(piece, piece_data)
	
	def send_have_msgs(self, piece: int):
		candidates = self.get_all_peers_without(piece)
		for peer in candidates:        
			peer.send_have(piece)

	''' Must be called with req_lock acquired '''
	def cancel_peer_request(self, peer: PeerConnection):
		if peer.has_assigned_req():
			piece = peer.get_assigned_request()
			if piece in self.assigned_reqs:
				self.assigned_reqs.remove(piece)
			peer.cancel_request()

	def request_pieces(self):
		if self.fstate.is_finished():
			return
		num_to_req = MAX_ASSIGNED_REQS - len(self.assigned_reqs)
		k = num_to_req * 2
		ordered_bitfield = sorted(enumerate(self.bitfield_sum), key = lambda x: x[1])  # Slow
		pruned_bitfield = [p for p in ordered_bitfield if not self.fstate.has(p[0])]  # Slow
		i = 0
		while i < len(pruned_bitfield) and pruned_bitfield[i][1] == 0:
			i += 1
		smallest_k = pruned_bitfield[i:i+k]
		if self.fstate.get_progress() < ENDGAME_THRESHOLD:
			i = 0
			while i < num_to_req and len(smallest_k) > 0:
				candidate_k = choice(smallest_k)
				candidate_piece = candidate_k[0]
				smallest_k.remove(candidate_k)
				if not candidate_piece in self.assigned_reqs:
					peer = self.find_peer_with(candidate_piece)
					if peer:
						# logging.debug(f'Queuing {candidate_piece} on {peer.to_str()}')
						self.req_lock.acquire()
						status = peer.assign_request(candidate_piece, self.fstate.num_pieces, self.fstate.piece_len, self.fstate.flen)
						if status == 0:
							i += 1
							self.assigned_reqs.add(candidate_piece)
						self.req_lock.release()
		else:
			candidate_k = choice(smallest_k)
			candidate_piece = candidate_k[0]
			peers = self.get_all_peers_with(candidate_piece)
			self.req_lock.acquire()
			self.assigned_reqs.add(candidate_piece)
			for peer in peers:
				peer.assign_request(candidate_piece, self.fstate.num_pieces, self.fstate.piece_len, self.fstate.flen)
			self.req_lock.release()

	''' Finds a random peer with the piece, giving preference to peers that are not choking us '''
	def find_peer_with(self, piece: int) -> PeerConnection | None:
		candidate_peers = self.get_all_peers_with(piece)
		if len(candidate_peers) == 0:
			return None
		unchoked_peers = [p for p in candidate_peers if not p.is_choking()]
		if len(unchoked_peers) > 0:
			return choice(unchoked_peers)
		else:
			return choice(candidate_peers)
		
	def get_all_peers_with(self, piece: int) -> list[PeerConnection]:
		peers = self.get_all_peers()
		candidate_peers = [p for p in peers if p.has_piece(piece) and not p.has_assigned_req()]
		return candidate_peers
	
	def get_all_peers_without(self, piece: int) -> list[PeerConnection]:
		peers = self.get_all_peers()
		candidate_peers = [p for p in peers if not p.has_piece(piece)]
		return candidate_peers
	
	def get_all_peers(self) -> list[PeerConnection]:
		return list(self.peers.values())

	def choke_loop(self, stop_event: threading.Event):
		while(not stop_event.is_set()):
			sleep(CHOKE_RATE)
			self.choke_lock.acquire()

			peers = self.get_all_peers()
			# Sort peers based on download rate or upload rate depending on the state of the torrent
			if not self.fstate.is_finished():
				sorted_rates = sorted(peers, key = lambda p: -p.get_dl_rate())
			else:
				sorted_rates = sorted(peers, key = lambda p: -p.get_ul_rate())
			to_unchoke = set()
			new_downloaders = set()
			# Keep the optimistic unchoke in downloaders if it's there
			if self.opt_unchoke in self.downloaders:
				new_downloaders.add(self.opt_unchoke)
			# Choose new peers to unchoke
			for p in sorted_rates:
				if not p.is_interested():
					to_unchoke.add(p)
				else:
					to_unchoke.add(p)
					new_downloaders.add(p)
					if len(new_downloaders) == 4:
						break
			# Choke and unchoke peers as needed
			old_unchoked = self.unchoked - to_unchoke
			new_unchoked = to_unchoke - self.unchoked
			for p in old_unchoked:
				p.choke()
			for p in new_unchoked:
				p.unchoke()
			self.unchoked = to_unchoke
			self.downloaders = new_downloaders

			self.choke_lock.release()
			sleep(OPT_UNCHOKE_RATE - CHOKE_RATE)
			self.choke_lock.acquire()

			new_opt_unchoke = self.choose_optimistic_unchoke()
			if not new_opt_unchoke == None:
				# Do not necessarily unchoke the old optimistic unchoke
				if not self.opt_unchoke in self.downloaders and not self.opt_unchoke == None:
					self.opt_unchoke.choke()
				new_opt_unchoke.unchoke()
				if new_opt_unchoke.is_interested():
					if len(self.downloaders) == 4:
						self.choke_slowest_downloader()
					self.downloaders.add(new_opt_unchoke)
				self.opt_unchoke = new_opt_unchoke
			
			self.choke_lock.release()

	def choke_slowest_downloader(self):
		downloader_lst = list(self.downloaders)
		if self.opt_unchoke in downloader_lst:
			downloader_lst.remove(self.opt_unchoke)
		if not len(downloader_lst) > 0:
			return
		if not self.fstate.is_finished():
			sorted_rates = sorted(downloader_lst, key = lambda p: -p.get_dl_rate())
		else:
			sorted_rates = sorted(downloader_lst, key = lambda p: -p.get_ul_rate())
		candidate = sorted_rates[0]
		if candidate in self.unchoked:
			self.unchoked.remove(candidate)
		self.downloaders.remove(candidate)
	
	def choose_optimistic_unchoke(self) -> PeerConnection | None:
		candidates = list(set(self.get_all_peers()) - self.unchoked)
		# Remove the current optimistic unchoke from the list of candidates
		if self.opt_unchoke in candidates:
			candidates.remove(self.opt_unchoke)
		# Recent connections are three times as likely to be chosen
		weights = []
		for c in candidates:
			if c.get_sock() in self.recent_conns:
				weights.append(3)
			else:
				weights.append(1)
		if len(candidates) > 0:
			return choices(candidates, weights=weights)[0]
		else:
			return None

	def begin_choke_loop(self, stop_event: threading.Event):
		thread = threading.Thread(target=self.choke_loop, args=[stop_event,])
		thread.start()

	def keepalive_loop(self, stop_event: threading.Event):
		while(not stop_event.is_set()):
			sleep(KEEPALIVE_RATE)
			logging.debug('Sending keepalive to all peers')
			curr_time = time()
			reaped_req = False
			for peer in list(self.peers.values()):
				if curr_time - peer.get_last_msg_stamp() > KEEPALIVE_TIMEOUT:
					self.sever_conn(peer, TIMEOUT)
				else:
					peer.send_keepalive()
				# Reap outstanding request
				self.req_lock.acquire()
				if peer.has_outstanding_req() and curr_time - peer.get_request_stamp() > REQ_TIMEOUT:
					self.cancel_peer_request(peer)
					reaped_req = True
				self.req_lock.release()
			if reaped_req:
				self.request_pieces()

	def begin_keepalive_loop(self, stop_event: threading.Event):
		logging.debug('Beginning keepalive loop')
		thread = threading.Thread(target=self.keepalive_loop, args=[stop_event,])
		thread.start()

	def get_all_socks(self) -> list[socket.socket]:
		return list(self.peers.keys())
	
	def num_conn_peers(self) -> int:
		return len(self.peers)