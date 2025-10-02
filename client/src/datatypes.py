from socket import socket
from dataclasses import dataclass

''' Represents parsed data from a torrent file '''
class TorrentFile:
	path: str 				# File path to the torrent file

	announce_url: str   	# URL of the tracker
	creation_date: int 
	comment: str
	created_by: str
	encoding: str
	
	info_hash: int			# Hash of the entire 'info' field
	piece_len: int			# Length of each piece in bytes
	pieces_hash_str: str	# Concatenated hash of all pieces
	private: bool 
	
	fname: str				# Advisory name of the file
	flen: int				# Length of the file in bytes

	''' Parse the torrent file at 'path' and initialize all fields '''
	def __init__(path: str):
		pass

	''' Create a FileState object from this object '''
	def to_filestate():
		pass

	''' Create a Tracker object from this object '''
	def to_tracker():
		pass

'''
	State of the file specified in the torrent file, including how complete it is
	One instance shared among all objects
'''
class FileState:
	flen: int				# Length of the file in bytes
	uploaded: int			# Total number of bytes of the file uploaded
	downloaded: int			# Total number of bytes of the file downloaded
	left: int				# Number of bytes the client still has to download

	path: str				# Path to the file
	fd: int					# Open file descriptor

	num_pieces: int
	piece_len: int			# Length of each piece in bytes
	pieces_hash: list[int]	# Hash of each piece
	bitfield: list[bool]	# Pieces that we have

	''' Open the file and init all fields '''
	def __init__(path: str, 
			  flen: int, 
			  piece_len: int, 
			  pieces_hash_str: str):
		pass

''' Information about a peer '''
@dataclass
class PeerInfo:
	peer_id: str | None
	ip: str
	port: int

	def to_byte_dict(self):
		return {b'peerId': self.peer_id, b'ip': self.ip, b'port': self.port}

''' Represents a peer connection '''
class PeerConnection:
	peer_info: PeerInfo
	sock: socket
	
	am_choking: bool
	am_interested: bool
	peer_choking: bool
	peer_interested: bool

	bitfield: list[bool]	# Pieces that the peer has

	''' Initialize a connection with the peer '''
	def __init__(peer_info: PeerInfo, sock: socket = None):
		pass

''' Handles all peer connections '''
class PeerConnections:
	peers: list[PeerConnection]
	fstate: FileState

	''' Static fields needed for each handshake '''
	info_hash: bytes
	client_peer_id: bytes

	''' Initialize connections with all of the specified peers '''
	def __init__(peers: list[PeerInfo], fstate: FileState, info_hash: bytes, client_peer_id: bytes):
		pass

	''' Add a new peer connection after a peer has initiated with us '''
	def add_conn(peer_sock: socket):
		pass

	''' Implement a number of request and response handlers '''

	''' Find a peer that has the specified piece '''
	def find_piece(piece_index: int):
		pass

''' Handles relationship with the tracker '''
class Tracker:
	url: str

	tid: str			# Torrent id
	interval: int		# Interval in seconds between regular requests
	min_interval: int   # Absolute minimum request interval
	complete: int		# Number of peers with the entire file
	incomplete: int		# Number of leechers
	peers: list[PeerInfo]

	''' Static fields needed for each request '''
	info_hash: int
	client_peer_id: str
	local_port: str
	tracker_id: str

	def __init__(url: str, 
			  info_hash: int, 
			  client_peer_id: str, 
			  local_port: int):
		pass

	''' Send a request to the tracker '''
	def request(fstate: FileState,  # Alternatively, the tracker can hold a pointer to the global FileState object
			 event: str = None, 
			 compact: bool = False, 
			 no_peer_id: bool = False, 
			 numwant: int = 50):
		pass


class TorrentClient:
	tfile: TorrentFile
	fstate: FileState
	tracker: Tracker
	conns: PeerConnections

	peer_id: str
	local_port: int
	local_sock: socket

	def __init__(self, port: int):
		self.create_peer_id()

	def create_peer_id(self):
		pass

	def create_sock():
		pass