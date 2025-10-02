import socket
from src.datatypes import PeerInfo
import hashlib
import threading
from urllib.parse import urlparse, parse_qs 
from src import bencoder
import json

class TrackerServer:
	"""HTTP/HTTPS service which responds to HTTP GET requests. The response 
	includes a list of peers that helps the client participate in the 
	torrent.
	
	https://wiki.theory.org/BitTorrentSpecification#Tracker_HTTP.2FHTTPS_Protocol
	
	## Example Usage
	```python
	serv, port = TrackerServer.load_toml('tracker.toml')
	serv.listen(port)
	serv.run()
	```
	"""

	sock: socket.socket
	peer_list: list[PeerInfo]
	torrent_file: str
	info_hash: bytes
	sha1: hashlib.sha1
	sha256: hashlib.sha256
	event: threading.Event

	def __init__(self, peer_list: list[PeerInfo], torrent_file: str):
		self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		self.peer_list = peer_list
		self.sha256 = hashlib.sha256()
		self.sha1 = hashlib.sha1()
		self.event = threading.Event()
		self.event.set()
		self.torrent_file = torrent_file
		self.info_hash = self._get_info_hash()

	@staticmethod
	def _deserialize(data: dict):
		"""Deserializes a dictionary into a TrackerServer object"""
		peer_list = []
		if 'torrentFile' not in data:
			raise Exception('No torrent_file found in tracker config')
		if 'port' not in data:
			raise Exception('No port found in tracker config')
		if 'peers' not in data:
			raise Exception('No peers found in tracker config')
		for peer in data['peers']:
			peer_list.append(PeerInfo(peer['peerId'], peer['ip'], peer['port']))
		return TrackerServer(peer_list, data['torrentFile']), data['port']

	@staticmethod
	def load_toml(file_path: str):
		import tomllib
		"""Loads a TrackerServer object from a TOML file."""
		with open(file_path, 'rb') as f:
			data = tomllib.load(f)

		return TrackerServer._deserialize(data)

	def load_json(file_path: str):
		"""Loads a TrackerServer object from a JSON file."""
		with open(file_path, 'rb') as f:
			data = json.load(f)

		return TrackerServer._deserialize(data)

	def listen(self, port):
		host = '127.0.0.1'
		self.sock.bind((host, port))
		self.sock.listen(5)

	def run(self):
		while self.event.is_set():
			conn, addr = self.sock.accept()
			data = conn.recv(10000)
			if not data:
				break
			self.handle_request(data, conn)
			conn.close()

	def close(self):
		self.event.clear()
		self.sock.shutdown(socket.SHUT_RDWR)
		self.sock.close()

	def send_bencoded(self, conn: socket.socket, data: dict):
		"""Sends a bencoded dictionary to the client."""
		response_body = bencoder.encode(data)
		response_headers = [
			f"HTTP/1.1 200 OK",
			f"Content-Type: text/plain",
			f"Content-Length: {len(response_body)}",
			"Connection: close",
			"",
			""
		]
		response = '\r\n'.join(response_headers).encode() + response_body
		conn.sendall(response)

	def send_failure(self, conn: socket.socket, reason: str):
		response_dict = {b'failure reason': reason}
		self.send_bencoded(conn, response_dict)

	def _get_info_hash(self):
		"""Gets the info hash of the torrent file.
		
		The info hash is the SHA1 hash of the value of the 'info' key in the
		torrent file. The value is bencoded."""

		with open(self.torrent_file, 'rb') as f:
			contents = f.read()
			
		info_encoded = bencoder.get_bencoded_val(contents, b'info')
		sha1 = hashlib.sha1()
		sha1.update(info_encoded)
		hash = sha1.digest()
		return hash

	def get_peer_list(self, compact: bool):
		if compact:
			result = b''
			for peer_info in self.peer_list:
				result += peer_info.to_compact()
		else:
			result = []
			for peer_info in self.peer_list:
				result.append(peer_info.to_byte_dict())
		return result


	def handle_request(self, data: str, conn: socket.socket):
		"""Parses the request and sends the response to the client."""
		request = data.decode('utf-8')

		request_lines = request.split('\r\n')
		request_line = request_lines[0]
		method, path, version = request_line.split()

		# Get the ip address of the client
		ip = conn.getpeername()[0]
		self.sha256.update(ip.encode())
		tracker_id = self.sha256.hexdigest()

		parse = urlparse(path)
		if parse.path != '/announce':
			print(f"Received request with invalid path: {parse.path}")
			return
		query_params = parse_qs(parse.query)
		if 'info_hash' not in query_params:
			self.send_failure(conn, 'No info_hash found in request')

		if 'compact' not in query_params:
			self.send_failure(conn, 'No compact found in request')
		compact = int(query_params['compact'][0])

		info_hash = query_params['info_hash'][0]
		# TODO: Fix this. For some reason, the info_hash is not being decoded correctly
		# if info_hash.encode() != self.info_hash:
		# 	print("Received request with invalid info_hash")
		# 	self.send_failure(conn, 'Invalid info_hash')

		response_dict = {}
		response_dict[b'interval'] = 10
		response_dict[b'tracker id'] = tracker_id
		# TODO: Track the number of complete and incomplete peers
		response_dict[b'complete'] = len(self.peer_list)
		response_dict[b'incomplete'] = 0
		response_dict[b'peers'] = self.get_peer_list(bool(compact))
		self.send_bencoded(conn, response_dict)

if __name__ == "__main__":
	"""Example usage of the TrackerServer class"""
	serv, port = TrackerServer.load_toml('tracker.toml')
	serv.listen(port)
	serv.run()