from .filestate import FileState
from . import bencoder
from .lib import hash_bytes, STR_ENC, FileInfo
from .tracker import RemoteTracker
import logging

''' Represents parsed data from a torrent file '''
class TorrentFile:
	path: str 				# File path to the torrent file

	announce_url: str   	# URL of the tracker
	creation_date: int 
	comment: str
	created_by: str
	encoding: str
	
	info_hash: bytes		# Hash of the entire 'info' field
	piece_len: int			# Length of each piece in bytes
	pieces_hash: bytes		# Concatenated hash of all pieces
	private: bool 
	
	dirname: str
	files: list[FileInfo]	# List of (path, length)

	''' Parse the torrent file at 'path' and initialize all fields '''
	def __init__(self, path: str):
		self.path = path
	
	def parse_tfile(self):
		try:
			with open(self.path, 'rb') as f:
				contents = f.read()
				bdecoded = bencoder.decode(contents)
				e = TorrentFile.validate_contents(bdecoded)
				if(e): raise e
				#TODO: Add support for announce-list
				self.announce_url = bdecoded.get(b'announce').decode(STR_ENC)
				self.creation_date = bdecoded.get(b'creation date', None)
				self.comment = bdecoded.get(b'comment', b'').decode(STR_ENC)
				self.created_by = bdecoded.get(b'created by', b'').decode(STR_ENC)
				self.encoding = bdecoded.get(b'encoding', b'').decode(STR_ENC)
				info: dict = bdecoded.get(b'info')
				self.piece_len = info.get(b'piece length')
				self.pieces_hash = info.get(b'pieces')
				self.private = info.get(b'private', None)
				if b'length' in info:
					self.dirname = ''
					path = [info[b'name'].decode(STR_ENC)]
					self.files = [(path, info[b'length'])]
				elif b'files' in info:
					self.dirname = info[b'name'].decode(STR_ENC)
					file_dicts = info[b'files']
					self.files = []
					for f in file_dicts:
						path = f[b'path']
						for i, p in enumerate(path):
							path[i] = p.decode(STR_ENC)
						self.files.append((path, f[b'length']))
				#info_encoded = bencoder.encode(info)
				info_encoded = bencoder.get_bencoded_val(contents, b'info')
				self.info_hash = hash_bytes(info_encoded)
		except Exception as e:
			logging.error("Error: ", e)
			exit(-1)

	''' Validates the contents of a parsed torrent file '''
	def validate_contents(bdecoded) -> Exception | None:
		if(not type(bdecoded) == dict):
			return Exception('Parse is not a dict')
		if not b'info' in bdecoded:
			return Exception('No info field')
		if not b'announce' in bdecoded and not b'announce-list' in bdecoded:
			return Exception('No announce or announce-list field')
		if not (len(bdecoded[b'info'][b'pieces']) % 20 == 0):
			return Exception('Hash list is incorrect length')
		return None

	def to_filestate(self, output_dir: str) -> FileState:
		hash_list = [self.pieces_hash[i:i+20] for i in range(0, len(self.pieces_hash), 20)]
		fstate = FileState(self.piece_len, hash_list, self.dirname, self.files, output_dir)
		return fstate