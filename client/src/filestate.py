from dataclasses import dataclass
from io import TextIOWrapper
from math import ceil
from .bitfield import BitField
from .lib import hash_bytes, FileInfo
from time import sleep
import os
import logging

'''
	State of the file specified in the torrent file, including how complete it is
	One instance shared among all objects
'''
class FileState:
	flen: int				# Total length of the file(s) in bytes
	uploaded: int			# Total number of bytes of the file uploaded since contacting the tracker
	downloaded: int			# Total number of bytes of the file downloaded since contacting the tracker
	left: int				# Number of bytes the client still has to download

	output_dir: str             # Base directory for output files
	dirname: str				# Path to the files
	files: list[FileInfo]
	fds: list[TextIOWrapper]	# Open file descriptors
	cum_flen: list[int]

	num_pieces: int
	piece_len: int
	hashes: list[bytes]         # Hash of each piece
	bitfield: BitField	    # Pieces that we have

	def __init__(self,
			  piece_len: int, 
			  hashes: list[bytes],
			  dirname: str,
			  files: list[FileInfo],
			  output_dir: str):
		self.files = files
		self.flen = 0
		self.cum_flen = []
		for f in files:
			self.flen += f[1]
			self.cum_flen.append(self.flen)
		self.piece_len = piece_len
		self.hashes = hashes
		self.num_pieces = ceil(self.flen / piece_len)
		self.uploaded = 0
		self.downloaded = 0
		self.bitfield = BitField(self.num_pieces)
		self.dirname = dirname
		self.output_dir = output_dir
		self.fds = []
		
	''' Opens the output file(s) '''
	def open(self):
		base_path = os.path.join(self.output_dir, self.dirname)
		os.makedirs(base_path, exist_ok=True)
		for f in self.files:
			path = os.path.join(base_path, *f[0])
			if not self.dirname == '':
				os.makedirs(os.path.dirname(path), exist_ok=True)
			try:
				fd = open(path, 'r+b')
			except:
				try:
					fd = open(path, 'x')
					fd.close()
					fd = open(path, 'r+b')
				except Exception as e:
					logging.error('Error opening output file:', e)
					exit(-1)
			self.fds.append(fd)
		self.left = self.flen
	
	def close(self):
		for fd in self.fds:
			fd.close()

	def write(self, piece: int, piece_data: bytes):
		hash = hash_bytes(piece_data)
		if not self.hashes[piece] == hash:
			logging.error('hash doesnt match :(')
			return -1
		if self.bitfield.is_set(piece):
			logging.warn('already have this piece')
			return -1
		# Get the file(s) this piece belongs in
		byte_index = piece * self.piece_len
		if byte_index + len(piece_data) > self.flen:
			raise Exception('Piece out of bounds')
		i = 0
		while byte_index >= self.cum_flen[i]:
			i += 1
		file_index = byte_index - self.cum_flen[i-1] if i > 0 else byte_index
		bytes_left = piece_data
		while len(bytes_left) > 0:
			left_in_file = self.cum_flen[i] - byte_index
			len_to_write = min(left_in_file, len(bytes_left))
			fd = self.fds[i]
			fd.seek(file_index)
			fd.write(bytes_left[:len_to_write])
			bytes_left = bytes_left[len_to_write:]
			byte_index += len_to_write
			file_index = 0
			i += 1
		self.bitfield.set_bit(piece)
		self.downloaded += self.piece_len
		self.left -= self.piece_len
		logging.info(f'{self.downloaded // self.piece_len}/{self.num_pieces} pieces complete')
		if self.left <= 0:
			logging.info('File(s) complete!')
		return 0
		
	def has(self, piece: int) -> bool:
		return self.bitfield.is_set(piece)

	def read(self, piece: int, begin: int, length: int) -> bytes:
		"""
		Params:
			piece: the index of the piece to read
			begin: the offset in bytes from the beginning of the piece
			length: the number of bytes to read

		Returns:
			bytes: the data read from the file(s).
		"""
		byte_index = piece*self.piece_len + begin
		if byte_index + length > self.flen:
			raise Exception('Piece out of bounds')
		i = 0
		while byte_index >= self.cum_flen[i]:
			i += 1
		file_index = byte_index - self.cum_flen[i-1] if i > 0 else byte_index
		bytes_left = length
		data = b''
		while bytes_left > 0:
			left_in_file = self.cum_flen[i] - byte_index
			len_to_read = min(left_in_file, bytes_left)
			fd = self.fds[i]
			fd.seek(file_index)
			data += fd.read(len_to_read)
			bytes_left -= len_to_read
			byte_index += len_to_read
			file_index = 0
			i += 1
		return data

	''' Checks if the file is consistent with its hashes '''
	def check_file():
		pass

	def get_bitfield(self) -> BitField:
		return self.bitfield
	
	def get_progress(self) -> float:
		return (self.flen - self.left) / self.flen
	
	def is_finished(self) -> bool:
		return self.left <= 0