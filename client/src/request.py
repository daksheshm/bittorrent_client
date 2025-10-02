from .lib import REQUEST, CANCEL, BLOCK_LEN, LEN_LEN, build_msg
from time import time
from math import ceil
import logging
from typing import Literal

class Request:

	piece: int | None

	outstanding: bool
	send_stamp: float | None

	total_len: int
	buffer: list[bytes]
	recvd_blocks: set[int]
	num_blocks: int
	final_block_len: int | None

	def __init__(self, piece: int, num_pieces: int, piece_len: int, flen: int):
		self.piece = piece
		self.outstanding = False
		if piece < num_pieces - 1 or flen % piece_len == 0:
			self.total_len = piece_len
		else:
			self.total_len = flen % piece_len
		self.num_blocks = ceil(self.total_len / BLOCK_LEN)
		final_block_len = self.total_len % BLOCK_LEN
		self.final_block_len = final_block_len if final_block_len > 0 else BLOCK_LEN
		self.buffer = [b''] * self.num_blocks
		self.recvd_blocks = set()
		self.send_stamp = None

	def build_reqs(self, type: int = REQUEST) -> list[bytes]:
		reqs = []
		for i in range(self.num_blocks - 1):
			reqs.append(build_msg(type,
				[int.to_bytes(self.piece, LEN_LEN, 'big'),
				int.to_bytes(i*BLOCK_LEN, LEN_LEN, 'big'),
				int.to_bytes(BLOCK_LEN, LEN_LEN, 'big') ] ))
		reqs.append(build_msg(type,
			[int.to_bytes(self.piece, LEN_LEN, 'big'),
			int.to_bytes((self.num_blocks-1)*BLOCK_LEN, LEN_LEN, 'big'),
			int.to_bytes(self.final_block_len, LEN_LEN, 'big') ] ))
		return reqs
		
	def add_to_buffer(self, offset: int, data: bytes) -> int:
		if not self.outstanding:
			logging.error('request is not outstanding')
			return -1
		block = offset // BLOCK_LEN
		if block >= self.num_blocks:
			logging.error('block index too high')
			return -1
		if len(data) > BLOCK_LEN:
			logging.error('bad block len')
			return -1
		if block == self.num_blocks - 1 and len(data) > self.final_block_len:
			logging.error('bad block len')
			return -1
		self.buffer[block] = data
		self.recvd_blocks.add(block)
		return 0

	def is_complete(self) -> bool:
		return len(self.recvd_blocks) == self.num_blocks

	def get_buffer(self) -> bytes:
		buf = b''
		for i in self.buffer:
			buf += i
		return buf

	def get_piece(self) -> int:
		return self.piece

	def set_outstanding(self):
		self.outstanding = True
		self.send_stamp = time()

	def cancel_outstanding(self):
		self.outstanding = False
		self.send_stamp = None
		self.buffer = [b''] * self.num_blocks
		self.recvd_blocks = set()

	def is_outstanding(self) -> bool:
		return self.outstanding
	
	def get_send_stamp(self) -> float | None:
		return self.send_stamp

	def build_cancel_reqs(self) -> list[bytes]:
		return self.build_reqs(CANCEL)