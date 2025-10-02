import hashlib
import socket


PORT_MIN = 6881
PORT_MAX = 6889
STR_ENC = 'utf-8'
DEF_PEERS = 50

CONN_TIMEOUT = 1.0  # Impatient for now

BLOCK_LEN = 2**14

CHOKE = 0
UNCHOKE = 1
INTERESTED = 2
UNINTERESTED = 3
HAVE = 4
BITFIELD = 5
REQUEST = 6
PIECE = 7
CANCEL = 8

HANDSHAKE_LEN = 68
LEN_LEN = 4
ID_LEN = 1

Addr = tuple[str, int]
AddrList = list[Addr]
FileInfo = tuple[list[str], int]


def hash_bytes(input: bytes) -> bytes:
    sha1 = hashlib.sha1()
    sha1.update(input)
    hash = sha1.digest()
    return hash

def hash_str(input: str) -> bytes:
    return hash_bytes(input.encode())

def build_msg(id: int, payload: list[bytes] | None = None) -> bytes:
		msg_payload = b''
		if payload:
			for p in payload:
				msg_payload += p
		msg = int.to_bytes(ID_LEN + len(msg_payload), LEN_LEN, 'big')
		msg += int.to_bytes(id, ID_LEN, 'big')
		msg += msg_payload
		return msg

def create_sock(addr: Addr, tries: int = 1) -> socket.socket | None:
    for i in range(tries):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(CONN_TIMEOUT)
        try:
            sock.connect(addr)
            sock.settimeout(None)
            return sock
        except:
            sock.close()
    return None