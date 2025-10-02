from src.client import TorrentClient
from src.peers import PeerConnections

class SeedTorrentClient(TorrentClient):

    def __init__(self, port, torrent_file_path: str, seed_file_path: str, output_dir: str):
        super().__init__(port)
        self.open_tfile(torrent_file_path)
        self.init_filestate(output_dir)
        self.populate_filestate(seed_file_path)

    def populate_filestate(self, seed_file_path: str):
        with open(seed_file_path, 'rb') as f:
            for i in range(self.fstate.num_pieces):
                piece = f.read(self.fstate.piece_len)
                if len(piece) == 0:
                    break
                self.fstate.write(i, piece)

    def begin_torrent(self):
        self.conns = PeerConnections(self.fstate, self.tfile.info_hash, self.peer_id)
        self.begin_loops()

    def begin_loops(self):
        self.conns.begin_keepalive_loop(self.stop_event)
        self.conns.begin_choke_loop(self.stop_event)
        self.begin_socket_loop(self.stop_event)
