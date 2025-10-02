from src.argparser import parse, Opts
from src.client import TorrentClient
from src.torrentfile import TorrentFile

from signal import signal, SIGINT
import sys

import logging
import os


client: TorrentClient = None

def main():
    args = parse()
    if args.logname:
        logging.basicConfig(
            level=args.loglevel, 
            format='%(asctime)s %(levelname)-8s %(message)s',
            filename=args.logname, 
            filemode='w')
    else:
        logging.basicConfig(
            level=args.loglevel, 
            format='%(asctime)s %(levelname)-8s %(message)s')

    global client
    if not args.seed_file_path:
        client = TorrentClient(args.port)
        signal(SIGINT, ctrlc_handler)
        client.open_tfile(args.tf)
        client.init_filestate(args.outdir)
        client.begin_torrent(args.peers)
    else:
        from src.seed_client import SeedTorrentClient
        client = SeedTorrentClient(args.port, args.tf, args.seed_file_path, args.outdir)
        client.begin_torrent()

def ctrlc_handler(sig, frame):
    global client
    client.end_torrent()
    exit(0)

if __name__ == '__main__':
    main()

"""
1. Init a torrent client
2. Create a peer id for the torrent client
3. Open a client socket listening for connections
4. Parse the torrent file by initiating a TorrentFile object
5. Create a FileState object from the TorrentFile object
6. Init a Tracker object from the TorrentFile object
7. Send a 'started' request to the tracker
8. Parse the response from the tracker
9. Begin a thread sending regular requests to the tracker
10. Init a PeerConnections object from the peers received from the tracker
11. Begin the peer wire protocol
"""
