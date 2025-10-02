import argparse
from random import randint
from .lib import PORT_MIN, PORT_MAX, DEF_PEERS
from dataclasses import dataclass
import logging

@dataclass
class Opts:
    tf: str
    port: int
    peers: int
    outdir: str
    logname: str
    loglevel: str
    seed_file_path: str | None
    # outname: str

def init_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument('-t', '--torrentfile', metavar='torrent_file', help="The .torrent file to open", required=True)
    parser.add_argument('--port', '-p', metavar='port', help='The port that the client is listening on', required=False)
    parser.add_argument('--peers', '-np', metavar='peers', help='The number of peers to request from the tracker', required=False)
    parser.add_argument('--outdir', '-d', metavar='output_dir', help='The output directory for the downloaded file', required=False)
    parser.add_argument('--loglevel', '-ll', metavar='log_level', help='The log level for the client', required=False)
    parser.add_argument('--logname', '-ln', metavar='log_name', help='The name of the log file', required=False)
    parser.add_argument('--seedfile', '-s', metavar='seed_file', help='The file to seed', required=False)
    #parser.add_argument('--outname', '-n', metavar='output_name', help='The output name for the downloaded file', required=False)
    return parser

def validate_args(args):
    if(args.port and not args.port.isdigit()):
        logging.error("Port must be a number")
        exit(-1)
    if(args.peers and not args.peers.isdigit()):
        logging.error("Peers must be a digit")
        exit(-1)


def parse() -> Opts:
    parser = init_parser()
    args = parser.parse_args()
    validate_args(args)
    port = int(args.port) if args.port else randint(PORT_MIN, PORT_MAX)
    peers = int(args.peers) if args.peers else DEF_PEERS
    outdir = args.outdir if args.outdir else '.'
    loglevel = args.loglevel.upper() if args.loglevel else 'WARNING'
    seed_file_path = args.seedfile if args.seedfile else None

    opts = Opts(args.torrentfile, port, peers, outdir, args.logname, loglevel, seed_file_path)
    return opts
