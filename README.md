# Simple Python BitTorrent Client

A basic BitTorrent client implemented in Python 3. This project focuses on the foundational steps of the BitTorrent protocol: parsing `.torrent` files and communicating with trackers to discover peers.

## Features

-   Parses `.torrent` metainfo files to extract details.
-   Calculates the torrent's unique info hash.
-   Communicates with trackers over both HTTP and UDP protocols.
-   Simple command-line interface.

## Getting Started

Follow these steps to get the client up and running on your local machine.

### Prerequisites

-   Python 3.6+
-   `pip`

### Installation

1.  **Clone the repository:**
    ```bash
    git clone https://github.com/bbswitzer/BitTorrent-Client.git
    cd BitTorrent-Client
    ```

2.  **Install the required dependencies:**
    *(It is recommended to use a Python virtual environment)*
    ```bash
    pip install -r requirements.txt
    ```

## Usage

To run the client, provide the path to a `.torrent` file as a command-line argument.

```bash
python -m torrent.client /path/to/your/torrent-file.torrent
```

The client will then parse the file, display its metadata, and attempt to connect to the tracker to get a list of available peers.
