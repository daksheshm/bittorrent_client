from src.tracker import TrackerServer
import argparse

if __name__ == "__main__":
    """Get the configuration file path from arguments"""
    parser = argparse.ArgumentParser(description="Run the tracker server")
    parser.add_argument("config", help="[tracker.(json|toml)] Path to the configuration file")
    args = parser.parse_args()

    """Load the tracker server from the configuration file"""
    if args.config.endswith(".toml"):
        server, port = TrackerServer.load_toml(args.config)
    elif args.config.endswith(".json"):
        server, port = TrackerServer.load_json(args.config)
    else:
        raise Exception("Unsupported configuration file format")

    server.listen(port)
    print(f"Tracker server listening on port {port}...")
    try:
        server.run()
    except KeyboardInterrupt:
        print("Shutting down tracker server...")
        server.close()
        print("Tracker server shut down.")
        exit(0)
    except Exception as e:
        print("An error occurred while running the tracker server.")
        server.close()
        raise e
