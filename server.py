import os
from server_manager import FTPServerManager

if __name__ == "__main__":
    server_disk_path = "./assets/server/"
    os.makedirs(server_disk_path, exist_ok=True)
    server = FTPServerManager("127.0.0.1", 2120, server_disk_path)
    server.start()
    print("Server running. Press Ctrl+C to stop.")
    try:
        while True:
            pass  # Keep server running
    except KeyboardInterrupt:
        server.stop()
        print("Server stopped.")