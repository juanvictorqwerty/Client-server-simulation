import threading
import socket
import json
from pyftpdlib.authorizers import DummyAuthorizer
from pyftpdlib.handlers import FTPHandler
from pyftpdlib.servers import FTPServer
from virtual_network import VirtualNetwork
import os
import re
import tempfile
import shutil

class CustomFTPHandler(FTPHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.session_state = {
            "current_filename": None,
            "expected_chunks": 5,
            "received_chunks": 0,
            "total_received_size": 0,
            "target_node": None,
            "temp_file_path": None,
            "expected_chunk_size": None,
            "folder_name": None
        }

    def _get_unique_folder_name(self):
        """Generate a unique folder name (e.g., file_001)."""
        counter = 1
        while True:
            folder_name = f"file_{counter:03d}"
            folder_path = os.path.join(self.server.manager.disk_path, folder_name)
            with self.server.manager.pending_files_lock:
                if not os.path.exists(folder_path) and folder_name not in self.server.manager.pending_files:
                    return folder_name
            counter += 1

    def on_file_received(self, file_path):
        """Handle chunk reception for the server."""
        with open(file_path, 'rb') as f:
            data = f.read()

        header_pattern = re.compile(b"CHUNK:(\d+):(\d+):([^\n]+)\n")
        match = header_pattern.match(data)
        if not match:
            print(f"Error: Invalid chunk header in {file_path}")
            return

        chunk_number = int(match.group(1))
        chunk_size = int(match.group(2))
        target_node = match.group(3).decode()
        header_end = match.end()
        payload = data[header_end:header_end + chunk_size]
        actual_payload_size = len(payload)

        if actual_payload_size != chunk_size:
            print(f"Error: Chunk {chunk_number} size mismatch, expected {chunk_size}, got {actual_payload_size}")
            return

        original_filename = os.path.basename(file_path)

        if chunk_number == 1:
            folder_name = self._get_unique_folder_name()
            folder_path = os.path.join(self.server.manager.disk_path, folder_name)
            os.makedirs(folder_path, exist_ok=True)
            self.session_state["current_filename"] = original_filename
            self.session_state["received_chunks"] = 1
            self.session_state["total_received_size"] = actual_payload_size
            self.session_state["target_node"] = target_node
            self.session_state["folder_name"] = folder_name
            self.session_state["temp_file_path"] = tempfile.NamedTemporaryFile(delete=False, dir=folder_path).name
            self.session_state["expected_chunk_size"] = chunk_size
            with open(self.session_state["temp_file_path"], 'wb') as f:
                f.write(payload)
            with self.server.manager.pending_files_lock:
                self.server.manager.pending_files[folder_name] = (target_node, original_filename)
        else:
            if original_filename != self.session_state["current_filename"] or target_node != self.session_state["target_node"]:
                print(f"Error: Chunk {chunk_number} for {original_filename}:{target_node} does not match expected {self.session_state['current_filename']}:{self.session_state['target_node']}")
                return
            if chunk_number != self.session_state["received_chunks"] + 1:
                print(f"Error: Received chunk {chunk_number} out of order, expected {self.session_state['received_chunks'] + 1}")
                return
            if chunk_size != self.session_state["expected_chunk_size"]:
                print(f"Error: Chunk {chunk_number} size {chunk_size} does not match expected {self.session_state['expected_chunk_size']}")
                return
            self.session_state["received_chunks"] += 1
            self.session_state["total_received_size"] += actual_payload_size
            with open(self.session_state["temp_file_path"], 'ab') as f:
                f.write(payload)

        print(f"Server received chunk {chunk_number}/{self.session_state['expected_chunks']} for {original_filename} (folder: {self.session_state['folder_name']}, target: {target_node}): {self.session_state['total_received_size']} bytes total")

        if self.session_state["received_chunks"] == self.session_state["expected_chunks"]:
            final_path = os.path.join(self.server.manager.disk_path, self.session_state["folder_name"], original_filename)
            os.rename(self.session_state["temp_file_path"], final_path)
            print(f"Server stored {original_filename} in folder {self.session_state['folder_name']}: {self.session_state['total_received_size']} bytes for {target_node}")
            # Check if the target node is available and forward the file
            self.server.manager.check_node_and_forward(self.session_state["folder_name"], target_node, final_path, original_filename)
            self.session_state = {
                "current_filename": None,
                "expected_chunks": 5,
                "received_chunks": 0,
                "total_received_size": 0,
                "target_node": None,
                "temp_file_path": None,
                "expected_chunk_size": None,
                "folder_name": None
            }

        try:
            os.remove(file_path)
        except OSError:
            pass

class FTPServerManager:
    def __init__(self, ip_address, ftp_port, disk_path):
        self.ip_address = ip_address
        self.ftp_port = ftp_port
        self.disk_path = disk_path
        self.network = VirtualNetwork(self)
        self.pending_files = {}  # folder_name: (target_node_name, original_filename)
        self.pending_files_lock = threading.Lock()
        self.ftp_server = None
        self.socket_server = None
        self.socket_port = 9999
        self.active_nodes = set()  # Track active nodes
        self.active_nodes_lock = threading.Lock()  # Lock for active_nodes

    def start(self):
        """Start the FTP server and socket server."""
        authorizer = DummyAuthorizer()
        authorizer.add_user("user", "password", self.disk_path, perm="elradfmw")
        handler = CustomFTPHandler
        handler.authorizer = authorizer
        self.ftp_server = FTPServer(("0.0.0.0", self.ftp_port), handler)
        self.ftp_server.node = None
        self.ftp_server.manager = self
        ftp_thread = threading.Thread(target=self.ftp_server.serve_forever, daemon=True)
        ftp_thread.start()
        print(f"FTP server started on {self.ip_address}:{self.ftp_port}")

        self.socket_server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket_server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket_server.bind(("0.0.0.0", self.socket_port))
        self.socket_server.listen(10)  # Increased backlog for more concurrent connections
        socket_thread = threading.Thread(target=self._handle_socket_connections, daemon=True)
        socket_thread.start()
        print(f"Socket server started on {self.ip_address}:{self.socket_port}")

    def stop(self):
        """Stop the FTP server and socket server."""
        if self.ftp_server:
            self.ftp_server.close_all()
            print(f"FTP server stopped for {self.ip_address}")
        if self.socket_server and not self.ftp_server and not self.fenchmarksself.socket_server:
            self.socket_server.close()
            print(f"Socket server stopped for {self.ip_address}")

    def _handle_socket_connections(self):
        """Handle incoming socket connections from nodes."""
        while True:
            try:
                client_socket, addr = self.socket_server.accept()
                threading.Thread(target=self._process_socket_message, args=(client_socket,), daemon=True).start()
            except Exception as e:
                print(f"Socket server error: {e}")
                break

    def _process_socket_message(self, client_socket):
        """Process node availability messages."""
        try:
            data = client_socket.recv(1024).decode()
            message = json.loads(data)
            if message.get("action") == "node_started":
                node_name = message.get("node_name")
                print(f"Node {node_name} started, checking for pending files")
                with self.active_nodes_lock:
                    self.active_nodes.add(node_name)  # Mark node as active
                self.network.forward_file(None, node_name)
            client_socket.close()
        except Exception as e:
            print(f"Error processing socket message: {e}")
            client_socket.close()

    def check_node_and_forward(self, folder_name, target_node, file_path, original_filename):
        """Check if the target node is available and forward the file."""
        with self.active_nodes_lock:
            if target_node in self.active_nodes:
                print(f"Target node {target_node} is active, forwarding file {original_filename}")
                self.network.forward_file(folder_name, target_node)
            else:
                print(f"Target node {target_node} is not active, keeping file {original_filename} in pending")