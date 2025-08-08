import os
import ftplib
from pyftpdlib.authorizers import DummyAuthorizer
from pyftpdlib.handlers import FTPHandler
from pyftpdlib.servers import FTPServer
import threading
import tempfile
import math
import time
import re
import shutil

class CustomFTPHandler(FTPHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.session_state = {
            "current_filename": None,
            "expected_chunks": 5,
            "received_chunks": 0,
            "total_received_size": 0,
            "temp_file_path": None,
            "expected_chunk_size": None
        }

    def on_file_received(self, file_path):
        """Handle chunk reception for nodes."""
        if file_path.endswith("disk_metadata.json"):
            return

        with open(file_path, 'rb') as f:
            data = f.read()

        header_pattern = re.compile(b"CHUNK:(\d+):(\d+)\n")
        match = header_pattern.match(data)
        if not match:
            print(f"Error: Invalid chunk header in {file_path}")
            return

        chunk_number = int(match.group(1))
        chunk_size = int(match.group(2))
        header_end = match.end()
        payload = data[header_end:header_end + chunk_size]
        actual_payload_size = len(payload)

        if actual_payload_size != chunk_size:
            print(f"Error: Chunk {chunk_number} size mismatch, expected {chunk_size}, got {actual_payload_size}")
            return

        filename = os.path.basename(file_path)
        final_path = os.path.join(os.path.dirname(file_path), filename)

        if chunk_number == 1:
            self.session_state["current_filename"] = filename
            self.session_state["received_chunks"] = 1
            self.session_state["total_received_size"] = actual_payload_size
            self.session_state["temp_file_path"] = tempfile.NamedTemporaryFile(delete=False, dir=os.path.dirname(file_path)).name
            self.session_state["expected_chunk_size"] = chunk_size
            with open(self.session_state["temp_file_path"], 'wb') as f:
                f.write(payload)
        else:
            if filename != self.session_state["current_filename"]:
                print(f"Error: Chunk {chunk_number} for {filename} does not match expected file {self.session_state['current_filename']}")
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

        self.server.node.virtual_disk[filename] = self.session_state["total_received_size"]
        self.server.node._save_disk()
        print(f"Received chunk {chunk_number}/{self.session_state['expected_chunks']} for {filename}: {self.session_state['total_received_size']} bytes total")

        if self.session_state["received_chunks"] == self.session_state["expected_chunks"]:
            os.rename(self.session_state["temp_file_path"], final_path)
            print(f"Completed receiving {filename}: {self.session_state['total_received_size']} bytes")
            self.session_state = {
                "current_filename": None,
                "expected_chunks": 5,
                "received_chunks": 0,
                "total_received_size": 0,
                "temp_file_path": None,
                "expected_chunk_size": None
            }

        try:
            os.remove(file_path)
        except OSError:
            pass

class VirtualNetwork:
    def __init__(self, manager=None):
        self.manager = manager
        self.ip_map = {
            "192.168.1.1": {"disk_path": "./assets/node1/", "ftp_port": 2121, "node_name": "node1"},
            "192.168.1.2": {"disk_path": "./assets/node2/", "ftp_port": 2122, "node_name": "node2"},
            "192.168.1.3": {"disk_path": "./assets/node3/", "ftp_port": 2123, "node_name": "node3"}
        }
        self.ftp_servers = {}
        self.num_chunks = 5
        self.bandwidth_bytes_per_sec = 100 * 1024 * 1024 // 8  # 100 Mb/s = 12.5 MB/s
        self.header_size = 32
        self.server_ip = "127.0.0.1"
        self.server_port = 2120
        self.server_disk_path = "./assets/server/"

    def start_ftp_server(self, node, ip_address, ftp_port, disk_path):
        """Start an FTP server for a node."""
        authorizer = DummyAuthorizer()
        authorizer.add_user("user", "password", disk_path, perm="elradfmw")
        handler = CustomFTPHandler
        handler.authorizer = authorizer
        ftp_server = FTPServer(("0.0.0.0", ftp_port), handler)
        ftp_server.node = node
        self.ftp_servers[ip_address] = ftp_server
        ftp_thread = threading.Thread(target=ftp_server.serve_forever, daemon=True)
        ftp_thread.start()
        print(f"FTP server started on {ip_address}:{ftp_port}")

    def stop_ftp_server(self, ip_address):
        """Stop the FTP server for a given IP address."""
        if ip_address in self.ftp_servers:
            self.ftp_servers[ip_address].close_all()
            print(f"FTP server stopped for {ip_address}")
            del self.ftp_servers[ip_address]

    def check_target_storage(self, target_ip, size, total_storage):
        """Check if the target node has enough storage via FTP."""
        if target_ip not in self.ip_map and target_ip != self.server_ip:
            return False, f"Error: Target IP {target_ip} not found"
        try:
            ftp = ftplib.FTP()
            ftp.connect(host="127.0.0.1", port=self.ip_map.get(target_ip, {"ftp_port": self.server_port})["ftp_port"])
            ftp.login(user="user", passwd="password")
            files = []
            ftp.dir(lambda x: files.append(x))
            used_storage = 0
            for line in files:
                parts = line.split()
                if len(parts) > 4 and parts[0].startswith("-"):
                    file_size = int(parts[4])
                    if parts[-1] != "disk_metadata.json":
                        used_storage += file_size
            ftp.quit()
            if used_storage + size <= total_storage:
                return True, None
            return False, f"Error: Not enough storage on {target_ip}'s disk"
        except Exception as e:
            return False, f"Error checking storage on {target_ip}: {e}"

    def _get_unique_filename(self, filename, target_ip):
        """Generate a unique filename for the target node."""
        try:
            ftp = ftplib.FTP()
            ftp.connect(host="127.0.0.1", port=self.ip_map[target_ip]["ftp_port"])
            ftp.login(user="user", passwd="password")
            file_list = ftp.nlst()
            ftp.quit()
        except Exception:
            file_list = []
        base, ext = os.path.splitext(filename)
        counter = 1
        new_filename = filename
        while new_filename in file_list:
            new_filename = f"{base}_{counter}{ext}"
            counter += 1
        return new_filename

    def send_file(self, filename, source_ip, target_ip, virtual_disk, target_node_name=None):
        """Send a file to another node's disk or server using FTP."""
        if target_ip not in self.ip_map and target_ip != self.server_ip:
            return f"Error: Target IP {target_ip} not found"
        if source_ip == target_ip:
            return f"Error: Cannot send file to self"
        if filename not in virtual_disk:
            return f"Error: File {filename} does not exist"

        size = virtual_disk[filename]
        total_storage = 1024 * 1024 * 1024 if target_ip != self.server_ip else float('inf')
        can_store, error = self.check_target_storage(target_ip, size, total_storage)
        if not can_store:
            return error

        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_file.write(b"\0" * size)
            temp_file_path = temp_file.name

        try:
            ftp = ftplib.FTP()
            ftp.connect(host="127.0.0.1", port=self.ip_map.get(target_ip, {"ftp_port": self.server_port})["ftp_port"])
            ftp.login(user="user", passwd="password")
            start_time = time.time()
            print(f"Transfer started at {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(start_time))}")

            chunk_size = math.ceil(size / self.num_chunks)
            sent_bytes = 0
            chunk_count = 0
            with open(temp_file_path, 'rb') as f:
                while chunk_count < self.num_chunks and sent_bytes < size:
                    chunk_count += 1
                    remaining_bytes = size - sent_bytes
                    current_chunk_size = min(chunk_size, remaining_bytes)
                    chunk = f.read(current_chunk_size)
                    if not chunk:
                        break
                    header = f"CHUNK:{chunk_count}:{current_chunk_size}" + (f":{target_node_name}" if target_node_name else "") + "\n"
                    header = header.encode().ljust(self.header_size, b'\0')
                    chunk_with_header = header + chunk
                    with tempfile.NamedTemporaryFile(delete=False) as chunk_file:
                        chunk_file.write(chunk_with_header)
                        chunk_file_path = chunk_file.name
                    chunk_start_time = time.time()
                    with open(chunk_file_path, 'rb') as cf:
                        mode = 'STOR' if chunk_count == 1 else 'APPE'
                        ftp.storbinary(f"{mode} {filename}", cf)
                    os.unlink(chunk_file_path)
                    sent_bytes += current_chunk_size
                    elapsed_time = time.time() - chunk_start_time
                    expected_time = (current_chunk_size + self.header_size) / self.bandwidth_bytes_per_sec
                    sleep_time = max(0, expected_time - elapsed_time)
                    if sleep_time > 0:
                        time.sleep(sleep_time)
                    total_time = time.time() - chunk_start_time
                    print(f"Sent chunk {chunk_count}/5 ({current_chunk_size} bytes) for {filename} to {target_ip} in {total_time:.2f} seconds")

            end_time = time.time()
            ftp.quit()
            os.unlink(temp_file_path)
            print(f"Transfer ended at {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(end_time))}")
            print(f"Transferred {filename}: {size} bytes ({size / (1024 * 1024):.2f} MB)")
            print(f"Completed sending {filename} ({size} bytes) in {chunk_count} chunks to {target_ip}")
            return f"Sent {filename} ({size} bytes) to {target_ip}"
        except Exception as e:
            if os.path.exists(temp_file_path):
                os.unlink(temp_file_path)
            return f"Error sending file to {target_ip}: {e}"

    def forward_file(self, filename, target_node_name):
        """Forward pending files to the target node in a separate thread."""
        def forward_task(folder_name, target_ip, file_path, size, original_filename):
            try:
                target_filename = self._get_unique_filename(original_filename, target_ip)
                ftp = ftplib.FTP()
                ftp.connect(host="127.0.0.1", port=self.ip_map[target_ip]["ftp_port"])
                ftp.login(user="user", passwd="password")
                start_time = time.time()
                print(f"Forwarding started at {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(start_time))}")

                chunk_size = math.ceil(size / self.num_chunks)
                sent_bytes = 0
                chunk_count = 0
                with open(file_path, 'rb') as f:
                    while chunk_count < self.num_chunks and sent_bytes < size:
                        chunk_count += 1
                        remaining_bytes = size - sent_bytes
                        current_chunk_size = min(chunk_size, remaining_bytes)
                        chunk = f.read(current_chunk_size)
                        if not chunk:
                            break
                        header = f"CHUNK:{chunk_count}:{current_chunk_size}\n".encode().ljust(self.header_size, b'\0')
                        chunk_with_header = header + chunk
                        with tempfile.NamedTemporaryFile(delete=False) as chunk_file:
                            chunk_file.write(chunk_with_header)
                            chunk_file_path = chunk_file.name
                        chunk_start_time = time.time()
                        with open(chunk_file_path, 'rb') as cf:
                            mode = 'STOR' if chunk_count == 1 else 'APPE'
                            ftp.storbinary(f"{mode} {target_filename}", cf)
                        os.unlink(chunk_file_path)
                        sent_bytes += current_chunk_size
                        elapsed_time = time.time() - chunk_start_time
                        expected_time = (current_chunk_size + self.header_size) / self.bandwidth_bytes_per_sec
                        sleep_time = max(0, expected_time - elapsed_time)
                        if sleep_time > 0:
                            time.sleep(sleep_time)
                        total_time = time.time() - chunk_start_time
                        print(f"Forwarded chunk {chunk_count}/5 ({current_chunk_size} bytes) for {original_filename} as {target_filename} to {target_ip} in {total_time:.2f} seconds")

                end_time = time.time()
                ftp.quit()
                with self.manager.pending_files_lock:
                    if folder_name in self.manager.pending_files:
                        del self.manager.pending_files[folder_name]
                shutil.rmtree(os.path.dirname(file_path))
                print(f"Deleted folder {folder_name} from server after forwarding")
                print(f"Forwarding ended at {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(end_time))}")
                print(f"Forwarded {original_filename} as {target_filename}: {size} bytes ({size / (1024 * 1024):.2f} MB)")
                print(f"Completed forwarding {original_filename} ({size} bytes) in {chunk_count} chunks to {target_ip}")
            except Exception as e:
                print(f"Error forwarding file {original_filename} to {target_ip}: {e}")

        target_ip = None
        for ip, info in self.ip_map.items():
            if info["node_name"] == target_node_name:
                target_ip = ip
                break
        if not target_ip:
            print(f"Error: Target node {target_node_name} not found")
            return

        files_to_forward = []
        with self.manager.pending_files_lock:
            for folder_name, (tname, fname) in list(self.manager.pending_files.items()):
                if tname == target_node_name:
                    file_path = os.path.join(self.server_disk_path, folder_name, fname)
                    if os.path.exists(file_path):
                        size = os.path.getsize(file_path)
                        can_store, error = self.check_target_storage(target_ip, size, 1024 * 1024 * 1024)
                        if can_store:
                            files_to_forward.append((folder_name, file_path, size, fname))
                        else:
                            print(error)

        for folder_name, file_path, size, fname in files_to_forward:
            threading.Thread(target=forward_task, args=(folder_name, target_ip, file_path, size, fname), daemon=True).start()