import base64
import hashlib
import os
import socket
import struct
import threading
import time


HOST = "127.0.0.1"
PORT = 8765
WS_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"


def log(message):
    print(f"{time.strftime('%H:%M:%S')}  {message}", flush=True)


class WebSocketProtocol:
    TEXT = 0x1
    CLOSE = 0x8
    PING = 0x9
    PONG = 0xA

    @staticmethod
    def encode(payload, opcode=TEXT, mask=False):
        if isinstance(payload, str):
            payload = payload.encode("utf-8")

        first_byte = 0x80 | opcode
        length = len(payload)
        mask_bit = 0x80 if mask else 0

        if length < 126:
            header = bytes([first_byte, mask_bit | length])
        elif length < 65536:
            header = bytes([first_byte, mask_bit | 126]) + struct.pack("!H", length)
        else:
            header = bytes([first_byte, mask_bit | 127]) + struct.pack("!Q", length)

        if not mask:
            return header + payload

        mask_key = os.urandom(4)
        masked_payload = bytes(byte ^ mask_key[index % 4] for index, byte in enumerate(payload))
        return header + mask_key + masked_payload

    @staticmethod
    def read(sock):
        first_two = recv_exact(sock, 2)
        if not first_two:
            return None, b""

        first_byte, second_byte = first_two
        opcode = first_byte & 0x0F
        masked = bool(second_byte & 0x80)
        length = second_byte & 0x7F

        if length == 126:
            length_bytes = recv_exact(sock, 2)
            if not length_bytes:
                return None, b""
            length = struct.unpack("!H", length_bytes)[0]
        elif length == 127:
            length_bytes = recv_exact(sock, 8)
            if not length_bytes:
                return None, b""
            length = struct.unpack("!Q", length_bytes)[0]

        mask_key = recv_exact(sock, 4) if masked else b""
        payload = recv_exact(sock, length) if length else b""

        if masked:
            payload = bytes(byte ^ mask_key[index % 4] for index, byte in enumerate(payload))

        return opcode, payload


def recv_exact(sock, length):
    chunks = []
    remaining = length
    while remaining:
        chunk = sock.recv(remaining)
        if not chunk:
            return b""
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def read_http_headers(sock):
    data = b""
    while b"\r\n\r\n" not in data:
        chunk = sock.recv(1024)
        if not chunk:
            break
        data += chunk
    return data.decode("utf-8", errors="replace")


def parse_headers(raw_request):
    lines = raw_request.split("\r\n")
    headers = {}
    for line in lines[1:]:
        if ":" in line:
            key, value = line.split(":", 1)
            headers[key.strip().lower()] = value.strip()
    return lines[0], headers


class WebSocketServer:
    def __init__(self):
        self.server_socket = None
        self.clients = {}
        self.next_client_id = 1
        self.running = threading.Event()
        self.lock = threading.Lock()

    def start(self):
        self.running.set()
        thread = threading.Thread(target=self._serve, daemon=True)
        thread.start()

    def stop(self):
        self.running.clear()
        if self.server_socket:
            try:
                self.server_socket.close()
            except OSError:
                pass

        with self.lock:
            clients = list(self.clients.values())
            self.clients.clear()

        for client_socket in clients:
            try:
                client_socket.close()
            except OSError:
                pass

    def _serve(self):
        try:
            self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.server_socket.bind((HOST, PORT))
            self.server_socket.listen()
            self.server_socket.settimeout(0.4)
            log(f"Server listening on ws://{HOST}:{PORT}")
        except OSError as error:
            self.running.clear()
            log(f"Could not start server: {error}")
            return

        while self.running.is_set():
            try:
                client_socket, address = self.server_socket.accept()
            except socket.timeout:
                continue
            except OSError:
                break

            thread = threading.Thread(
                target=self._handle_client,
                args=(client_socket, address),
                daemon=True,
            )
            thread.start()

    def _handle_client(self, client_socket, address):
        try:
            request = read_http_headers(client_socket)
            request_line, headers = parse_headers(request)
            log(f"Handshake: received HTTP request: {request_line}")

            websocket_key = headers.get("sec-websocket-key")
            if not websocket_key:
                log("Handshake rejected: missing Sec-WebSocket-Key")
                client_socket.close()
                return

            accept_key = base64.b64encode(
                hashlib.sha1((websocket_key + WS_GUID).encode("utf-8")).digest()
            ).decode("ascii")

            response = (
                "HTTP/1.1 101 Switching Protocols\r\n"
                "Upgrade: websocket\r\n"
                "Connection: Upgrade\r\n"
                f"Sec-WebSocket-Accept: {accept_key}\r\n"
                "\r\n"
            )
            client_socket.sendall(response.encode("ascii"))
            log("Handshake: sent 101 Switching Protocols")

            with self.lock:
                client_id = self.next_client_id
                self.next_client_id += 1
                self.clients[client_id] = client_socket

            log(f"Client {client_id} connected from {address[0]}:{address[1]}")
            self._send_text(client_socket, f"Welcome. You are client {client_id}.")
            self.broadcast(f"Client {client_id} joined.")

            while self.running.is_set():
                opcode, payload = WebSocketProtocol.read(client_socket)
                if opcode is None:
                    break

                if opcode == WebSocketProtocol.TEXT:
                    text = payload.decode("utf-8", errors="replace")
                    log(f"Frame: client {client_id} sent text: {text}")
                    self.broadcast(f"Client {client_id}: {text}")
                elif opcode == WebSocketProtocol.PING:
                    log(f"Frame: client {client_id} sent ping")
                    client_socket.sendall(WebSocketProtocol.encode(payload, WebSocketProtocol.PONG))
                elif opcode == WebSocketProtocol.CLOSE:
                    break
        except OSError as error:
            log(f"Client connection error: {error}")
        finally:
            self._remove_client(client_socket)

    def _remove_client(self, client_socket):
        removed_id = None
        with self.lock:
            for client_id, existing_socket in list(self.clients.items()):
                if existing_socket is client_socket:
                    removed_id = client_id
                    del self.clients[client_id]
                    break

        try:
            client_socket.close()
        except OSError:
            pass

        if removed_id:
            log(f"Client {removed_id} disconnected")
            self.broadcast(f"Client {removed_id} left.")

    def broadcast(self, text):
        with self.lock:
            clients = list(self.clients.values())

        for client_socket in clients:
            self._send_text(client_socket, text)

    def _send_text(self, client_socket, text):
        try:
            client_socket.sendall(WebSocketProtocol.encode(text, WebSocketProtocol.TEXT))
        except OSError:
            self._remove_client(client_socket)


class WebSocketClient:
    def __init__(self):
        self.sock = None
        self.connected = threading.Event()
        self.lock = threading.Lock()

    def connect(self):
        if self.connected.is_set():
            log("Client is already connected")
            return

        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((HOST, PORT))

            key = base64.b64encode(os.urandom(16)).decode("ascii")
            request = (
                "GET /chat HTTP/1.1\r\n"
                f"Host: {HOST}:{PORT}\r\n"
                "Upgrade: websocket\r\n"
                "Connection: Upgrade\r\n"
                f"Sec-WebSocket-Key: {key}\r\n"
                "Sec-WebSocket-Version: 13\r\n"
                "\r\n"
            )
            sock.sendall(request.encode("ascii"))
            log("Client: sent HTTP Upgrade request")

            response = read_http_headers(sock)
            status_line = response.split("\r\n", 1)[0]
            if "101" not in status_line:
                raise OSError(f"Upgrade rejected: {status_line}")

            with self.lock:
                self.sock = sock

            self.connected.set()
            log(f"Client: server replied {status_line}")

            thread = threading.Thread(target=self._read_loop, args=(sock,), daemon=True)
            thread.start()
        except OSError as error:
            self.connected.clear()
            log(f"Client connection failed: {error}")

    def disconnect(self):
        with self.lock:
            sock = self.sock

        if not sock:
            return

        try:
            sock.sendall(WebSocketProtocol.encode(b"", WebSocketProtocol.CLOSE, mask=True))
            sock.close()
        except OSError:
            pass
        self.connected.clear()
        log("Client disconnected")

    def send_text(self, text):
        if not self.connected.is_set():
            log("Connect first with /connect")
            return

        self._send_frame(WebSocketProtocol.encode(text, WebSocketProtocol.TEXT, mask=True))
        log(f"Client: sent text frame: {text}")

    def ping(self):
        if not self.connected.is_set():
            log("Connect first with /connect")
            return

        sent_at = str(time.time()).encode("ascii")
        self._send_frame(WebSocketProtocol.encode(sent_at, WebSocketProtocol.PING, mask=True))
        log("Client: sent ping frame")

    def _read_loop(self, sock):
        try:
            while self.connected.is_set():
                opcode, payload = WebSocketProtocol.read(sock)
                if opcode is None:
                    break

                if opcode == WebSocketProtocol.TEXT:
                    text = payload.decode("utf-8", errors="replace")
                    log(f"Client received text: {text}")
                elif opcode == WebSocketProtocol.PONG:
                    try:
                        sent_at = float(payload.decode("ascii"))
                        latency_ms = round((time.time() - sent_at) * 1000, 1)
                        log(f"Client received pong: {latency_ms} ms")
                    except ValueError:
                        log("Client received pong")
                elif opcode == WebSocketProtocol.CLOSE:
                    break
        except OSError:
            pass
        finally:
            self.connected.clear()
            with self.lock:
                if self.sock is sock:
                    self.sock = None
            try:
                sock.close()
            except OSError:
                pass
            log("Client read loop ended")

    def _send_frame(self, frame):
        with self.lock:
            sock = self.sock

        if not sock:
            return

        try:
            sock.sendall(frame)
        except OSError as error:
            self.connected.clear()
            log(f"Send failed: {error}")


def print_help():
    print(
        """
Commands:
  /connect       connect the built-in client to the local server
  /ping          send a WebSocket ping frame
  /quit          close the client, stop the server, and exit
  /help          show this help

Any other text is sent as a WebSocket text frame.
""".strip()
    )


def main():
    server = WebSocketServer()
    client = WebSocketClient()

    print("Python WebSocket Learning Lab")
    print("This starts a local server and lets a built-in client connect to it.")
    print_help()
    server.start()

    try:
        while True:
            try:
                command = input("> ").strip()
            except EOFError:
                break

            if not command:
                continue
            if command == "/connect":
                client.connect()
            elif command == "/ping":
                client.ping()
            elif command == "/help":
                print_help()
            elif command == "/quit":
                break
            else:
                client.send_text(command[:240])
    except KeyboardInterrupt:
        print()
    finally:
        client.disconnect()
        server.stop()
        log("Goodbye")


if __name__ == "__main__":
    main()
