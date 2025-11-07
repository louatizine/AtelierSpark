import socketserver
import time
import random

LOG_LINES = [
    '127.0.0.1 - - [01/Jan/2025:00:00:00 +0000] "GET /ok HTTP/1.1" 200 123',
    '127.0.0.1 - - [01/Jan/2025:00:00:01 +0000] "GET /notfound HTTP/1.1" 404 0',
    '127.0.0.1 - - [01/Jan/2025:00:00:02 +0000] "POST /error HTTP/1.1" 501 0',
    '127.0.0.1 - - [01/Jan/2025:00:00:03 +0000] "GET /forbidden HTTP/1.1" 403 0',
    '127.0.0.1 - - [01/Jan/2025:00:00:04 +0000] "GET /ok HTTP/1.1" 200 456',
]

class LogTCPHandler(socketserver.BaseRequestHandler):
    def handle(self):
        print(f"Client connected: {self.client_address}")
        try:
            while True:
                # send 1-3 lines per second to simulate natural traffic
                for _ in range(random.randint(1, 3)):
                    line = random.choice(LOG_LINES) + "\n"
                    self.request.sendall(line.encode("utf-8"))
                time.sleep(1)
        except (ConnectionResetError, BrokenPipeError):
            print(f"Client disconnected: {self.client_address}")

if __name__ == "__main__":
    HOST, PORT = "0.0.0.0", 9999
    with socketserver.ThreadingTCPServer((HOST, PORT), LogTCPHandler) as server:
        server.allow_reuse_address = True
        print(f"Log generator listening on {HOST}:{PORT}")
        server.serve_forever()