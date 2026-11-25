from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


def _handler(engine):
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path == "/healthz":
                body, status = b"ok", 200
            elif self.path == "/status":
                body, status = json.dumps(engine.snapshot()).encode(), 200
            else:
                body, status = b"not found", 404
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args):
            pass

    return Handler


class HealthServer:
    def __init__(self, engine, port: int) -> None:
        self._server = ThreadingHTTPServer(("0.0.0.0", port), _handler(engine))

    def start(self) -> None:
        threading.Thread(target=self._server.serve_forever, daemon=True).start()

    def stop(self) -> None:
        self._server.shutdown()