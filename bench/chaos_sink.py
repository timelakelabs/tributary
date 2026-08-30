#!/usr/bin/env python3
"""A fault-injecting mock TimeLakeDB for the chaos drills (tributary#61).

Every `/write` is diced against configurable probabilities:

  - success        record the body, 204
  - 5xx            503, do NOT record
  - reset          RST the connection with no response, do NOT record
  - latency        sleep, then record + 204 (a slow-but-good write)
  - ambiguous-ack  record the body, then RST *before* the 204 — the write
                   LANDED but the client cannot know, so it retries, and a real
                   primary-key store collapses the duplicate. This is the one
                   fault that records without acking; every other fault records
                   nothing, so a plain failure never reads as a delivery.

Records to a plain line-protocol file, so the drill can assert the *distinct*
idx set is complete (nothing lost) while the raw count exceeds it (the
at-least-once duplicates a real DB dedups).

  chaos_sink.py <received.lp> [host:port]

Env (probabilities in [0,1]; the remainder is success). All default to a
moderately hostile mix:
  CHAOS_5XX=0.15 CHAOS_RESET=0.15 CHAOS_LATENCY=0.10 CHAOS_AMBIGUOUS=0.10
  CHAOS_SEED=1        deterministic dice
  CHAOS_LAT_MAX=0.8   max latency-fault sleep (seconds)
"""
import http.server
import os
import random
import socket
import struct
import sys
import time

LOG = sys.argv[1]
HOST, PORT = (sys.argv[2].split(":") if len(sys.argv) > 2 else ("127.0.0.1", "8899"))
P_5XX = float(os.environ.get("CHAOS_5XX", "0.15"))
P_RESET = float(os.environ.get("CHAOS_RESET", "0.15"))
P_LAT = float(os.environ.get("CHAOS_LATENCY", "0.10"))
P_AMB = float(os.environ.get("CHAOS_AMBIGUOUS", "0.10"))
LAT_MAX = float(os.environ.get("CHAOS_LAT_MAX", "0.8"))
_rng = random.Random(int(os.environ.get("CHAOS_SEED", "1")))


def _dice():
    # One lock-free-enough source of dice; the GIL serialises the reads, which
    # is fine — determinism per seed is what matters, not throughput.
    return _rng.random()


def _record(body):
    with open(LOG, "ab") as f:
        f.write(body if body.endswith(b"\n") else body + b"\n")


class Handler(http.server.BaseHTTPRequestHandler):
    def do_POST(self):
        n = int(self.headers.get("content-length", 0))
        body = self.rfile.read(n)
        r = _dice()
        if r < P_5XX:
            self.send_response(503)
            self.end_headers()
        elif r < P_5XX + P_RESET:
            self._reset()
        elif r < P_5XX + P_RESET + P_LAT:
            time.sleep(_rng.uniform(0.1, LAT_MAX))
            _record(body)
            self.send_response(204)
            self.end_headers()
        elif r < P_5XX + P_RESET + P_LAT + P_AMB:
            _record(body)  # the write landed...
            self._reset()  # ...but the ack never arrives → the client retries
        else:
            _record(body)
            self.send_response(204)
            self.end_headers()

    def _reset(self):
        # SO_LINGER (1, 0) makes close() send a RST rather than a graceful FIN,
        # so the client sees a connection reset, the way a crashing server does.
        try:
            self.connection.setsockopt(
                socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0)
            )
            self.connection.close()
        except OSError:
            pass

    def log_message(self, *_a):
        pass


def main():
    server = http.server.ThreadingHTTPServer((HOST, int(PORT)), Handler)
    server.daemon_threads = True
    server.serve_forever()


if __name__ == "__main__":
    main()
