import json
import tempfile
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from scripts import v220_bench_graph_http as bench


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def do_POST(self):
        body = self.rfile.read(int(self.headers["content-length"]))
        assert len(json.loads(body)["query"]) == 768
        reply = json.dumps({"ids": list(range(100)), "base_visits": 123,
                            "vector_body_gets": 0}).encode()
        self.send_response(200)
        self.send_header("content-length", str(len(reply)))
        self.end_headers()
        self.wfile.write(reply)

    def log_message(self, *_):
        pass


class BenchTest(unittest.TestCase):
    def test_preserves_every_response_and_latency(self):
        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            with tempfile.TemporaryDirectory() as directory, patch.object(bench, "COUNT", 8), \
                    patch.object(bench, "WORKERS", 2):
                root = Path(directory)
                requests = root / "requests.jsonl"
                requests.write_text("".join(json.dumps({"query_ordinal": index,
                    "query": [1.0] * 768}) + "\n" for index in range(8)))
                prep = root / "prep.json"
                prep.write_text(json.dumps({"schema": "borsuk-v217-graph-1m-preparation-v1",
                                            "requests_sha256": bench.digest(requests)}))
                args = SimpleNamespace(host="127.0.0.1", port=server.server_port,
                    pass_label="single_pass",
                    prep=prep, requests=requests, raw=root / "raw.jsonl",
                    summary=root / "summary.json")
                bench.run(args)
                rows = [json.loads(line) for line in args.raw.read_text().splitlines()]
                summary = json.loads(args.summary.read_text())
                self.assertEqual([row["ordinal"] for row in rows], list(range(8)))
                self.assertTrue(all(row["whole_ns"] > 0 and row["returned_ids"] ==
                                    list(range(100)) for row in rows))
                self.assertEqual(summary["raw_sha256"], bench.digest(args.raw))
                self.assertGreaterEqual(summary["p99_ns"], summary["p90_ns"])
        finally:
            server.shutdown()
            server.server_close()
            thread.join()


if __name__ == "__main__":
    unittest.main()
