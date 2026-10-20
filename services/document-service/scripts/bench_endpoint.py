"""Load/latency probe for POST /api/v1/documents/process.

Usage:
    python scripts/bench_endpoint.py --url http://127.0.0.1:8000 --documents 1000 --iterations 20

Reports p50/p95/p99 latency and verifies the 202 contract. It is intentionally
dependency-free (stdlib only) so it can run inside any container.
"""

from __future__ import annotations

import argparse
import json
import statistics
import time
import urllib.request
from datetime import datetime, timezone


def build_payload(count: int, batch_id: str) -> bytes:
    now = datetime.now(timezone.utc).isoformat()
    documents = [
        {
            "document_id": f"bench-{batch_id}-{index}",
            "document_type": "invoice",
            "issuer_tax_id": "900123456-7",
            "amount_cents": 125_000 + index,
            "currency": "COP",
            "issued_at": now,
        }
        for index in range(count)
    ]

    return json.dumps({"batch_id": batch_id, "documents": documents}).encode()


def percentile(values: list[float], fraction: float) -> float:
    ordered = sorted(values)
    index = min(len(ordered) - 1, max(0, int(round(fraction * (len(ordered) - 1)))))

    return ordered[index]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="http://127.0.0.1:8000")
    parser.add_argument("--documents", type=int, default=1000)
    parser.add_argument("--iterations", type=int, default=20)
    args = parser.parse_args()

    latencies: list[float] = []

    for iteration in range(1, args.iterations + 1):
        batch_id = f"bench-{int(time.time())}-{iteration}"
        body = build_payload(args.documents, batch_id)
        request = urllib.request.Request(
            f"{args.url.rstrip('/')}/api/v1/documents/process",
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )

        started = time.perf_counter()

        with urllib.request.urlopen(request, timeout=10) as response:
            status = response.status
            payload = json.loads(response.read())

        elapsed_ms = (time.perf_counter() - started) * 1000
        latencies.append(elapsed_ms)

        assert status == 202, f"expected 202, got {status}"
        assert payload["accepted"] == args.documents

    print(f"documents per request : {args.documents}")
    print(f"requests              : {args.iterations}")
    print(f"p50 latency           : {percentile(latencies, 0.50):.2f} ms")
    print(f"p95 latency           : {percentile(latencies, 0.95):.2f} ms")
    print(f"p99 latency           : {percentile(latencies, 0.99):.2f} ms")
    print(f"max latency           : {max(latencies):.2f} ms")
    print(f"mean latency          : {statistics.mean(latencies):.2f} ms")
    print(f"throughput            : {args.documents * args.iterations / (sum(latencies) / 1000):,.0f} docs/s (accepted)")

    return 0 if percentile(latencies, 0.99) < 200 else 1


if __name__ == "__main__":
    raise SystemExit(main())
