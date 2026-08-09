#!/usr/bin/env python3
"""Compare the shop endpoint with Caffeine L1 enabled or disabled."""

import argparse
import http.client
import json
import math
import statistics
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urlparse


def percentile(values, percent):
    if not values:
        return 0.0
    ordered = sorted(values)
    index = max(0, math.ceil(percent / 100.0 * len(ordered)) - 1)
    return ordered[index]


def run_load(url, request_count, concurrency):
    parsed = urlparse(url)
    path = parsed.path or "/"
    if parsed.query:
        path += "?" + parsed.query
    port = parsed.port or (443 if parsed.scheme == "https" else 80)
    connection_type = (
        http.client.HTTPSConnection if parsed.scheme == "https" else http.client.HTTPConnection
    )
    barrier = threading.Barrier(concurrency + 1)

    def worker(worker_index):
        count = request_count // concurrency
        if worker_index < request_count % concurrency:
            count += 1
        connection = connection_type(parsed.hostname, port, timeout=10)
        latencies = []
        errors = 0
        barrier.wait()
        for _ in range(count):
            started = time.perf_counter()
            try:
                connection.request("GET", path, headers={"Connection": "keep-alive"})
                response = connection.getresponse()
                body = response.read()
                if response.status != 200 or b'"success":true' not in body or b'"id":1' not in body:
                    errors += 1
            except Exception:
                errors += 1
                connection.close()
                connection = connection_type(parsed.hostname, port, timeout=10)
            latencies.append((time.perf_counter() - started) * 1000.0)
        connection.close()
        return latencies, errors

    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = [executor.submit(worker, index) for index in range(concurrency)]
        barrier.wait()
        started = time.perf_counter()
        results = [future.result() for future in futures]
        duration = time.perf_counter() - started

    latencies = [latency for worker_latencies, _ in results for latency in worker_latencies]
    errors = sum(worker_errors for _, worker_errors in results)
    return {
        "requests": request_count,
        "concurrency": concurrency,
        "duration_seconds": round(duration, 3),
        "throughput_rps": round(request_count / duration, 2),
        "mean_ms": round(statistics.fmean(latencies), 3),
        "p50_ms": round(percentile(latencies, 50), 3),
        "p95_ms": round(percentile(latencies, 95), 3),
        "p99_ms": round(percentile(latencies, 99), 3),
        "errors": errors,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="http://127.0.0.1:18081/shop/1")
    parser.add_argument("--scenario", required=True)
    parser.add_argument("--requests", type=int, default=10000)
    parser.add_argument("--concurrency", type=int, default=40)
    parser.add_argument("--warmup", type=int, default=1000)
    args = parser.parse_args()

    if args.requests <= 0 or args.concurrency <= 0 or args.warmup < 0:
        parser.error("requests and concurrency must be positive; warmup must not be negative")
    if args.requests < args.concurrency:
        parser.error("requests must be greater than or equal to concurrency")

    if args.warmup:
        run_load(args.url, args.warmup, min(args.concurrency, args.warmup))
    result = run_load(args.url, args.requests, args.concurrency)
    result["scenario"] = args.scenario
    result["url"] = args.url
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()
