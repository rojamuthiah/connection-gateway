#!/usr/bin/env python3
"""Benchmark the Erlang and JavaScript channel gateways."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import platform
import sys
import time
import urllib.error
import urllib.request
import uuid
from collections import defaultdict, deque
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urljoin, urlsplit, urlunsplit

import websockets
from websockets.asyncio.client import ClientConnection, connect


DEFAULT_CHANNELS = 2
DEFAULT_CONNECTIONS_PER_CHANNEL = 400
DEFAULT_NORMAL_PUBLISHES = 1
DEFAULT_CONNECTION_CONCURRENCY = 1
DEFAULT_TIMEOUT_SECONDS = 30.0
DEFAULT_ERLANG_URL = "http://127.0.0.1:8001"
DEFAULT_JS_URL = "http://127.0.0.1:8080"
ENABLE_BAD_MESSAGES = True
BAD_MESSAGES_PER_CHANNEL = 10


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Create channels, connect and subscribe WebSocket clients, then "
            "benchmark normal publishes against both gateways."
        )
    )
    parser.add_argument("--channels", type=positive_int, default=DEFAULT_CHANNELS)
    parser.add_argument(
        "--connections-per-channel",
        type=positive_int,
        default=DEFAULT_CONNECTIONS_PER_CHANNEL,
        help=f"Default: {DEFAULT_CONNECTIONS_PER_CHANNEL}",
    )
    parser.add_argument(
        "--normal-publishes",
        type=positive_int,
        default=DEFAULT_NORMAL_PUBLISHES,
        help="Normal publishes per channel. Default: 1",
    )
    parser.add_argument(
        "--connection-concurrency",
        type=positive_int,
        default=DEFAULT_CONNECTION_CONCURRENCY,
        help=f"Maximum simultaneous connection attempts. Default: {DEFAULT_CONNECTION_CONCURRENCY}",
    )
    parser.add_argument(
        "--timeout",
        type=positive_float,
        default=DEFAULT_TIMEOUT_SECONDS,
        help="Connect, response, and delivery timeout in seconds. Default: 30",
    )
    parser.add_argument("--erlang-url", default=DEFAULT_ERLANG_URL)
    parser.add_argument("--js-url", default=DEFAULT_JS_URL)
    args = parser.parse_args()
    if ENABLE_BAD_MESSAGES:
        if BAD_MESSAGES_PER_CHANNEL < 1:
            parser.error("BAD_MESSAGES_PER_CHANNEL must be at least 1")
        if args.connections_per_channel < BAD_MESSAGES_PER_CHANNEL:
            parser.error(
                "BAD_MESSAGES_PER_CHANNEL cannot exceed "
                "--connections-per-channel because each concurrent bad publish "
                "uses a separate publishing connection"
            )
    return args


def positive_int(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be an integer") from exc
    if parsed < 1:
        raise argparse.ArgumentTypeError("must be at least 1")
    return parsed


def positive_float(value: str) -> float:
    try:
        parsed = float(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be a number") from exc
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be greater than 0")
    return parsed


def websocket_url(http_url: str) -> str:
    parts = urlsplit(http_url)
    scheme = {"http": "ws", "https": "wss"}.get(parts.scheme)
    if scheme is None:
        raise ValueError(f"Gateway URL must start with http:// or https://: {http_url}")
    return urlunsplit((scheme, parts.netloc, "/ws", "", ""))


def post_json(base_url: str, path: str, payload: dict[str, Any], timeout: float):
    url = urljoin(base_url.rstrip("/") + "/", path.lstrip("/"))
    request = urllib.request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            response_body = response.read().decode("utf-8")
            return response.status, json.loads(response_body)
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"POST {url} returned {exc.code}: {body[:300]}") from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"Could not reach {url}: {exc.reason}") from exc


async def create_channels(
    base_url: str, channel_ids: list[str], timeout: float
) -> float:
    started = time.perf_counter()
    for channel_id in channel_ids:
        status, response = await asyncio.to_thread(
            post_json,
            base_url,
            "/channels",
            {"channelId": channel_id},
            timeout,
        )
        if status != 201 or response != {
            "status": "created",
            "channelId": channel_id,
        }:
            raise RuntimeError(f"Unexpected channel creation response: {response}")
    return (time.perf_counter() - started) * 1000


@dataclass
class Subscriber:
    connection: ClientConnection
    channel_id: str
    connection_id: str
    pending_events: dict[
        tuple[str, str, str], deque[dict[str, Any]]
    ] = field(default_factory=lambda: defaultdict(deque))
    event_waiters: dict[
        tuple[str, str, str], list[asyncio.Future[dict[str, Any]]]
    ] = field(default_factory=lambda: defaultdict(list))
    reader_error: str | None = None
    reader_task: asyncio.Task[None] | None = None

    async def read_messages(self) -> None:
        try:
            async for frame in self.connection:
                try:
                    message = json.loads(frame)
                except (json.JSONDecodeError, TypeError) as exc:
                    raise RuntimeError("Received a non-JSON WebSocket frame") from exc
                if message.get("type") == "error":
                    raise RuntimeError(f"WebSocket server error: {message}")
                key = (
                    message.get("type"),
                    message.get("channelId"),
                    message.get("message"),
                )
                waiters = self.event_waiters.get(key)
                while waiters:
                    waiter = waiters.pop(0)
                    if not waiter.done():
                        waiter.set_result(message)
                        break
                else:
                    self.pending_events[key].append(message)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self.reader_error = str(exc)
            for waiters in self.event_waiters.values():
                for waiter in waiters:
                    if not waiter.done():
                        waiter.set_exception(RuntimeError(self.reader_error))

    async def wait_for_event(
        self, expected_type: str, expected_channel: str, expected_message: str, timeout: float
    ) -> dict[str, Any]:
        key = (expected_type, expected_channel, expected_message)
        if self.reader_error is not None:
            raise RuntimeError(self.reader_error)

        pending = self.pending_events.get(key)
        if pending:
            message = pending.popleft()
            if not pending:
                self.pending_events.pop(key, None)
            return message

        waiter = asyncio.get_running_loop().create_future()
        self.event_waiters[key].append(waiter)
        try:
            return await asyncio.wait_for(waiter, timeout)
        except TimeoutError as exc:
            raise TimeoutError(
                f"Timed out waiting for {expected_type} delivery "
                f"to channel {expected_channel}"
            ) from exc
        finally:
            waiters = self.event_waiters.get(key)
            if waiters is not None:
                try:
                    waiters.remove(waiter)
                except ValueError:
                    pass
                if not waiters:
                    self.event_waiters.pop(key, None)

    async def close(self) -> None:
        if self.reader_task is not None:
            self.reader_task.cancel()
            await asyncio.gather(self.reader_task, return_exceptions=True)
        await self.connection.close()


async def connect_and_subscribe(
    ws_url: str, channel_id: str, timeout: float
) -> Subscriber:
    try:
        connection = await connect(
            ws_url,
            open_timeout=timeout,
            close_timeout=2,
            ping_interval=None,
            compression=None,
            max_size=1024 * 1024,
        )
    except Exception as exc:
        raise RuntimeError(
            "opening handshake failed; server_connection_id=not received: "
            f"{exc}"
        ) from exc

    try:
        await connection.send(
            json.dumps({"type": "subscribe", "channelId": channel_id})
        )
        response = json.loads(await asyncio.wait_for(connection.recv(), timeout))
        if response.get("type") != "subscribed":
            connection_id = response.get("connectionId")
            connection_detail = (
                f"server_connection_id={connection_id}"
                if isinstance(connection_id, str)
                else "server_connection_id=not provided by gateway"
            )
            raise RuntimeError(
                f"subscription rejected: {response}; {connection_detail}"
            )
        if response.get("channelId") != channel_id:
            raise RuntimeError(
                "subscription acknowledged for an unexpected channel: "
                f"{response}"
            )
        connection_id = response.get("connectionId")
        if not isinstance(connection_id, str):
            raise RuntimeError(
                "subscription acknowledged without a server-generated connectionId"
            )

        subscriber = Subscriber(connection, channel_id, connection_id)
        subscriber.reader_task = asyncio.create_task(subscriber.read_messages())
        return subscriber
    except Exception:
        await connection.close()
        raise


async def connect_subscribers(
    ws_url: str,
    channel_ids: list[str],
    connections_per_channel: int,
    concurrency: int,
    timeout: float,
) -> tuple[list[list[Subscriber]], list[str]]:
    tasks = [
        (channel_index, channel_id, attempt_number)
        for channel_index, channel_id in enumerate(channel_ids)
        for attempt_number in range(1, connections_per_channel + 1)
    ]
    semaphore = asyncio.Semaphore(concurrency)
    channel_subscribers: list[list[Subscriber]] = [
        [] for _ in channel_ids
    ]
    completed = 0

    async def worker(
        channel_index: int, channel_id: str, attempt_number: int
    ) -> None:
        nonlocal completed
        try:
            async with semaphore:
                subscriber = await connect_and_subscribe(
                    ws_url, channel_id, timeout
                )
        except Exception as exc:
            raise RuntimeError(
                f"channel={channel_id} connection_attempt="
                f"{attempt_number}/{connections_per_channel}: {exc}"
            ) from exc
        channel_subscribers[channel_index].append(subscriber)
        completed += 1
        if completed % 100 == 0 or completed == len(tasks):
            print(
                f"\r    connected and subscribed {completed}/{len(tasks)}",
                end="",
                flush=True,
            )

    results = await asyncio.gather(
        *(
            worker(channel_index, channel_id, attempt_number)
            for channel_index, channel_id, attempt_number in tasks
        ),
        return_exceptions=True,
    )
    if tasks:
        print()
    errors = [str(result) for result in results if isinstance(result, BaseException)]
    return channel_subscribers, errors


async def close_subscribers(channel_subscribers: list[list[Subscriber]]) -> None:
    await asyncio.gather(
        *(
            subscriber.close()
            for subscribers in channel_subscribers
            for subscriber in subscribers
        ),
        return_exceptions=True,
    )


async def publish_and_measure(
    subscribers: list[Subscriber],
    publisher: Subscriber,
    channel_id: str,
    message: str,
    message_type: str,
    timeout: float,
) -> dict[str, Any]:
    recipients = [subscriber for subscriber in subscribers if subscriber is not publisher]
    waits = [
        asyncio.create_task(
            recipient.wait_for_event("message", channel_id, message, timeout)
        )
        for recipient in recipients
    ]
    started = time.perf_counter()
    try:
        await publisher.connection.send(
            json.dumps(
                {
                    "type": message_type,
                    "channelId": channel_id,
                    "message": message,
                }
            )
        )
    except Exception as exc:
        await asyncio.gather(*waits, return_exceptions=True)
        return {
            "expected": len(recipients),
            "delivered": 0,
            "latency_ms": (time.perf_counter() - started) * 1000,
            "error": str(exc),
        }

    results = await asyncio.gather(*waits, return_exceptions=True)
    delivered = sum(not isinstance(result, BaseException) for result in results)
    errors = [str(result) for result in results if isinstance(result, BaseException)]
    return {
        "expected": len(recipients),
        "delivered": delivered,
        "latency_ms": (time.perf_counter() - started) * 1000,
        "error": errors[0] if errors else None,
    }


def percentile(values: list[float], value: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = max(0, min(len(ordered) - 1, int(value * len(ordered) + 0.999999) - 1))
    return ordered[index]


def phase_summary(results: list[dict[str, Any]]) -> dict[str, Any]:
    latencies = [result["latency_ms"] for result in results]
    return {
        "publishes": len(results),
        "expected": sum(result["expected"] for result in results),
        "delivered": sum(result["delivered"] for result in results),
        "failures": sum(result["error"] is not None for result in results),
        "p50_ms": percentile(latencies, 0.50),
        "p95_ms": percentile(latencies, 0.95),
        "max_ms": max(latencies, default=0.0),
    }


def make_message_id(
    run_id: str, gateway_name: str, channel_index: int, kind: str, publish_index: int
) -> str:
    return f"{run_id}-{gateway_name}-channel-{channel_index}-{kind}-{publish_index}"


async def run_gateway(
    name: str,
    base_url: str,
    channel_ids: list[str],
    args: argparse.Namespace,
    run_id: str,
) -> dict[str, Any]:
    result: dict[str, Any] = {
        "name": name,
        "base_url": base_url,
        "channels": len(channel_ids),
        "connections_per_channel": args.connections_per_channel,
        "connected": 0,
        "errors": [],
    }
    channel_subscribers: list[list[Subscriber]] = []
    ws_url = websocket_url(base_url)

    try:
        result["channel_creation_ms"] = await create_channels(
            base_url, channel_ids, args.timeout
        )

        started = time.perf_counter()
        channel_subscribers, connection_errors = await connect_subscribers(
            ws_url,
            channel_ids,
            args.connections_per_channel,
            args.connection_concurrency,
            args.timeout,
        )
        result["connect_and_subscribe_ms"] = (time.perf_counter() - started) * 1000
        result["connected"] = sum(len(items) for items in channel_subscribers)
        if connection_errors:
            details = "; ".join(connection_errors[:5])
            raise RuntimeError(
                f"{len(connection_errors)} of "
                f"{args.channels * args.connections_per_channel} WebSocket "
                f"connections failed: {details}"
            )

        normal_results = []
        started = time.perf_counter()
        for channel_index, channel_id in enumerate(channel_ids):
            subscribers = channel_subscribers[channel_index]
            publisher = subscribers[0]
            for publish_index in range(args.normal_publishes):
                message = make_message_id(
                    run_id, name.lower(), channel_index, "normal", publish_index
                )
                normal_results.append(
                    await publish_and_measure(
                        subscribers,
                        publisher,
                        channel_id,
                        message,
                        "publish_normal",
                        args.timeout,
                    )
                )
        result["normal_publish_elapsed_ms"] = (time.perf_counter() - started) * 1000
        result["normal"] = normal_results

        if ENABLE_BAD_MESSAGES:
            started = time.perf_counter()
            bad_publishes = []
            for channel_index, channel_id in enumerate(channel_ids):
                subscribers = channel_subscribers[channel_index]
                for publish_index in range(BAD_MESSAGES_PER_CHANNEL):
                    message = make_message_id(
                        run_id,
                        name.lower(),
                        channel_index,
                        "bad",
                        publish_index,
                    )
                    bad_publishes.append(
                        publish_and_measure(
                            subscribers,
                            subscribers[publish_index],
                            channel_id,
                            message,
                            "publish_bad",
                            args.timeout,
                        )
                    )
            bad_results = await asyncio.gather(*bad_publishes)
            result["bad_publish_elapsed_ms"] = (time.perf_counter() - started) * 1000
            result["bad"] = bad_results
    except Exception as exc:
        result["errors"].append(str(exc))
    finally:
        await close_subscribers(channel_subscribers)

    return result


def print_phase(
    name: str, results: list[dict[str, Any]], elapsed_ms: float | None
) -> None:
    summary = phase_summary(results)
    rate = (
        summary["delivered"] * 1000 / elapsed_ms
        if elapsed_ms and elapsed_ms > 0
        else 0
    )
    print(f"  {name}:")
    print(
        f"    publishes: {summary['publishes']}; deliveries: "
        f"{summary['delivered']}/{summary['expected']}; "
        f"failed publishes: {summary['failures']}"
    )
    print(
        f"    phase: {elapsed_ms:.2f} ms; delivery rate: {rate:.2f} messages/s"
        if elapsed_ms is not None
        else "    phase: n/a"
    )
    print(
        f"    delivery latency: p50 {summary['p50_ms']:.2f} ms; "
        f"p95 {summary['p95_ms']:.2f} ms; max {summary['max_ms']:.2f} ms"
    )
    for item in results:
        if item["error"]:
            print(f"    delivery error: {item['error']}")


def print_gateway_result(result: dict[str, Any], args: argparse.Namespace) -> None:
    expected_connections = result["channels"] * result["connections_per_channel"]
    print(f"\n{result['name']} ({result['base_url']})")
    creation_time = result.get("channel_creation_ms")
    setup_time = result.get("connect_and_subscribe_ms")
    print(f"  Channels created:             {result['channels']}")
    print(
        "  Channel creation time:        "
        f"{creation_time:.2f} ms" if creation_time is not None else
        "  Channel creation time:        n/a"
    )
    print(f"  WebSocket connections:        {result['connected']}/{expected_connections}")
    print(
        "  Connect + subscribe time:     "
        f"{setup_time:.2f} ms" if setup_time is not None else
        "  Connect + subscribe time:     n/a"
    )
    if "normal" in result:
        print_phase(
            "Normal publish",
            result["normal"],
            result.get("normal_publish_elapsed_ms"),
        )
    if ENABLE_BAD_MESSAGES and "bad" in result:
        print_phase(
            "Bad publish",
            result["bad"],
            result.get("bad_publish_elapsed_ms"),
        )
    for error in result["errors"]:
        print(f"  ERROR: {error}")


async def main() -> int:
    args = parse_args()
    run_id = f"{int(time.time() * 1000)}-{os.getpid()}-{uuid.uuid4().hex[:8]}"
    targets = [
        ("Erlang", args.erlang_url),
        ("JavaScript", args.js_url),
    ]

    print("Gateway benchmark")
    print(f"  Python: {platform.python_version()}")
    print(f"  websockets: {websockets.__version__}")
    print(f"  OS: {platform.platform()}")
    print(f"  CPU count: {os.cpu_count()}")
    print(f"  Channels per gateway: {args.channels}")
    print(f"  Connections per channel: {args.connections_per_channel}")
    print(
        "  Total connections per gateway: "
        f"{args.channels * args.connections_per_channel}"
    )
    print(f"  Normal publishes per channel: {args.normal_publishes}")
    print(
        "  Bad-message phase: "
        f"{'enabled' if ENABLE_BAD_MESSAGES else 'disabled'}"
    )
    print(
        "  Bad publishes per channel: "
        f"{BAD_MESSAGES_PER_CHANNEL if ENABLE_BAD_MESSAGES else 0}"
    )
    print(f"  Connection concurrency: {args.connection_concurrency}")
    print(f"  Timeout: {args.timeout:g} seconds")
    print("  Gateways run sequentially to avoid competing with each other.")

    failed = False
    for gateway_name, base_url in targets:
        channel_ids = [
            f"bench-{run_id}-{gateway_name.lower()}-{index + 1}"
            for index in range(args.channels)
        ]
        print(f"\nRunning {gateway_name} gateway...")
        result = await run_gateway(
            gateway_name, base_url, channel_ids, args, run_id
        )
        print_gateway_result(result, args)
        expected = args.channels * args.connections_per_channel
        failed |= bool(result["errors"]) or result["connected"] != expected
        for phase in ("normal", "bad"):
            failed |= any(item["error"] for item in result.get(phase, []))

    return 1 if failed else 0


if __name__ == "__main__":
    try:
        raise SystemExit(asyncio.run(main()))
    except KeyboardInterrupt:
        print("\nBenchmark interrupted.", file=sys.stderr)
        raise SystemExit(130)
