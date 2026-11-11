# Gateway benchmark scripts

This folder contains two client-side benchmark scripts:

* [`benchmark-gateways.mjs`](./benchmark-gateways.mjs) — JavaScript/Node.js
  client. Most settings are command-line options.
* [`benchmark_gateways.py`](./benchmark_gateways.py) — Python asyncio client.
  Settings for bad publishes are edited as constants near the top of the file;
  other settings can be passed as command-line options.

Neither script starts a gateway server. Start the Erlang gateway and the
JavaScript gateway first, then run one of these clients from the repository
root. Each script tests the Erlang gateway first and the JavaScript gateway
second, sequentially, to avoid load-test runs competing with each other.

## What a benchmark run does

For each gateway, the client:

1. Creates unique channels using `POST /channels`.
2. Opens the configured number of WebSocket connections and subscribes each
   connection to a channel.
3. Sends normal publishes and checks that every other subscriber receives the
   message. A publisher does not receive its own broadcast.
4. Optionally sends bad publishes, which run the gateway's CPU workload before
   broadcasting.
5. Reports channel creation and connection setup times, successful deliveries,
   publish timing, delivery latency, and errors, then closes its WebSockets.

Connection setup concurrency controls only how many connect/subscribe attempts
are in flight. It does not control publish concurrency.

## JavaScript benchmark: `benchmark-gateways.mjs`

### Run

From the repository root:

```powershell
node scripts/benchmark-gateways.mjs
```

See every accepted option and its current default:

```powershell
node scripts/benchmark-gateways.mjs --help
```

Example with a smaller test and bad publishing enabled:

```powershell
node scripts/benchmark-gateways.mjs `
  --channels 1 `
  --connections-per-channel 20 `
  --connection-concurrency 5 `
  --bad-message
```

### Settings

The `DEFAULTS` object at the top of `benchmark-gateways.mjs` provides defaults.
Command-line options override the corresponding values for a run.

| Setting | Default | Meaning |
|---|---:|---|
| `channels` / `--channels N` | `2` | Number of fresh channels created on each gateway. |
| `connectionsPerChannel` / `--connections-per-channel N` | `1000` | WebSocket subscribers created for each channel. Total WebSockets per gateway are `channels × connectionsPerChannel`. |
| `normalPublishes` / `--normal-publishes N` | `1` | Normal messages published to each channel. The script sends these sequentially from that channel's first connection. |
| `badMessage` / `--bad-message` | `false` | Enables one `publish_bad` per channel. Without the option, bad publishing is disabled. At least two connections per channel are required by this script when enabled. |
| `connectionConcurrency` / `--connection-concurrency N` | `100` | Maximum simultaneous WebSocket connection/subscription setup attempts. This is not publish concurrency. |
| `timeoutMs` / `--timeout-ms N` | `30000` ms | Timeout used while opening connections, awaiting subscriptions, creating channels, or waiting for message delivery. |
| `erlangUrl` / `--erlang-url URL` | `http://127.0.0.1:8001` | Erlang gateway HTTP base URL. The script derives its `ws` or `wss` WebSocket URL from this. |
| `jsUrl` / `--js-url URL` | `http://127.0.0.1:8080` | JavaScript gateway HTTP base URL. The script derives its WebSocket URL from this. |

The JavaScript benchmark currently sends one bad publish per channel when
`--bad-message` is set. Its bad publishes are sent sequentially across
channels; `--bad-message` does not accept a count.

## Python benchmark: `benchmark_gateways.py`

### Run

Install the Python dependency, then run the client from the repository root:

```powershell
py -m pip install -r requirements-benchmark.txt
py scripts/benchmark_gateways.py
```

The Python script accepts command-line options for channel count, connection
count, normal publish count, connection concurrency, timeout, and gateway
URLs. Use `--help` to see the current options:

```powershell
py scripts/benchmark_gateways.py --help
```

### Settings near the top of the Python file

Edit these constants in `benchmark_gateways.py` before running:

| Constant | Current default | Meaning |
|---|---:|---|
| `DEFAULT_CHANNELS` | `2` | Default number of channels created on each gateway. |
| `DEFAULT_CONNECTIONS_PER_CHANNEL` | `400` | Default WebSocket subscriber connections per channel. |
| `DEFAULT_NORMAL_PUBLISHES` | `1` | Default normal publishes per channel. |
| `DEFAULT_CONNECTION_CONCURRENCY` | `1` | Default maximum simultaneous connection/subscription attempts. |
| `DEFAULT_TIMEOUT_SECONDS` | `30.0` seconds | Default connection, response, and delivery timeout. |
| `DEFAULT_ERLANG_URL` | `http://127.0.0.1:8001` | Default Erlang gateway base URL. |
| `DEFAULT_JS_URL` | `http://127.0.0.1:8080` | Default JavaScript gateway base URL. |
| `ENABLE_BAD_MESSAGES` | `True` | Enables or disables the bad-publish phase. |
| `BAD_MESSAGES_PER_CHANNEL` | `10` | Number of bad publishes to issue per channel when enabled. |

When enabled, Python starts the configured bad publishes concurrently and uses
a different publisher connection for each one. Therefore
`--connections-per-channel` must be at least `BAD_MESSAGES_PER_CHANNEL`. This
is different from the JavaScript script, which currently sends one bad publish
per channel sequentially.

## Reading the results

* **Channel creation time** measures HTTP channel setup.
* **Connect + subscribe time** measures how long the client takes to establish
  and subscribe all requested WebSockets.
* **Deliveries** reports received messages versus expected messages. A publish
  is expected at every other subscriber, excluding the publisher.
* **Delivery latency** is measured from sending a publish until the expected
  subscribers have received it.
* **Publish phase time** covers the whole phase, including publish requests and
  waiting for their deliveries. With concurrent Python bad publishes, this is
  the elapsed phase time, not the sum of each individual publish latency.

These are local benchmark clients, so results include client scheduling,
machine load, socket limits, message delivery, and gateway work. Compare
repeated runs under similar conditions rather than treating one run as a
hardware-independent gateway ranking.
