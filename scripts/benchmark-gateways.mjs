#!/usr/bin/env node

import os from "node:os";
import { performance } from "node:perf_hooks";

const DEFAULTS = {
  channels: 2,
  connectionsPerChannel: 1000,
  normalPublishes: 1,
  connectionConcurrency: 100,
  timeoutMs: 30000,
  erlangUrl: "http://127.0.0.1:8001",
  jsUrl: "http://127.0.0.1:8080",
  badMessage: false,
};

function parseArgs(argv) {
  const options = { ...DEFAULTS };
  const numericOptions = new Map([
    ["--channels", "channels"],
    ["--connections-per-channel", "connectionsPerChannel"],
    ["--normal-publishes", "normalPublishes"],
    ["--connection-concurrency", "connectionConcurrency"],
    ["--timeout-ms", "timeoutMs"],
  ]);

  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];
    if (arg === "--help" || arg === "-h") {
      printHelp();
      process.exit(0);
    }
    if (arg === "--bad-message") {
      options.badMessage = true;
      continue;
    }
    if (numericOptions.has(arg)) {
      const key = numericOptions.get(arg);
      const rawValue = argv[index + 1];
      if (!rawValue || rawValue.startsWith("--")) {
        throw new Error(`Missing value for ${arg}`);
      }
      const value = Number(rawValue);
      if (!Number.isSafeInteger(value)) {
        throw new Error(`${arg} must be an integer`);
      }
      options[key] = value;
      index += 1;
      continue;
    }
    if (arg === "--erlang-url" || arg === "--js-url") {
      const rawValue = argv[index + 1];
      if (!rawValue || rawValue.startsWith("--")) {
        throw new Error(`Missing value for ${arg}`);
      }
      options[arg === "--erlang-url" ? "erlangUrl" : "jsUrl"] = rawValue;
      index += 1;
      continue;
    }
    throw new Error(`Unknown option: ${arg}`);
  }

  if (options.channels < 1) throw new Error("--channels must be at least 1");
  if (options.connectionsPerChannel < 1) {
    throw new Error("--connections-per-channel must be at least 1");
  }
  if (options.badMessage && options.connectionsPerChannel < 2) {
    throw new Error("--bad-message requires at least 2 connections per channel");
  }
  if (options.normalPublishes < 1) {
    throw new Error("--normal-publishes must be at least 1");
  }
  if (options.connectionConcurrency < 1) {
    throw new Error("--connection-concurrency must be at least 1");
  }
  if (options.timeoutMs < 1) throw new Error("--timeout-ms must be positive");
  return options;
}

function printHelp() {
  console.log(`Usage: node scripts/benchmark-gateways.mjs [options]

Runs each gateway sequentially: create channels, connect and subscribe
WebSocket clients, publish normal messages, then optionally publish bad ones.

Options:
  --channels N                    Channels per gateway (default: ${DEFAULTS.channels})
  --connections-per-channel N     WebSocket subscribers per channel (default: ${DEFAULTS.connectionsPerChannel})
  --normal-publishes N            Normal publishes per channel (default: ${DEFAULTS.normalPublishes})
  --bad-message                   Also run one publish_bad per channel
  --connection-concurrency N      Concurrent connection/subscription attempts (default: ${DEFAULTS.connectionConcurrency})
  --timeout-ms N                  Timeout for connect/subscribe/delivery (default: ${DEFAULTS.timeoutMs})
  --erlang-url URL                Erlang HTTP base URL (default: ${DEFAULTS.erlangUrl})
  --js-url URL                    JavaScript HTTP base URL (default: ${DEFAULTS.jsUrl})
  -h, --help                      Show this help

Example:
  node scripts/benchmark-gateways.mjs --channels 2 --connections-per-channel 1000
  node scripts/benchmark-gateways.mjs --channels 2 --connections-per-channel 1000 --bad-message`);
}

function percentile(values, percentileValue) {
  if (values.length === 0) return 0;
  const sorted = [...values].sort((left, right) => left - right);
  const index = Math.ceil(percentileValue * sorted.length) - 1;
  return sorted[Math.max(0, index)];
}

function summarizePhase(results) {
  const latencies = results.map((result) => result.latencyMs);
  const expected = results.reduce((sum, result) => sum + result.expected, 0);
  const delivered = results.reduce((sum, result) => sum + result.delivered, 0);
  return {
    publishCount: results.length,
    expected,
    delivered,
    failures: results.filter((result) => result.error).length,
    p50Ms: percentile(latencies, 0.5),
    p95Ms: percentile(latencies, 0.95),
    maxMs: latencies.length ? Math.max(...latencies) : 0,
    elapsedMs: latencies.reduce((sum, latency) => sum + latency, 0),
  };
}

function makeMessageId(runId, gatewayName, channelIndex, kind, publishIndex) {
  return `${runId}-${gatewayName}-channel-${channelIndex}-${kind}-${publishIndex}`;
}

async function createChannels(baseUrl, channelIds, timeoutMs) {
  const started = performance.now();
  for (const channelId of channelIds) {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeoutMs);
    try {
      const response = await fetch(new URL("/channels", baseUrl), {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ channelId }),
        signal: controller.signal,
      });
      const body = await response.text();
      if (response.status !== 201) {
        throw new Error(
          `POST /channels returned ${response.status}: ${body.slice(0, 300)}`,
        );
      }
      const parsed = JSON.parse(body);
      if (parsed.status !== "created" || parsed.channelId !== channelId) {
        throw new Error(`Unexpected channel creation response: ${body}`);
      }
    } finally {
      clearTimeout(timer);
    }
  }
  return performance.now() - started;
}

class GatewayClient {
  constructor(socket, timeoutMs) {
    this.socket = socket;
    this.timeoutMs = timeoutMs;
    this.queue = [];
    this.waiters = new Set();
    this.closed = false;

    socket.addEventListener("message", (event) => {
      let value;
      try {
        value = JSON.parse(String(event.data));
      } catch {
        this.rejectWaiters(new Error("Received non-JSON WebSocket frame"));
        return;
      }

      if (value?.type === "error") {
        this.rejectWaiters(
          new Error(`WebSocket server error: ${JSON.stringify(value)}`),
        );
        return;
      }

      const waiter = [...this.waiters].find((item) => item.predicate(value));
      if (waiter) {
        this.finishWaiter(waiter, undefined, value);
      } else {
        this.queue.push(value);
      }
    });

    socket.addEventListener("close", (event) => {
      this.closed = true;
      this.rejectWaiters(
        new Error(`WebSocket closed (${event.code}): ${event.reason}`),
      );
    });

    socket.addEventListener("error", () => {
      this.rejectWaiters(new Error("WebSocket connection error"));
    });
  }

  waitFor(predicate, description) {
    const queuedIndex = this.queue.findIndex(predicate);
    if (queuedIndex !== -1) {
      return Promise.resolve(this.queue.splice(queuedIndex, 1)[0]);
    }
    if (this.closed) {
      return Promise.reject(new Error("WebSocket is closed"));
    }

    return new Promise((resolve, reject) => {
      const waiter = {
        predicate,
        resolve,
        reject,
        timer: setTimeout(() => {
          this.finishWaiter(
            waiter,
            new Error(`Timed out waiting for ${description}`),
          );
        }, this.timeoutMs),
      };
      this.waiters.add(waiter);
    });
  }

  send(value) {
    if (this.socket.readyState !== WebSocket.OPEN) {
      throw new Error("Cannot send on a WebSocket that is not open");
    }
    this.socket.send(JSON.stringify(value));
  }

  finishWaiter(waiter, error, value) {
    if (!this.waiters.delete(waiter)) return;
    clearTimeout(waiter.timer);
    if (error) waiter.reject(error);
    else waiter.resolve(value);
  }

  rejectWaiters(error) {
    for (const waiter of this.waiters) {
      this.finishWaiter(waiter, error);
    }
  }

  async close() {
    if (this.socket.readyState === WebSocket.CLOSED) return;
    await new Promise((resolve) => {
      const timer = setTimeout(resolve, 1000);
      this.socket.addEventListener(
        "close",
        () => {
          clearTimeout(timer);
          resolve();
        },
        { once: true },
      );
      if (this.socket.readyState === WebSocket.CONNECTING) {
        this.socket.addEventListener(
          "open",
          () => this.socket.close(),
          { once: true },
        );
      } else {
        this.socket.close();
      }
    });
  }

  waitUntilOpen() {
    if (this.socket.readyState === WebSocket.OPEN) return Promise.resolve();
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        cleanup();
        reject(new Error("Timed out waiting for WebSocket connection"));
      }, this.timeoutMs);
      const onOpen = () => {
        cleanup();
        resolve();
      };
      const onError = () => {
        cleanup();
        reject(new Error("WebSocket connection error"));
      };
      const cleanup = () => {
        clearTimeout(timer);
        this.socket.removeEventListener("open", onOpen);
        this.socket.removeEventListener("error", onError);
      };
      this.socket.addEventListener("open", onOpen, { once: true });
      this.socket.addEventListener("error", onError, { once: true });
    });
  }
}

async function connectAndSubscribe(wsUrl, channelId, timeoutMs) {
  const socket = new WebSocket(wsUrl);
  const client = new GatewayClient(socket, timeoutMs);
  try {
    await client.waitUntilOpen();
  } catch (error) {
    await client.close();
    throw new Error(`Could not connect to ${wsUrl}: ${error.message}`);
  }

  const subscription = client.waitFor(
    (message) =>
      message?.type === "subscribed" && message.channelId === channelId,
    `subscription to ${channelId}`,
  );
  client.send({ type: "subscribe", channelId });
  const response = await subscription;
  if (typeof response.connectionId !== "string") {
    throw new Error(`Server did not return a connectionId for ${channelId}`);
  }
  return { client, connectionId: response.connectionId };
}

async function mapWithConcurrency(items, concurrency, callback) {
  let nextIndex = 0;
  const results = new Array(items.length);
  const workerCount = Math.min(concurrency, items.length);

  await Promise.all(
    Array.from({ length: workerCount }, async () => {
      while (true) {
        const index = nextIndex;
        nextIndex += 1;
        if (index >= items.length) return;
        results[index] = await callback(items[index], index);
      }
    }),
  );
  return results;
}

async function connectSubscribers(wsUrl, channelIds, options) {
  const tasks = channelIds.flatMap((channelId, channelIndex) =>
    Array.from(
      { length: options.connectionsPerChannel },
      (_, connectionIndex) => ({ channelId, channelIndex, connectionIndex }),
    ),
  );
  const channelClients = channelIds.map(() => []);
  let completed = 0;

  await mapWithConcurrency(
    tasks,
    options.connectionConcurrency,
    async ({ channelId, channelIndex }) => {
      const subscription = await connectAndSubscribe(
        wsUrl,
        channelId,
        options.timeoutMs,
      );
      channelClients[channelIndex].push(subscription);
      completed += 1;
      if (completed % 100 === 0 || completed === tasks.length) {
        process.stdout.write(
          `\r    connected and subscribed ${completed}/${tasks.length}`,
        );
      }
    },
  );
  if (tasks.length) console.log();
  return channelClients;
}

async function publishAndMeasure(
  clients,
  publisher,
  channelId,
  message,
  type,
  timeoutMs,
) {
  const recipients = clients.filter((client) => client !== publisher);
  const waits = recipients.map((client) =>
    client.client.waitFor(
      (event) =>
        event?.type === "message" &&
        event.channelId === channelId &&
        event.message === message,
      `${type} delivery to channel ${channelId}`,
    ),
  );
  const started = performance.now();

  try {
    publisher.client.send({ type, channelId, message });
  } catch (error) {
    await Promise.allSettled(waits);
    return {
      expected: recipients.length,
      delivered: 0,
      latencyMs: performance.now() - started,
      error: error.message,
    };
  }

  const results = await Promise.allSettled(waits);
  const delivered = results.filter((result) => result.status === "fulfilled")
    .length;
  const firstError = results.find((result) => result.status === "rejected");
  return {
    expected: recipients.length,
    delivered,
    latencyMs: performance.now() - started,
    error: firstError?.reason?.message,
  };
}

async function runGateway(name, baseUrl, channelIds, options, runId) {
  const wsUrl = new URL("/ws", baseUrl);
  wsUrl.protocol = wsUrl.protocol === "https:" ? "wss:" : "ws:";
  const result = {
    name,
    baseUrl,
    channelCount: channelIds.length,
    connectionsPerChannel: options.connectionsPerChannel,
    createChannelsMs: 0,
    connectAndSubscribeMs: 0,
    connected: 0,
    normal: [],
    bad: [],
    errors: [],
  };
  let channelClients = [];

  try {
    result.createChannelsMs = await createChannels(
      baseUrl,
      channelIds,
      options.timeoutMs,
    );

    let started = performance.now();
    channelClients = await connectSubscribers(
      wsUrl,
      channelIds,
      options,
    );
    result.connectAndSubscribeMs = performance.now() - started;
    result.connected = channelClients.reduce(
      (sum, clients) => sum + clients.length,
      0,
    );

    started = performance.now();
    for (let channelIndex = 0; channelIndex < channelIds.length; channelIndex += 1) {
      const clients = channelClients[channelIndex];
      const publisher = clients[0];
      for (let publishIndex = 0; publishIndex < options.normalPublishes; publishIndex += 1) {
        const message = makeMessageId(
          runId,
          name,
          channelIndex,
          "normal",
          publishIndex,
        );
        result.normal.push(
          await publishAndMeasure(
            clients,
            publisher,
            channelIds[channelIndex],
            message,
            "publish_normal",
            options.timeoutMs,
          ),
        );
      }
    }
    result.normalElapsedMs = performance.now() - started;

    if (options.badMessage) {
      started = performance.now();
      for (let channelIndex = 0; channelIndex < channelIds.length; channelIndex += 1) {
        const clients = channelClients[channelIndex];
        const message = makeMessageId(
          runId,
          name,
          channelIndex,
          "bad",
          0,
        );
        result.bad.push(
          await publishAndMeasure(
            clients,
            clients[0],
            channelIds[channelIndex],
            message,
            "publish_bad",
            options.timeoutMs,
          ),
        );
      }
      result.badElapsedMs = performance.now() - started;
    }
  } catch (error) {
    result.errors.push(error.message);
  } finally {
    await Promise.all(
      channelClients.flatMap((clients) =>
        clients.map(({ client }) => client.close()),
      ),
    );
  }

  return result;
}

function printGatewayResult(result, options) {
  console.log(`\n${result.name} (${result.baseUrl})`);
  console.log(`  Channels created:             ${result.channelCount}`);
  console.log(
    `  Channel creation time:        ${result.createChannelsMs?.toFixed(2) ?? "n/a"} ms`,
  );
  console.log(
    `  WebSocket connections:        ${result.connected}/${result.channelCount * result.connectionsPerChannel}`,
  );
  console.log(
    `  Connect + subscribe time:     ${result.connectAndSubscribeMs?.toFixed(2) ?? "n/a"} ms`,
  );
  printPhase("Normal publish", result.normal, result.normalElapsedMs);
  if (options.badMessage) {
    printPhase("Bad publish", result.bad, result.badElapsedMs);
  }
  for (const error of result.errors) {
    console.error(`  ERROR: ${error}`);
  }
}

function printPhase(name, results, elapsedMs) {
  if (!results) return;
  const phase = summarizePhase(results);
  const rate = elapsedMs > 0 ? (phase.delivered * 1000) / elapsedMs : 0;
  console.log(`  ${name}:`);
  console.log(
    `    publishes: ${phase.publishCount}; deliveries: ${phase.delivered}/${phase.expected}; failed publishes: ${phase.failures}`,
  );
  console.log(
    `    publish phase: ${elapsedMs?.toFixed(2) ?? "n/a"} ms; delivery rate: ${rate.toFixed(2)} messages/s`,
  );
  console.log(
    `    per-publish delivery latency: p50 ${phase.p50Ms.toFixed(2)} ms; p95 ${phase.p95Ms.toFixed(2)} ms; max ${phase.maxMs.toFixed(2)} ms`,
  );
  for (const result of results) {
    if (result.error) console.error(`    delivery error: ${result.error}`);
  }
}

async function main() {
  let options;
  try {
    options = parseArgs(process.argv.slice(2));
  } catch (error) {
    console.error(`Error: ${error.message}\n`);
    printHelp();
    process.exitCode = 2;
    return;
  }

  const runId = `${Date.now()}-${process.pid}`;
  const targets = [
    { name: "Erlang", url: options.erlangUrl },
    { name: "JavaScript", url: options.jsUrl },
  ];

  console.log("Gateway benchmark");
  console.log(`  Node.js: ${process.version}`);
  console.log(`  OS: ${os.platform()} ${os.release()} (${os.arch()})`);
  console.log(`  Channels per gateway: ${options.channels}`);
  console.log(
    `  Connections per channel: ${options.connectionsPerChannel}`,
  );
  console.log(
    `  Total connections per gateway: ${options.channels * options.connectionsPerChannel}`,
  );
  console.log(
    `  Normal publishes per channel: ${options.normalPublishes}`,
  );
  console.log(`  Bad-message phase: ${options.badMessage ? "enabled" : "disabled"}`);
  console.log(`  Connection concurrency: ${options.connectionConcurrency}`);
  console.log(`  Timeout: ${options.timeoutMs} ms`);
  console.log("  Gateways run sequentially to avoid competing with each other.");

  const results = [];
  for (const target of targets) {
    const channelIds = Array.from(
      { length: options.channels },
      (_, index) => `bench-${runId}-${target.name.toLowerCase()}-${index + 1}`,
    );
    console.log(`\nRunning ${target.name} gateway...`);
    const result = await runGateway(
      target.name,
      target.url,
      channelIds,
      options,
      runId,
    );
    printGatewayResult(result, options);
    results.push(result);
  }

  const failed = results.some(
    (result) =>
      result.errors.length > 0 ||
      result.normal.some((item) => item.error) ||
      result.bad.some((item) => item.error) ||
      result.connected !== result.channelCount * result.connectionsPerChannel,
  );
  if (failed) process.exitCode = 1;
}

await main();
