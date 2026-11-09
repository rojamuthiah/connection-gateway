relay_tree
=====

An OTP application

Structure
---------

Application source is under `apps/relay_tree/src/`:

* `relay_tree` coordinates tree lifecycle, subscriptions, and broadcasts.
* `relay_tree_builder` creates relay workers and their topology metadata.
* `relay_tree_recovery` replaces failed relays and reconnects surviving workers.
  If a WebSocket client cannot be reattached to a replacement leaf after three
  attempts, recovery tries the other live leaves from least-loaded to most-
  loaded.
* `relay_server` implements one relay worker.
* `http_server` registers the HTTP routes.
* `http_handler`, `publish_handler`, and `websocket_handler` implement the
  health check, publish endpoint, and WebSocket endpoint respectively.
* `relay_tree_app` and `relay_tree_sup` start and supervise the application.
* `apps/connection_gateway/src/` contains the separate channel-based gateway
  application. Its channel manager owns channel membership, generated
  connection IDs, and the per-channel designated workload subscriber.

The HTTP API is:

* `GET /health` returns a JSON health response.
* `POST /publish` broadcasts a non-empty request body (up to 1 MiB).
* `GET /ws` upgrades to a WebSocket that receives published messages as text
  frames. Client frames are ignored.
* `POST /tree/create` creates a tree. Send a JSON body such as
  `{"layers": 4}`; `layers` must be between 0 and 10.
* `GET /tree` returns the root PID and relay/leaf counts.
* `GET /tree/relays` returns relay IDs, PIDs, levels, parents, and children.
* `GET /tree/leaves` returns the leaf relay details.

The tree-management endpoints use Erlang/OTP's standard `json` module and
therefore require OTP 27 or later.

### Channel gateway protocol

The separate `connection_gateway` application implements:

* `POST /channels` with `{"channelId":"stream-101"}` to create a channel.
* `GET /ws` for WebSocket `subscribe`, `publish_normal`, and `publish_bad`
  messages. Subscribe with
  `{"type":"subscribe","channelId":"stream-101"}`; the response includes the
  server-generated connection ID.
* The server generates each `connectionId` when its WebSocket connects.
  A successful subscription returns it in a `subscribed` message.
* Both `publish_normal` and `publish_bad` are broadcast as `message` events to
  every other subscriber in the channel; the publisher does not receive its
  own message.
* A `publish_bad` message also runs a fixed one-million-iteration CPU task in
  exactly one other subscriber's WebSocket process. If the channel's
  designated subscriber is the publisher, another subscriber is selected.
  Further bad publishes to that channel are rejected while its current task
  runs, but the publish message is still broadcast to the other subscribers.

The relay-tree listener reads `PORT` and defaults to `8000`. The channel
gateway reads `CONNECTION_GATEWAY_PORT` and defaults to `8080`. `.env.example`
documents both values. Erlang does not load `.env` automatically; when using a
local `.env`, pass it to Rebar3:

    $env:PORT = "8000"
    rebar3 shell --env-file .env

`rebar.config` configures the shell to start both OTP applications. Import
`connection-gateway-erl.postman_collection.json` into Postman for the relay
tree API and `connection-gateway-channels.postman_collection.json` for the
channel gateway.

Build
-----

    $ rebar3 compile
