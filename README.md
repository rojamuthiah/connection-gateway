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

The HTTP listener reads its port from the `PORT` environment variable and
defaults to `8000` when it is unset. `.env.example` documents the setting; it
is a template and is not loaded automatically by Erlang. Set the variable in
the shell before starting the application, for example in PowerShell:

    $env:PORT = "8000"
    rebar3 shell

Import `connection-gateway-erl.postman_collection.json` into Postman to create
requests for the tree-management and publish endpoints. Its `baseUrl` variable
defaults to `http://localhost:8000`.

Build
-----

    $ rebar3 compile
