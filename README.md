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

Build
-----

    $ rebar3 compile
