# Python WebSocket Learning Lab

This is a small native Python app for learning the core idea behind WebSockets.
It uses only Python's standard library: `socket`, `threading`, `hashlib`,
`base64`, and `struct`. No `tkinter`, browser, Node, or third-party packages are
required.

## Run

```bash
python3 websocket_lab.py
```

Then:

1. Type `/connect`.
2. Watch the HTTP upgrade handshake appear.
3. Type any message and press Enter.
4. Type `/ping` to see a round-trip message over the same open connection.
5. Type `/quit` to exit.

## What to learn from it

WebSocket starts as a normal HTTP request. The client asks the server to
`Upgrade: websocket`, and the server accepts with `101 Switching Protocols`.

After that, the connection stays open. Instead of making a new HTTP request for
every update, both sides send lightweight WebSocket frames over the same socket.

That pattern is useful for chat, notifications, live dashboards, multiplayer
games, collaboration tools, and streaming progress updates.

## Project files

- `websocket_lab.py`: terminal app, local WebSocket server, local WebSocket
  client, handshake code, and frame encoding/decoding.
- `README.md`: this guide.

## Notes

This app intentionally implements a tiny part of the WebSocket protocol so the
moving pieces are easy to see. Real production Python apps usually use a battle
tested package such as `websockets`, `aiohttp`, `FastAPI`, or `Django Channels`.
