# Build Your Own Redis — in Java

A minimal but **real, working** Redis clone: a TCP server on port 6379 that
speaks the RESP protocol, written in pure Java 21 with only the standard
library. You can drive it with `redis-cli` or any RESP client, exactly like
real Redis.

> Repo name (planned): **`build-your-own-redis-java`**
> This folder is a standalone repo in its own right: its root has its own
> `README.md`, `LICENSE` (MIT) and `.gitignore`. It is **not** uploaded to
> GitHub yet — no remote has been created and no credentials were used.

## What it does

- TCP server on port **6379**, speaking RESP (arrays of bulk strings), so
  `redis-cli` works against it.
- **Concurrent clients** — one Java 21 virtual thread per connection.
- In-memory key–value store with **expiry**: `SET` with `EX` (seconds) or
  `PX` (milliseconds); expired keys are removed lazily on access and by a
  background sweeper that runs every second.
- Also accepts plain inline commands (handy for `telnet` / `nc`).

### Commands that work

| Command | Behaviour |
|---|---|
| `PING [message]` | `+PONG`, or echoes the message |
| `ECHO message` | Echoes the message back as a bulk string |
| `SET key value [EX seconds \| PX ms]` | Stores a value, optionally with expiry; replies `+OK` |
| `GET key` | Value, or `nil` (`$-1`) if missing or expired |
| `DEL key [key …]` | Deletes keys; replies with the count deleted |
| `EXISTS key [key …]` | Replies with the count of existing keys |
| `TTL key` | Seconds to expiry; `-1` = no expiry, `-2` = missing key |
| `COMMAND`, `QUIT` | Minimal replies so `redis-cli` handshakes cleanly |

Unknown commands and wrong argument counts return `-ERR …` errors, like
real Redis.

## How to run it

Requirements: **Java 21+** and nothing else. No dependencies, no Docker,
no build tool.

```bash
# Option A — run the source directly (no build step)
java src/RedisServer.java
# redis-clone listening on port 6379 (RESP, Java 21)

# Option B — compile, then run
javac -d out src/RedisServer.java
java -cp out RedisServer

# Run on a different port
java src/RedisServer.java 6380
```

Then talk to it:

```bash
redis-cli -p 6379 PING            # PONG
redis-cli -p 6379 SET name luna  # OK
redis-cli -p 6379 GET name       # "luna"
redis-cli -p 6379 SET temp v PX 200
redis-cli -p 6379 GET temp       # (nil) after 200 ms

# Raw RESP, no client needed:
printf '*1\r\n$4\r\nPING\r\n' | nc localhost 6379   # +PONG
```

## What was tested

Tested on 2026-11-21 on this machine (Ubuntu, OpenJDK 21.0.13.1, Python
3.12.3 for the test harness). The suite is `tests/test_redis.py` — it starts
the real server on a scratch port and drives it over raw RESP sockets.

```bash
python3 tests/test_redis.py
```

**Result: 30 passed, 0 failed**, covering:

- `PING` (plain, with message, lowercase) and `ECHO`
- `SET` / `GET`, overwrite, missing key → `nil`
- `DEL`, `EXISTS`, `TTL` (including `-1` / `-2`)
- Expiry: `SET … PX 200` (gone after ~350 ms), `SET … EX 1` (gone after
  ~1.2 s), `TTL` counting down from 5
- Errors: unknown command, wrong argument count, bad `SET` option
- **Two concurrent clients** writing and reading different keys at the same
  time, each seeing its own value

Also verified by hand: the single-file run mode (`java src/RedisServer.java
16380`) answers `+PONG`, `+OK`, `$3\r\nbar` to raw RESP sent over a socket.

## Project layout

```
build-your-own-redis-java/
├── README.md               # this file
├── LICENSE                 # MIT
├── .gitignore
├── src/
│   └── RedisServer.java    # the whole server — one file, stdlib only
├── tests/
│   └── test_redis.py       # end-to-end test suite (30 checks)
└── demo/
    └── index.html          # static demo page (see "Live / Deploy")
```

## Live / Deploy

"Live" for a Redis clone does **not** mean a hosted Redis server — the
server is a TCP process that runs on a machine you control. What *can* go
live is the demo page in `demo/index.html`: a static page that explains the
clone, shows example RESP commands and outputs, and how to run it.

**Nothing has been deployed.** To put the demo page live later, any static
host works — no environment variables, no backend, no secrets:

- **GitHub Pages** (recommended once the repo exists): push the repo, then
  in repo Settings → Pages, set the source to the `demo/` folder (or move
  `demo/index.html` to the repo root / a `docs/` folder). The page is then
  live at `https://<username>.github.io/build-your-own-redis-java/`.
- **Netlify Drop / Vercel / Cloudflare Pages**: drag the `demo/` folder into
  the dashboard, or `npx vercel demo` — no build command, no env vars.
- **Any static host** (`python3 -m http.server` in `demo/`, nginx, S3): it
  is a single self-contained `.html` file.

To make the *server itself* reachable remotely later, run it on a VPS or
container host (`java src/RedisServer.java`, port 6379 open) — that is a
separate step from the static page, and it should sit behind a firewall /
auth before being exposed to the internet, since this clone has no
authentication.

### To take this project live (checklist)

1. Connect GitHub and create the repo **`build-your-own-redis-java`**
   (name to be confirmed).
2. `git init`, commit this folder, push to the new remote.
3. Enable GitHub Pages on the repo (source: `demo/` folder) — the demo page
   goes live; no env vars needed.
4. (Optional, later) Run the server on a VPS for a live TCP endpoint.

## Limitations

- **In-memory only** — all data is lost when the server stops. No RDB
  snapshots, no AOF log, no persistence of any kind.
- **Strings only** — no lists, sets, hashes, sorted sets, streams, or
  bitmaps.
- **No authentication** (`AUTH`), no TLS, no ACLs. Do not expose port 6379
  to the internet as-is.
- **No replication, clustering, or pub/sub.**
- No `SET` options beyond `EX`/`PX` (no `NX`/`XX`/`KEEPTTL`/`GET`).
- Values are held as Java strings; binary-safe byte handling is not
  implemented.
- Single process, single node; no config file (port is the only argument).

## Next steps (to make it fuller)

1. **Persistence** — RDB snapshot on shutdown / interval, and AOF append log
   replayed on startup.
2. **More data types** — lists (`LPUSH`/`RPOP`), sets (`SADD`/`SMEMBERS`),
   hashes (`HSET`/`HGET`).
3. **`SET` options** — `NX`, `XX`, `KEEPTTL`, `GET`.
4. **Pub/sub** — `SUBSCRIBE`/`PUBLISH` with per-client subscriptions.
5. **Transactions** — `MULTI`/`EXEC`/`DISCARD`.
6. **Replication** — a replica mode that streams the command log.
7. **Benchmarks** — throughput/latency numbers vs. real Redis on the same box.
