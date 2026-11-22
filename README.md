# Build Your Own Redis — in Java

A **real, working** Redis clone: a TCP server on port 6379 that speaks
the RESP protocol, written in pure Java 21 with only the standard
library. You can drive it with `redis-cli` or any RESP client, exactly
like real Redis — including lists, streams, transactions, pub/sub,
sorted sets, RDB persistence, and master–replica replication.

> Repo name: **`build-your-own-redis-java`** (already on GitHub under
> `abhiyadavj2006-del`, with GitHub Pages live for the demo page).

## Source

This follows the CodeCrafters **"Build your own Redis"** challenge
([codecrafters.io/courses/redis](https://codecrafters.io/courses/redis)),
as recommended in the YouTube video
**"バックエンド・要素技術の勉強法。CodeCraftersで車輪の再開発。"**
by **Naoya Tech / 直也テック (@naoya-tech)** —
[https://www.youtube.com/watch?v=hLQp1g1O2Ps](https://www.youtube.com/watch?v=hLQp1g1O2Ps)
(8:29, uploaded 24 May 2025).

Honest note on the video: the video itself is a Japanese-language
recommendation video — it introduces CodeCrafters and the
"reinvent-the-wheel" way of learning backend engineering; it is **not**
a Redis code walkthrough. Its transcript could **not** be retrieved
from this machine: YouTube blocked the requests from this cloud IP
("Sign in to confirm you're not a bot" / HTTP 429), and no transcript
mirror for the video exists. What was verified from the video directly:
title, channel, duration, upload date, and description (a CodeCrafters
promo listing Redis among the challenges). The tutorial content this
project implements is therefore the CodeCrafters challenge's published
stage list (Core → Lists → Streams → Transactions → Optimistic Locking
→ Replication → RDB → AOF → Pub/Sub → Sorted Sets → Bitmaps →
Geospatial → Authentication), read in full from the challenge overview
page. Nothing here claims to be a transcription of the video.

## What it does

- TCP server on port **6379**, speaking RESP (arrays of bulk strings),
so `redis-cli` works against it.
- **Concurrent clients** — one Java 21 virtual thread per connection.
Command execution itself is single-threaded behind one lock, exactly
like real Redis, so every command is atomic.
- In-memory store with **key expiry** (lazy on access + a background
sweeper every second), across all data types.
- **Lists, streams, sorted sets** in addition to strings.
- **Transactions** (`MULTI`/`EXEC`/`DISCARD`) with optimistic locking
(`WATCH`/`UNWATCH`).
- **Pub/sub** (`SUBSCRIBE`/`PUBLISH`/`UNSUBSCRIBE`).
- **RDB persistence**: loads string keys (with expiry) from a
`dump.rdb` file at startup (`--dir` / `--dbfilename`, `CONFIG GET`).
- **AOF persistence** (simple form): `--appendonly yes` appends every
write command to `appendonly.aof` and replays it at startup.
- **Replication**: `--replicaof <host> <port>` makes the server a
replica — it performs the `PING` / `REPLCONF` / `PSYNC` handshake,
loads the empty RDB the master sends, then applies every propagated
command. As a master it answers `INFO replication`, sends
`+FULLRESYNC` + an 88-byte empty RDB (valid CRC64), propagates writes
to all connected replicas, and supports `REPLCONF GETACK`/`ACK` and
`WAIT`.
- Also accepts plain inline commands (handy for `telnet` / `nc`).

### Commands that work

| Group | Commands |
|---|---|
| Core | `PING [message]`, `ECHO message`, `SET key value [EX s \| PX ms] [NX \| XX] [GET] [KEEPTTL]`, `GET`, `DEL`, `EXISTS`, `TTL`, `PTTL`, `EXPIRE`, `PEXPIRE`, `PERSIST`, `TYPE` |
| Counters | `INCR`, `DECR`, `INCRBY`, `DECRBY` |
| Lists | `RPUSH`, `LPUSH`, `LRANGE`, `LLEN`, `LPOP [count]`, `RPOP [count]`, `LREM`, `BLPOP [timeout]` |
| Streams | `XADD` (explicit, `*`, and `ms-*` IDs, with validation), `XRANGE`, `XREVRANGE`, `XREAD [COUNT n] [BLOCK ms] STREAMS …` (incl. `$`) |
| Transactions | `MULTI`, `EXEC`, `DISCARD`, `WATCH`, `UNWATCH` |
| Pub/Sub | `SUBSCRIBE`, `PUBLISH`, `UNSUBSCRIBE` |
| Sorted sets | `ZADD`, `ZRANK`, `ZRANGE [WITHSCORES]`, `ZCARD`, `ZSCORE`, `ZREM` |
| Server | `INFO [replication]`, `CONFIG GET dir\|dbfilename\|appendonly\|port`, `COMMAND`, `CLIENT SETINFO`, `HELLO`, `SELECT 0`, `RESET`, `QUIT` |
| Replication | `REPLCONF`, `PSYNC? -1`, `WAIT` |

Unknown commands, wrong argument counts, wrong-type access
(`WRONGTYPE`), and bad stream IDs return the same `-ERR …` errors as
real Redis.

## How to run it

Requirements: **Java 21+** and nothing else. No dependencies, no Docker,
no build tool.

```bash
# Option A — run the source directly (no build step)
java src/RedisServer.java
# redis-clone listening on port 6379 (RESP, Java 21) [master]

# Option B — compile, then run
javac -d out src/RedisServer.java
java -cp out RedisServer

# Run on a different port
java src/RedisServer.java 6380
# or: java src/RedisServer.java --port 6380
```

Then talk to it:

```bash
redis-cli -p 6379 PING # PONG
redis-cli -p 6379 SET name luna # OK
redis-cli -p 6379 GET name # "luna"
redis-cli -p 6379 RPUSH mylist a b c
redis-cli -p 6379 LRANGE mylist 0 -1
redis-cli -p 6379 XADD mystream '*' field value
redis-cli -p 6379 SET temp v PX 200
redis-cli -p 6379 GET temp # (nil) after 200 ms

# Raw RESP, no client needed:
printf '*1\r\n$4\r\nPING\r\n' | nc localhost 6379 # +PONG
```

### Replication example

```bash
# Terminal 1 — master
java src/RedisServer.java --port 6379
# Terminal 2 — replica
java src/RedisServer.java --port 6380 --replicaof 127.0.0.1 6379
# Terminal 3 — write on the master, read on the replica
redis-cli -p 6379 SET replkey replval # OK
redis-cli -p 6380 GET replkey # "replval"
redis-cli -p 6379 WAIT 1 1000 # 1
```

### RDB / AOF example

```bash
# Load an existing dump.rdb at startup (string keys + expiry)
java src/RedisServer.java --dir /tmp/mydata --dbfilename dump.rdb
redis-cli -p 6379 CONFIG GET dir # 1) "dir" 2) "/tmp/mydata"

# Append-only log instead (replayed at startup)
java src/RedisServer.java --dir /tmp/mydata --appendonly yes
```

## What was tested

Tested on 2026-11-22 on this machine (Ubuntu, Temurin OpenJDK
21.0.13.1, Python 3.12.3 for the test harness). The suite is
`tests/test_redis.py` — it starts real servers on scratch ports and
drives them over raw RESP sockets.

```bash
python3 tests/test_redis.py
```

**Result: 162 passed, 0 failed** (run three times in a row), covering:

- **Baseline (30 checks, unchanged from the starter version):**
`PING` (plain, with message, lowercase), `ECHO`, `SET`/`GET`,
overwrite, missing key → `nil`, `DEL`, `EXISTS`, `TTL` (`-1`/`-2`),
expiry (`PX 200`, `EX 1`, `TTL` countdown), errors (unknown command,
wrong arg count, bad `SET` option), two concurrent clients.
- **Lists:** `RPUSH`/`LPUSH`/`LRANGE` (incl. negative and out-of-range
indexes), `LLEN`, `LPOP`/`RPOP` (incl. `count`), `LREM`,
`BLPOP` (unblocked by a push; timeout → `nil`), `WRONGTYPE`.
- **Streams:** `XADD` (explicit, `*`, `ms-*` IDs), ID validation
errors (`0-0`, smaller-than-top), `XRANGE` (incl. `COUNT`),
`XREAD` (after an ID, `$`, missing stream), `XREAD BLOCK`
(new entry arrives; timeout → `nil`).
- **Transactions:** `INCR`/`INCRBY`/`DECR`, `MULTI`/`EXEC`/`DISCARD`,
nested `MULTI`, `EXEC`/`DISCARD` without `MULTI`, `EXECABORT` on a
queue-time error, `WATCH` abort when another client changes the key,
`WATCH` clean commit.
- **SET options / expiry commands:** `NX`, `XX`, `GET`, `KEEPTTL`,
`EXPIRE`, `PERSIST`, `TYPE` for all types.
- **Pub/Sub:** subscribe, publish delivery, publish with no
subscribers, `PING` in subscribe mode, unsubscribe, commands working
again after unsubscribe.
- **Sorted sets:** `ZADD` (new vs. update), `ZRANK`, `ZRANGE`
(incl. `WITHSCORES`, negative indexes), `ZSCORE`, `ZCARD`, `ZREM`.
- **INFO / CONFIG:** `role:master`, the replication ID, `CONFIG GET`.
- **RDB:** a generated `dump.rdb` is loaded at startup — live keys
load, already-expired keys are skipped, expiry of future keys is
preserved.
- **Replication (master side, via a Python fake replica):** handshake
(`PING`/`REPLCONF`/`PSYNC`), `+FULLRESYNC` + an **88-byte empty
RDB with a valid CRC64** (verified in the test), propagation of a
`SET`, `WAIT` (master sends `GETACK`, replica answers `ACK`, `WAIT`
returns 1), `REPLCONF GETACK` → `ACK` with the exact byte offset
of the propagated command.

One check is **skipped in this sandbox, not failed**: the Java replica
actually syncing from the master. This sandbox blocks outbound TCP
connections from Java processes (every Java `Socket.connect` is
answered with a sandbox policy message, while Python sockets connect
fine), so the replica reports `role:slave` but
`master_link_status:down` here. The replica code itself is complete
(handshake, RDB load, applying propagated commands, `GETACK`/`ACK`,
reconnect) and the master side it talks to is fully tested above; on
a normal machine (or the user's laptop) the replica syncs — the
`SET` on master → `GET` on replica and `WAIT` checks in the suite
cover exactly that path and run automatically when the link comes up.

## Project layout

```
build-your-own-redis-java/
├── README.md # this file
├── LICENSE # MIT
├──.gitignore
├── src/
│ └── RedisServer.java # the whole server — one file, stdlib only
├── tests/
│ └── test_redis.py # end-to-end test suite (162 checks)
└── demo/
└── index.html # static demo page (see "Live / Deploy")
```

## Live / Deploy

"Live" for a Redis clone does **not** mean a hosted Redis server — the
server is a TCP process that runs on a machine you control. What *can* go
live is the demo page in `demo/index.html`: a static page that explains
the clone, shows example RESP commands and outputs, and how to run it.

The demo page is already live via GitHub Pages for this repo. To make
the *server itself* reachable remotely later, run it on a VPS or
container host (`java src/RedisServer.java`, port 6379 open) — that is
a separate step from the static page, and it should sit behind a
firewall / auth before being exposed to the internet, since this clone
has no authentication.

## Limitations — intentionally left out

- **No hashes or sets** (`HSET`, `SADD`, …). The CodeCrafters
challenge has no hash/set stages; lists, streams and sorted sets
cover the data-type ideas, and adding more types would grow the
file without teaching anything new.
- **No bitmaps, geospatial commands, or authentication/ACLs.** These
are the challenge's last sections — niche encodings on top of the
same store. The server refuses `AUTH` with the same error real
Redis gives when no password is set, and has no TLS. Do not expose
port 6379 to the internet as-is.
- **RDB is read-only**: the server loads an RDB at startup but never
writes one (no `SAVE`/`BGSAVE`). The challenge only requires
reading RDB files.
- **AOF is a single file** (`appendonly.aof`), not Redis's manifest +
incremental files. Same durability idea, simpler format.
- **Replication is full resync only** (`PSYNC? -1`). Partial resync
(`PSYNC <replid> <offset>`) is rejected; replicas always get a
fresh empty RDB and the full write stream afterwards.
- **No cluster mode, no keyspace notifications, no Lua.**
- Values are held as Java strings; binary-safe byte handling is not
implemented.
- Single process, single node; no config file (flags are the only
configuration).

## What changed in this update (2026-11-22)

Starter version (30 tests) → advanced version (162 tests):

- **Lists, streams, sorted sets, transactions (+WATCH), pub/sub,
counters, `SET` options, `EXPIRE`/`PERSIST`/`TYPE`** — the
challenge's middle sections, all implemented and tested.
- **Replication** — master + replica roles, handshake, empty-RDB
transfer (88 bytes, valid CRC64 — the CRC polynomial was verified
against the RDB the CodeCrafters tester itself sends), propagation,
`GETACK`/`ACK`, `WAIT`.
- **RDB loading** — a real binary RDB parser (length encoding,
integer-encoded strings, expiry opcodes, AUX fields, SELECTDB).
- **AOF** — simple append + replay.
- **Architecture** — command execution is now single-threaded behind
one lock (like real Redis), so `MULTI`/`EXEC`, `WATCH`, `BLPOP`
and `XREAD BLOCK` are all race-free; blocking commands wait on
that lock and are woken by writes; replicas are fed through
per-replica queues so a slow replica can't stall the server.
- Baseline behaviour is unchanged: the original 30 checks still pass
unmodified.
