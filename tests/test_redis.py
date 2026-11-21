#!/usr/bin/env python3
"""End-to-end test for the Redis clone in src/RedisServer.java.

Starts the server on a scratch port, then checks over raw RESP:
  PING, ECHO, SET/GET, expiry (PX and EX), DEL, EXISTS, TTL,
  unknown command, wrong arg count, and two concurrent clients
  writing/reading different keys at the same time.

Usage:  python3 tests/test_redis.py
Exit code 0 = all checks passed.
"""
import socket
import subprocess
import sys
import threading
import time

PORT = 16379
HOST = "127.0.0.1"


def resp(*args):
    """Encode a command as a RESP array of bulk strings."""
    out = f"*{len(args)}\r\n".encode()
    for a in args:
        b = a.encode()
        out += f"${len(b)}\r\n".encode() + b + b"\r\n"
    return out


def read_reply(f):
    """Read one RESP reply from a buffered socket file and return it decoded."""
    line = f.readline()
    if not line:
        raise ConnectionError("server closed the connection")
    kind, rest = line[:1], line[1:].rstrip(b"\r\n")
    if kind == b"+":
        return rest.decode()
    if kind == b"-":
        return ("ERR", rest.decode())
    if kind == b":":
        return int(rest)
    if kind == b"$":
        n = int(rest)
        if n == -1:
            return None
        data = f.read(n + 2)[:-2]
        return data.decode()
    raise ValueError(f"unexpected reply type {kind!r}: {line!r}")


class Client:
    def __init__(self):
        self.sock = socket.create_connection((HOST, PORT), timeout=5)
        self.f = self.sock.makefile("rb")

    def __call__(self, *args):
        self.sock.sendall(resp(*args))
        return read_reply(self.f)

    def close(self):
        self.sock.close()


passed = failed = 0


def check(name, actual, expected):
    global passed, failed
    if actual == expected:
        passed += 1
        print(f"  PASS  {name}: {actual!r}")
    else:
        failed += 1
        print(f"  FAIL  {name}: got {actual!r}, expected {expected!r}")


def main():
    server = subprocess.Popen(
        ["java", "src/RedisServer.java", str(PORT)],
        cwd="/home/hatch/workspace/five-projects/01-redis",
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
    )
    try:
        # Wait for the port to accept connections.
        for _ in range(50):
            try:
                probe = socket.create_connection((HOST, PORT), timeout=1)
                probe.close()
                break
            except OSError:
                time.sleep(0.1)
        else:
            print("server did not start")
            sys.exit(1)

        c = Client()
        print("PING / ECHO")
        check("PING", c("PING"), "PONG")
        check("PING with message", c("PING", "hello"), "hello")
        check("ECHO", c("ECHO", "hello world"), "hello world")
        check("lowercase ping", c("ping"), "PONG")

        print("SET / GET")
        check("SET", c("SET", "name", "luna"), "OK")
        check("GET existing", c("GET", "name"), "luna")
        check("GET missing", c("GET", "nope"), None)
        check("SET overwrite", c("SET", "name", "boss"), "OK")
        check("GET overwritten", c("GET", "name"), "boss")

        print("DEL / EXISTS / TTL")
        check("SET del key", c("SET", "todel", "x"), "OK")
        check("EXISTS existing", c("EXISTS", "todel"), 1)
        check("EXISTS missing", c("EXISTS", "nope"), 0)
        check("TTL no expiry", c("TTL", "name"), -1)
        check("TTL missing", c("TTL", "nope"), -2)
        check("DEL existing", c("DEL", "todel"), 1)
        check("DEL again", c("DEL", "todel"), 0)
        check("GET after DEL", c("GET", "todel"), None)

        print("Expiry")
        check("SET with PX 200ms", c("SET", "temp", "v", "PX", "200"), "OK")
        check("GET before expiry", c("GET", "temp"), "v")
        time.sleep(0.35)
        check("GET after PX expiry", c("GET", "temp"), None)
        check("SET with EX 1s", c("SET", "temp2", "v", "EX", "1"), "OK")
        time.sleep(1.2)
        check("GET after EX expiry", c("GET", "temp2"), None)
        check("TTL with expiry", c("SET", "ttlkey", "v", "PX", "5000"), "OK")
        ttl = c("TTL", "ttlkey")
        check("TTL is ~5", 4 <= ttl <= 5, True)

        print("Errors")
        check("unknown command", c("NOSUCHCMD")[0], "ERR")
        check("GET no args", c("GET")[0], "ERR")
        check("SET bad option", c("SET", "k", "v", "ZZ", "1")[0], "ERR")

        print("Concurrent clients")
        results = {}

        def worker(n):
            cc = Client()
            cc("SET", f"key{n}", f"value{n}")
            results[n] = cc("GET", f"key{n}")
            cc.close()

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(2)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        check("client 0 sees own key", results.get(0), "value0")
        check("client 1 sees own key", results.get(1), "value1")
        check("client 0 key still there", c("GET", "key0"), "value0")

        c.close()
        print(f"\n{passed} passed, {failed} failed")
        sys.exit(1 if failed else 0)
    finally:
        server.terminate()
        try:
            server.wait(timeout=5)
        except subprocess.TimeoutExpired:
            server.kill()


if __name__ == "__main__":
    main()
