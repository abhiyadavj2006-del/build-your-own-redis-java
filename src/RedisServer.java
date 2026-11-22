import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/**
* Build Your Own Redis — in Java.
*
* <p>A working Redis clone in pure Java 21, standard library only. It
* listens on TCP port 6379 and speaks RESP (the Redis wire protocol), so
* it can be driven by {@code redis-cli} or any RESP client.
*
* <p>It follows the CodeCrafters "Build your own Redis" challenge
* (codecrafters.io/courses/redis), as recommended in the YouTube video
* "バックエンド・要素技術の勉強法。CodeCraftersで車輪の再開発。"
* by Naoya Tech / 直也テック
* (https://www.youtube.com/watch?v=hLQp1g1O2Ps) — the video points at the
* CodeCrafters challenge; the challenge's stage list is the tutorial.
*
* <p>Implemented stages, in the challenge's order:
* <ul>
* <li>Core — bind, PING, concurrent clients, ECHO, SET/GET, expiry</li>
* <li>Lists — RPUSH, LPUSH, LRANGE, LLEN, LPOP, RPOP, LREM, BLPOP</li>
* <li>Streams — TYPE, XADD (incl. auto-generated IDs), XRANGE, XREAD
* (incl. BLOCK and {@code $})</li>
* <li>Transactions — INCR/DECR/INCRBY/DECRBY, MULTI/EXEC/DISCARD,
* WATCH/UNWATCH (optimistic locking)</li>
* <li>Replication — {@code --port} / {@code --replicaof}, INFO,
* PING/REPLCONF/PSYNC handshake, empty-RDB transfer, propagation,
* REPLCONF GETACK/ACK, WAIT</li>
* <li>RDB persistence — {@code --dir} / {@code --dbfilename},
* CONFIG GET, loading string keys (with expiry) at startup</li>
* <li>AOF persistence (simple form) — {@code --appendonly yes} appends
* every write command to {@code appendonly.aof} and replays it</li>
* <li>Pub/Sub — SUBSCRIBE / PUBLISH / UNSUBSCRIBE</li>
* <li>Sorted sets — ZADD, ZRANK, ZRANGE, ZCARD, ZSCORE, ZREM</li>
* </ul>
*
* <p>Deliberately <b>not</b> implemented (see README): hashes/sets, bitmaps,
* geospatial commands, authentication/ACLs, cluster mode, and the full
* multi-file AOF manifest format.
*
* <p>Design notes: command execution is single-threaded behind one lock —
* exactly like real Redis — so every command is atomic. Blocking commands
* (BLPOP, XREAD BLOCK) wait on that lock and are woken by writes. Each
* client connection runs on its own Java 21 virtual thread. Replication
* writes to replicas go through per-replica queues so a slow replica can
* never stall command execution.
*
* <p>Run: {@code java src/RedisServer.java} (Java 21+, no build step) or
* {@code javac -d out src/RedisServer.java && java -cp out RedisServer}.
* Flags: {@code --port <n> --dir <path> --dbfilename <file>
* --replicaof <host> <port> --appendonly yes|no}. A bare port number as
* the first argument still works.
*/
public class RedisServer {

public static final int PORT = 6379;

/** The replication ID every master in this challenge reports. */
public static final String REPL_ID = "8371b4fb1155b71f4a04d3e1bc3e18c4a990f";

// ------------------------------------------------------------ config

private static final class Config {
int port = PORT;
String dir = ".";
String dbfilename = "dump.rdb";
boolean appendonly = false;
String replicaHost = null;
int replicaPort = 0;
}

private static Config parseArgs(String[] args) {
Config cfg = new Config();
for (int i = 0; i < args.length; i++) {
String a = args[i];
switch (a) {
case "--port" -> cfg.port = Integer.parseInt(args[++i]);
case "--dir" -> cfg.dir = args[++i];
case "--dbfilename" -> cfg.dbfilename = args[++i];
case "--appendonly" -> cfg.appendonly = args[++i].equalsIgnoreCase("yes");
case "--replicaof" -> {
cfg.replicaHost = args[++i];
cfg.replicaPort = Integer.parseInt(args[++i]);
}
default -> {
if (i == 0 &&!a.startsWith("--")) {
cfg.port = Integer.parseInt(a); // legacy: bare port
} else {
throw new IllegalArgumentException("unknown argument: " + a);
}
}
}
}
return cfg;
}

// ------------------------------------------------------------ values

/** A stored value: its payload plus the epoch-millis it expires (0 = never). */
private sealed interface Value permits StrVal, ListVal, StreamVal, ZSetVal {
long expiresAtMillis();
String typeName();

default boolean isExpired() {
return expiresAtMillis()!= 0 && System.currentTimeMillis() >= expiresAtMillis();
}
}

private record StrVal(String s, long expiresAtMillis) implements Value {
public String typeName() { return "string";}
}

private record ListVal(ArrayDeque<String> list, long expiresAtMillis) implements Value {
public String typeName() { return "list";}
}

private record StreamEntry(long ms, long seq, List<String> fields) {
String id() { return ms + "-" + seq;}
}

private record StreamVal(ArrayList<StreamEntry> entries, long expiresAtMillis) implements Value {
public String typeName() { return "stream";}
}

private record ZSetVal(HashMap<String, Double> scores, long expiresAtMillis) implements Value {
public String typeName() { return "zset";}
}

/** An error reply (the message is sent verbatim after the {@code -}). */
private static final class RedisError extends RuntimeException {
RedisError(String message) { super(message);}
}

private static final String WRONGTYPE =
"WRONGTYPE Operation against a key holding the wrong kind of value";

// ------------------------------------------------------------ state

private final Config cfg;
private final boolean isReplica;
/** One lock for all command execution — like real Redis, commands are atomic. */
private final Object lock = new Object();
private final ConcurrentHashMap<String, Value> store = new ConcurrentHashMap<>();
/** Per-key modification counter, for WATCH. Bumped on every real change. */
private final ConcurrentHashMap<String, Long> versions = new ConcurrentHashMap<>();
/** channel -> subscribers (each subscriber is a live client connection). */
private final ConcurrentHashMap<String, CopyOnWriteArraySet<ClientCtx>> channels =
new ConcurrentHashMap<>();
/** Replicas currently connected to this master. */
private final CopyOnWriteArrayList<ReplicaLink> replicas = new CopyOnWriteArrayList<>();
/**
* Replication offset: on a master, bytes of commands propagated so far;
* on a replica, bytes of commands received from its master.
*/
private final AtomicLong replOffset = new AtomicLong(0);
/** Replid received from our master (replica mode); our own id otherwise. */
private volatile String masterReplId = REPL_ID;
private volatile boolean upstreamUp = false;

// AOF
private final Object aofLock = new Object();
private BufferedOutputStream aofOut = null;

public RedisServer(Config cfg) {
this.cfg = cfg;
this.isReplica = cfg.replicaHost!= null;
}

public static void main(String[] args) throws IOException {
new RedisServer(parseArgs(args)).start();
}

// ------------------------------------------------------------ startup

private void start() throws IOException {
loadPersistence();
startSweeper();

try (ServerSocket server = new ServerSocket(cfg.port)) {
System.out.println("redis-clone listening on port " + cfg.port
+ " (RESP, Java " + Runtime.version().feature() + ")"
+ (isReplica? " [replica of " + cfg.replicaHost + ":" + cfg.replicaPort + "]": " [master]"));
System.out.println("Try: printf '*1\\r\\n$4\\r\\nPING\\r\\n' | nc localhost " + cfg.port);
if (isReplica) {
Thread upstream = new Thread(this::runReplicaUpstream, "replica-upstream");
upstream.setDaemon(true);
upstream.start();
}
ExecutorService clients = Executors.newVirtualThreadPerTaskExecutor();
while (true) {
Socket socket = server.accept();
clients.execute(() -> handleClient(socket));
}
}
}

/** Loads the dataset from disk: AOF replay if append-only is on, else the RDB file. */
private void loadPersistence() {
Path dir = Path.of(cfg.dir);
if (cfg.appendonly) {
Path aof = dir.resolve("appendonly.aof");
if (Files.exists(aof)) {
try {
byte[] bytes = Files.readAllBytes(aof);
int applied = replayCommands(bytes);
System.out.println("replayed " + applied + " commands from " + aof);
} catch (IOException e) {
System.out.println("could not read AOF file " + aof + ": " + e.getMessage());
}
}
try {
aofOut = new BufferedOutputStream(
Files.newOutputStream(dir.resolve("appendonly.aof"),
StandardOpenOption.CREATE, StandardOpenOption.APPEND));
} catch (IOException e) {
System.out.println("could not open AOF file for append: " + e.getMessage());
}
return;
}
Path rdb = dir.resolve(cfg.dbfilename);
if (Files.exists(rdb)) {
try {
Map<String, Value> loaded = Rdb.parse(Files.readAllBytes(rdb));
store.putAll(loaded);
System.out.println("loaded " + loaded.size() + " keys from " + rdb);
} catch (Exception e) {
System.out.println("could not load RDB file " + rdb + ": " + e.getMessage());
}
}
}

/** Replays RESP-encoded commands (AOF) without re-appending them. Returns count applied. */
private int replayCommands(byte[] bytes) {
int applied = 0;
try {
ByteArrayInputStream in = new ByteArrayInputStream(bytes);
while (in.available() > 0) {
List<String> cmd = readCommand(in);
if (cmd == null || cmd.isEmpty()) {
break;
}
execute(MASTER_CTX, cmd, true);
applied++;
}
} catch (IOException ignored) {
// Truncated tail (crash mid-write): stop replaying, keep what applied.
}
return applied;
}

/** Background sweeper: purge expired keys once a second. */
private void startSweeper() {
Thread sweeper = new Thread(() -> {
while (true) {
try {
Thread.sleep(1000);
synchronized (lock) {
store.entrySet().removeIf(e -> e.getValue().isExpired());
}
} catch (InterruptedException e) {
Thread.currentThread().interrupt();
return;
}
}
}, "expiry-sweeper");
sweeper.setDaemon(true);
sweeper.start();
}

// ------------------------------------------------------------ client connections

/** Per-connection state: subscriptions, MULTI queue, WATCHed keys. */
private static final class ClientCtx {
final Socket socket;
final InputStream in;
final OutputStream out;
/** Guards writes to {@link #out} (pub/sub delivers from other threads). */
final Object outLock = new Object();
final LinkedHashSet<String> subscribed = new LinkedHashSet<>();
boolean inMulti = false;
List<List<String>> queue = new ArrayList<>();
boolean multiDirty = false;
final HashMap<String, Long> watched = new HashMap<>();
/** Set once this connection completed a PSYNC handshake: it is now a replica link. */
volatile boolean becameReplicaLink = false;
/** The replica link for this connection (set by PSYNC, registered after FULLRESYNC is sent). */
volatile ReplicaLink link = null;

ClientCtx(Socket socket, InputStream in, OutputStream out) {
this.socket = socket;
this.in = in;
this.out = out;
}

void write(byte[] bytes) throws IOException {
synchronized (outLock) {
out.write(bytes);
out.flush();
}
}

boolean inSubscribeMode() {
return!subscribed.isEmpty();
}
}

/** Dummy context used when applying commands from our master (replica) or replaying AOF. */
private static final ClientCtx MASTER_CTX = new ClientCtx(null, null, null);

/** One replica connected to this master, with a queue so a slow replica can't stall us. */
private static final class ReplicaLink {
final Socket socket;
final OutputStream out;
final Object outLock;
final LinkedBlockingQueue<byte[]> outQueue = new LinkedBlockingQueue<>();
volatile long ackedOffset = -1;
volatile boolean closed = false;

ReplicaLink(Socket socket, OutputStream out, Object outLock) {
this.socket = socket;
this.out = out;
this.outLock = outLock;
Thread writer = new Thread(() -> {
try {
while (!closed) {
byte[] bytes = outQueue.take();
synchronized (outLock) {
out.write(bytes);
out.flush();
}
}
} catch (InterruptedException e) {
Thread.currentThread().interrupt();
} catch (IOException e) {
closed = true;
}
}, "replica-writer");
writer.setDaemon(true);
writer.start();
}

void send(byte[] bytes) {
if (!closed) {
outQueue.offer(bytes);
}
}

void close() {
closed = true;
try {
socket.close();
} catch (IOException ignored) {
}
}
}

private void handleClient(Socket socket) {
try (socket;
InputStream in = socket.getInputStream();
OutputStream out = socket.getOutputStream()) {
ClientCtx ctx = new ClientCtx(socket, in, out);
while (true) {
List<String> command = readCommand(in);
if (command == null) {
return; // client disconnected
}
if (command.isEmpty()) {
continue;
}
String name = command.get(0) == null? "": command.get(0).toUpperCase(Locale.ROOT);

// In subscribe mode only SUBSCRIBE / UNSUBSCRIBE / PING / QUIT / RESET are allowed.
if (ctx.inSubscribeMode()
&&!(name.equals("SUBSCRIBE") || name.equals("UNSUBSCRIBE")
|| name.equals("PING") || name.equals("QUIT") || name.equals("RESET"))) {
ctx.write(error("ERR Can't execute '" + command.get(0).toLowerCase(Locale.ROOT)
+ "': only (P)SUBSCRIBE / (P)UNSUBSCRIBE / PING / QUIT are allowed in this context"));
continue;
}

byte[] reply;
if (ctx.inSubscribeMode() && name.equals("PING")) {
// In subscribe mode PING replies with an array, not a bulk string.
reply = command.size() > 1
? array(List.of(bulk("pong"), bulk(command.get(1))))
: array(List.of(bulk("pong"), bulk("")));
ctx.write(reply);
continue;
}
if (ctx.inMulti && !name.equals("EXEC") && !name.equals("DISCARD") && !name.equals("MULTI")) {
// Queue the command; validate arity now, like real Redis.
String arityError = validateArity(command);
if (arityError != null) {
ctx.multiDirty = true;
reply = error(arityError);
} else {
ctx.queue.add(command);
reply = simple("QUEUED");
}
} else {
reply = execute(ctx, command, false);
}
if (reply != null && reply.length > 0) {
ctx.write(reply);
}
if (ctx.becameReplicaLink) {
if (ctx.link != null) {
replicas.add(ctx.link); // only now may propagation flow to this replica
}
handleReplicaLink(ctx);
return;
}
}
} catch (IOException ignored) {
// Client went away mid-conversation; nothing to do.
} catch (RuntimeException e) {
// Malformed input or an unexpected bug: drop this connection, keep serving others.
System.out.println("client error: " + e);
} finally {
// Clean up subscriptions if this client held any.
// (The ctx is out of scope here; subscriptions are removed lazily
// on publish failure instead — see publish().)
}
}

/**
* After PSYNC, this connection is a replica link: it only ever sends
* REPLCONF ACK (which updates the offset WAIT looks at) and REPLCONF
* GETACK (which we answer with our offset).
*/
private void handleReplicaLink(ClientCtx ctx) {
ReplicaLink link = ctx.link;
try {
while (true) {
List<String> command = readCommand(ctx.in);
if (command == null || command.isEmpty()) {
return;
}
String name = command.get(0) == null? "": command.get(0).toUpperCase(Locale.ROOT);
if (name.equals("REPLCONF") && command.size() >= 3) {
String sub = command.get(1).toUpperCase(Locale.ROOT);
if (sub.equals("ACK")) {
if (link!= null) {
try {
link.ackedOffset = Long.parseLong(command.get(2));
} catch (NumberFormatException ignored) {
}
}
} else if (sub.equals("GETACK")) {
ctx.write(resp("REPLCONF", "ACK", Long.toString(replOffset.get())));
} else {
ctx.write(simple("OK"));
}
} else if (name.equals("PING")) {
ctx.write(simple("PONG"));
}
// Anything else on a replica link is ignored.
}
} catch (IOException ignored) {
} finally {
if (link!= null) {
replicas.remove(link);
link.close();
}
}
}

    // ---------------------------------------------------------------- RESP

    /**
     * Reads one command from the client. Accepts RESP arrays of bulk strings
     * (what redis-cli sends) and plain inline commands (handy for telnet/nc).
     * Returns null on EOF.
     */
    private static List<String> readCommand(InputStream in) throws IOException {
        int first = in.read();
        if (first == -1) {
            return null;
        }
        if (first == '*') {
            int count = Integer.parseInt(readLine(in));
            List<String> parts = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                int type = in.read();
                if (type != '$') {
                    throw new IOException("expected bulk string, got: " + (char) type);
                }
                int len = Integer.parseInt(readLine(in));
                if (len == -1) {
                    parts.add(null);
                    continue;
                }
                byte[] buf = in.readNBytes(len);
                if (buf.length != len) {
                    return null;
                }
                readLine(in); // trailing CRLF
                parts.add(new String(buf, StandardCharsets.UTF_8));
            }
            return parts;
        }
        // Inline command: read the rest of the line and split on spaces.
        String line = (char) first + readLine(in);
        String trimmed = line.trim();
        return trimmed.isEmpty() ? List.of() : Arrays.asList(trimmed.split("\\s+"));
    }

    /** Reads one bulk string ({@code $<len>\r\n<bytes>\r\n}) — used for the RDB transfer. */
    private static byte[] readBulk(InputStream in) throws IOException {
        int type = in.read();
        if (type != '$') {
            throw new IOException("expected bulk string, got: " + (char) type);
        }
        int len = Integer.parseInt(readLine(in));
        if (len < 0) {
            return null;
        }
        byte[] buf = in.readNBytes(len);
        if (buf.length != len) {
            throw new IOException("short read on bulk string");
        }
        return buf;
    }

    private static String readLine(InputStream in) throws IOException {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        int b;
        while ((b = in.read()) != -1) {
            if (b == '\r') {
                int next = in.read();
                if (next == '\n') {
                    break;
                }
                buf.write(b);
                if (next != -1) {
                    buf.write(next);
                }
            } else {
                buf.write(b);
            }
        }
        return buf.toString(StandardCharsets.UTF_8);
    }

    /** Counts bytes read — the replica's offset is the raw byte count of commands from its master. */
    private static final class CountingInputStream extends InputStream {
        private final InputStream in;
        long count = 0;

        CountingInputStream(InputStream in) {
            this.in = in;
        }

        @Override
        public int read() throws IOException {
            int b = in.read();
            if (b != -1) {
                count++;
            }
            return b;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            int n = in.read(b, off, len);
            if (n > 0) {
                count += n;
            }
            return n;
        }
    }

    // ------------------------------------------------------- RESP encoding

    private static byte[] simple(String s) {
        return ("+" + s + "\r\n").getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] error(String s) {
        return ("-" + s + "\r\n").getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] integer(long n) {
        return (":" + n + "\r\n").getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] bulk(String s) {
        byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
        return concat(("$" + bytes.length + "\r\n").getBytes(StandardCharsets.UTF_8), bytes, new byte[]{'\r', '\n'});
    }

    private static byte[] nullBulk() {
        return "$-1\r\n".getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] nullArray() {
        return "*-1\r\n".getBytes(StandardCharsets.UTF_8);
    }

    /** Encodes a RESP array whose elements are already fully-encoded RESP values. */
    private static byte[] array(List<byte[]> elements) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            out.write(("*" + elements.size() + "\r\n").getBytes(StandardCharsets.UTF_8));
            for (byte[] e : elements) {
                out.write(e);
            }
        } catch (IOException impossible) {
            throw new AssertionError(impossible);
        }
        return out.toByteArray();
    }

    /** Encodes a command (or reply) as a RESP array of bulk strings. */
    private static byte[] resp(String... parts) {
        List<byte[]> elements = new ArrayList<>(parts.length);
        for (String p : parts) {
            elements.add(bulk(p));
        }
        return array(elements);
    }

    private static byte[] concat(byte[]... chunks) {
        int total = 0;
        for (byte[] c : chunks) {
            total += c.length;
        }
        byte[] out = new byte[total];
        int pos = 0;
        for (byte[] c : chunks) {
            System.arraycopy(c, 0, out, pos, c.length);
            pos += c.length;
        }
        return out;
    }

    // ------------------------------------------------------------ commands

    /**
     * Executes one command. Runs under the store lock, so commands are
     * atomic. Returns the RESP reply bytes (never null from here, except
     * EXEC-abort-by-WATCH which returns a null array — still bytes).
     */
    private byte[] execute(ClientCtx ctx, List<String> parts, boolean fromMaster) {
        synchronized (lock) {
            return executeLocked(ctx, parts, fromMaster);
        }
    }

    private byte[] executeLocked(ClientCtx ctx, List<String> parts, boolean fromMaster) {
        if (parts.get(0) == null) {
            return error("ERR unknown command ''");
        }
        String cmd = parts.get(0).toUpperCase(Locale.ROOT);
        // A replica serves reads, but writes from its own clients are refused.
        if (isReplica && !fromMaster && isWriteCommand(cmd)) {
            return error("READONLY You can't write against a read only replica.");
        }
        String arityError = validateArity(parts);
        if (arityError != null) {
            return error(arityError);
        }
        try {
            return switch (cmd) {
                case "PING" -> parts.size() > 1 ? bulk(parts.get(1)) : simple("PONG");
                case "ECHO" -> bulk(parts.get(1));
                case "SET" -> doSet(parts, fromMaster);
                case "GET" -> get(parts.get(1));
                case "DEL" -> doDel(parts, fromMaster);
                case "EXISTS" -> {
                    int found = 0;
                    for (int i = 1; i < parts.size(); i++) {
                        if (getLive(parts.get(i)) != null) {
                            found++;
                        }
                    }
                    yield integer(found);
                }
                case "TTL" -> integer(ttl(parts.get(1), false));
                case "PTTL" -> integer(ttl(parts.get(1), true));
                case "EXPIRE" -> integer(setExpiry(parts, fromMaster));
                case "PEXPIRE" -> integer(setExpiry(parts, fromMaster));
                case "PERSIST" -> integer(setExpiry(parts, fromMaster));
                case "TYPE" -> {
                    Value v = getLive(parts.get(1));
                    yield simple(v == null ? "none" : v.typeName());
                }
                case "INCR" -> incrBy(parts, 1, fromMaster);
                case "DECR" -> incrBy(parts, -1, fromMaster);
                case "INCRBY" -> incrBy(parts, parseLong(parts.get(2)), fromMaster);
                case "DECRBY" -> incrBy(parts, -parseLong(parts.get(2)), fromMaster);
                case "RPUSH" -> push(parts, false, fromMaster);
                case "LPUSH" -> push(parts, true, fromMaster);
                case "LRANGE" -> lrange(parts);
                case "LLEN" -> {
                    Value v = getLive(parts.get(1));
                    if (v == null) {
                        yield integer(0);
                    }
                    if (!(v instanceof ListVal lv)) {
                        throw new RedisError(WRONGTYPE);
                    }
                    yield integer(lv.list().size());
                }
                case "LPOP" -> pop(parts, true, fromMaster);
                case "RPOP" -> pop(parts, false, fromMaster);
                case "LREM" -> lrem(parts, fromMaster);
                case "BLPOP" -> blpop(parts, fromMaster);
                case "XADD" -> xadd(parts, fromMaster);
                case "XRANGE" -> xrange(parts, false);
                case "XREVRANGE" -> xrange(parts, true);
                case "XREAD" -> xread(parts, fromMaster);
                case "MULTI" -> doMulti(ctx);
                case "EXEC" -> doExec(ctx);
                case "DISCARD" -> doDiscard(ctx);
                case "WATCH" -> doWatch(ctx, parts);
                case "UNWATCH" -> {
                    ctx.watched.clear();
                    yield simple("OK");
                }
                case "INFO" -> bulk(info(parts.size() > 1 ? parts.get(1) : null));
                case "CONFIG" -> config(parts);
                case "PUBLISH" -> publish(parts);
                case "SUBSCRIBE" -> subscribe(ctx, parts);
                case "UNSUBSCRIBE" -> unsubscribe(ctx, parts);
                case "ZADD" -> zadd(parts, fromMaster);
                case "ZRANK" -> zrank(parts);
                case "ZRANGE" -> zrange(parts);
                case "ZCARD" -> {
                    Value v = getLive(parts.get(1));
                    if (v == null) {
                        yield integer(0);
                    }
                    if (!(v instanceof ZSetVal zv)) {
                        throw new RedisError(WRONGTYPE);
                    }
                    yield integer(zv.scores().size());
                }
                case "ZSCORE" -> {
                    Value v = getLive(parts.get(1));
                    if (v == null) {
                        yield nullBulk();
                    }
                    if (!(v instanceof ZSetVal zv)) {
                        throw new RedisError(WRONGTYPE);
                    }
                    Double score = zv.scores().get(parts.get(2));
                    yield score == null ? nullBulk() : bulk(formatScore(score));
                }
                case "ZREM" -> zrem(parts, fromMaster);
                case "REPLCONF" -> replconf(ctx, parts);
                case "PSYNC" -> psync(ctx, parts);
                case "WAIT" -> waitForReplicas(parts);
                case "COMMAND" -> simple("OK"); // lets redis-cli handshake succeed
                case "CLIENT" -> client(parts);
                case "HELLO" -> hello(parts);
                case "SELECT" -> parts.get(1).equals("0")
                        ? simple("OK") : error("ERR DB index is out of range");
                case "AUTH" -> error("ERR Client sent AUTH, but no password is set");
                case "RESET" -> {
                    unsubscribeAll(ctx);
                    ctx.inMulti = false;
                    ctx.queue.clear();
                    ctx.multiDirty = false;
                    ctx.watched.clear();
                    yield simple("RESET");
                }
                case "QUIT" -> simple("OK");
                default -> error("ERR unknown command '" + parts.get(0) + "'");
            };
        } catch (RedisError e) {
            return error(e.getMessage());
        } catch (NumberFormatException e) {
            return error("ERR value is not an integer or out of range");
        }
    }

    /** Commands that change the dataset (and are therefore propagated / AOF-logged). */
    private static boolean isWriteCommand(String cmd) {
        return switch (cmd) {
            case "SET", "DEL", "EXPIRE", "PEXPIRE", "PERSIST",
                    "INCR", "DECR", "INCRBY", "DECRBY",
                    "RPUSH", "LPUSH", "LPOP", "RPOP", "LREM", "BLPOP",
                    "XADD", "ZADD", "ZREM" -> true;
            default -> false;
        };
    }

    /** Wrong-arity error, or null if the argument count is fine (unknown commands return null). */
    private static String validateArity(List<String> parts) {
        String cmd = parts.get(0) == null ? "" : parts.get(0).toUpperCase(Locale.ROOT);
        int n = parts.size();
        int min = -1, max = -1;
        switch (cmd) {
            case "PING" -> { min = 1; max = 2; }
            case "ECHO", "GET", "TTL", "PTTL", "PERSIST", "TYPE", "INCR", "DECR",
                    "LLEN", "ZCARD", "SELECT" -> { min = 2; max = 2; }
            case "SET" -> { min = 3; max = Integer.MAX_VALUE; }
            case "DEL", "EXISTS", "WATCH" -> { min = 2; max = Integer.MAX_VALUE; }
            case "EXPIRE", "PEXPIRE", "INCRBY", "DECRBY", "ZRANK", "ZSCORE", "REPLCONF", "PSYNC", "WAIT",
                    "PUBLISH", "HELLO" -> { min = 3; max = 3; }
            case "RPUSH", "LPUSH", "ZADD", "ZREM" -> { min = 3; max = Integer.MAX_VALUE; }
            case "LRANGE", "LREM" -> { min = 4; max = 4; }
            case "LPOP", "RPOP" -> { min = 2; max = 3; }
            case "BLPOP" -> { min = 3; max = Integer.MAX_VALUE; }
            case "XADD" -> { min = 5; max = Integer.MAX_VALUE; }
            case "XRANGE", "XREVRANGE" -> { min = 4; max = 6; }
            case "XREAD" -> { min = 4; max = Integer.MAX_VALUE; }
            case "MULTI", "EXEC", "DISCARD", "UNWATCH", "RESET", "QUIT" -> { min = 1; max = 1; }
            case "INFO" -> { min = 1; max = 2; }
            case "CONFIG" -> { min = 3; max = Integer.MAX_VALUE; }
            case "SUBSCRIBE" -> { min = 2; max = Integer.MAX_VALUE; }
            case "UNSUBSCRIBE" -> { min = 1; max = Integer.MAX_VALUE; }
            case "ZRANGE" -> { min = 4; max = 5; }
            case "COMMAND" -> { min = 1; max = Integer.MAX_VALUE; }
            case "CLIENT" -> { min = 2; max = Integer.MAX_VALUE; }
            case "AUTH" -> { min = 2; max = 3; }
            default -> { return null; } // unknown command: reported as such, not as arity
        }
        if (n < min || n > max) {
            return "ERR wrong number of arguments for '" + cmd.toLowerCase(Locale.ROOT) + "' command";
        }
        return null;
    }

    private static long parseLong(String s) {
        try {
            return Long.parseLong(s);
        } catch (NumberFormatException e) {
            throw new RedisError("ERR value is not an integer or out of range");
        }
    }

    // ------------------------------------------------------- key helpers

    /** Returns the live value for a key, deleting it first if it expired. */
    private Value getLive(String key) {
        Value v = store.get(key);
        if (v != null && v.isExpired()) {
            store.remove(key, v);
            return null;
        }
        return v;
    }

    /** Returns the live value, or throws WRONGTYPE unless it is a string (or missing). */
    private StrVal getString(String key) {
        Value v = getLive(key);
        if (v == null) {
            return null;
        }
        if (!(v instanceof StrVal sv)) {
            throw new RedisError(WRONGTYPE);
        }
        return sv;
    }

    /** TTL in seconds (or ms when {@code millis}); -1 = no expiry, -2 = missing. */
    private long ttl(String key, boolean millis) {
        Value v = getLive(key);
        if (v == null) {
            return -2;
        }
        if (v.expiresAtMillis() == 0) {
            return -1;
        }
        long remaining = v.expiresAtMillis() - System.currentTimeMillis();
        if (millis) {
            return Math.max(0, remaining);
        }
        return Math.max(0, (remaining + 999) / 1000);
    }

    /** Sets (or clears, when {@code expiresAtMillis == 0}) the expiry of a key. Returns 1/0. */
    private int setExpiry(List<String> parts, boolean fromMaster) {
        String key = parts.get(1);
        String cmd = parts.get(0).toUpperCase(Locale.ROOT);
        long expiresAtMillis;
        if (cmd.equals("PERSIST")) {
            expiresAtMillis = 0;
        } else {
            long amount = parseLong(parts.get(2));
            expiresAtMillis = cmd.equals("EXPIRE")
                    ? System.currentTimeMillis() + amount * 1000
                    : System.currentTimeMillis() + amount;
        }
        Value v = getLive(key);
        if (v == null) {
            return 0;
        }
        if (expiresAtMillis == 0 && v.expiresAtMillis() == 0) {
            return 0; // PERSIST on a key with no expiry
        }
        Value replacement = switch (v) {
            case StrVal sv -> new StrVal(sv.s(), expiresAtMillis);
            case ListVal lv -> new ListVal(lv.list(), expiresAtMillis);
            case StreamVal sv -> new StreamVal(sv.entries(), expiresAtMillis);
            case ZSetVal zv -> new ZSetVal(zv.scores(), expiresAtMillis);
        };
        store.put(key, replacement);
        recordWrite(parts, fromMaster, key);
        return 1;
    }

    /**
     * Records a real mutation: bumps WATCH versions, wakes blocking clients,
     * propagates the command to replicas and appends it to the AOF.
     * Skipped entirely for commands applied from our master.
     */
    private void recordWrite(List<String> parts, boolean fromMaster, String... keys) {
        for (String k : keys) {
            versions.merge(k, 1L, Long::sum);
        }
        synchronized (lock) {
            lock.notifyAll();
        }
        if (fromMaster) {
            return;
        }
        byte[] encoded = resp(parts.toArray(new String[0]));
        replOffset.addAndGet(encoded.length);
        for (ReplicaLink link : replicas) {
            link.send(encoded);
        }
        if (aofOut != null) {
            synchronized (aofLock) {
                try {
                    aofOut.write(encoded);
                    aofOut.flush();
                } catch (IOException ignored) {
                }
            }
        }
    }

    // ------------------------------------------------------- string commands

    private byte[] doSet(List<String> parts, boolean fromMaster) {
        String key = parts.get(1);
        String value = parts.get(2);
        long expiresAt = 0;
        boolean nx = false, xx = false, get = false, keepTtl = false;
        for (int i = 3; i < parts.size(); i++) {
            String opt = parts.get(i).toUpperCase(Locale.ROOT);
            switch (opt) {
                case "NX" -> nx = true;
                case "XX" -> xx = true;
                case "GET" -> get = true;
                case "KEEPTTL" -> keepTtl = true;
                case "EX", "PX" -> {
                    if (i + 1 >= parts.size()) {
                        throw new RedisError("ERR syntax error");
                    }
                    long amount = parseLong(parts.get(++i));
                    expiresAt = System.currentTimeMillis() + (opt.equals("EX") ? amount * 1000 : amount);
                }
                default -> throw new RedisError("ERR syntax error");
            }
        }
        StrVal old = getString(key);
        if (nx && old != null) {
            return nullBulk();
        }
        if (xx && old == null) {
            return nullBulk();
        }
        if (keepTtl && old != null) {
            expiresAt = old.expiresAtMillis();
        }
        store.put(key, new StrVal(value, expiresAt));
        recordWrite(parts, fromMaster, key);
        return get ? (old == null ? nullBulk() : bulk(old.s())) : simple("OK");
    }

    private byte[] get(String key) {
        StrVal v = getString(key);
        return v == null ? nullBulk() : bulk(v.s());
    }

    private byte[] doDel(List<String> parts, boolean fromMaster) {
        int deleted = 0;
        for (int i = 1; i < parts.size(); i++) {
            String key = parts.get(i);
            if (store.remove(key) != null) {
                deleted++;
            }
        }
        if (deleted > 0) {
            recordWrite(parts, fromMaster, parts.subList(1, parts.size()).toArray(new String[0]));
        }
        return integer(deleted);
    }

    private byte[] incrBy(List<String> parts, long delta, boolean fromMaster) {
        String key = parts.get(1);
        StrVal current = getString(key);
        long n = 0;
        if (current != null) {
            try {
                n = Long.parseLong(current.s());
            } catch (NumberFormatException e) {
                throw new RedisError("ERR value is not an integer or out of range");
            }
        }
        long result;
        try {
            result = Math.addExact(n, delta);
        } catch (ArithmeticException e) {
            throw new RedisError("ERR increment or decrement would overflow");
        }
        long expiresAt = current == null ? 0 : current.expiresAtMillis();
        store.put(key, new StrVal(Long.toString(result), expiresAt));
        recordWrite(parts, fromMaster, key);
        return integer(result);
    }

    // ------------------------------------------------------- list commands

    private ListVal getList(String key) {
        Value v = getLive(key);
        if (v == null) {
            return null;
        }
        if (!(v instanceof ListVal lv)) {
            throw new RedisError(WRONGTYPE);
        }
        return lv;
    }

    private ListVal getOrCreateList(String key) {
        ListVal lv = getList(key);
        if (lv == null) {
            lv = new ListVal(new ArrayDeque<>(), 0);
            store.put(key, lv);
        }
        return lv;
    }

    private byte[] push(List<String> parts, boolean head, boolean fromMaster) {
        String key = parts.get(1);
        ListVal lv = getOrCreateList(key);
        for (int i = 2; i < parts.size(); i++) {
            if (head) {
                lv.list().addFirst(parts.get(i));
            } else {
                lv.list().addLast(parts.get(i));
            }
        }
        recordWrite(parts, fromMaster, key);
        return integer(lv.list().size());
    }

    private byte[] lrange(List<String> parts) {
        ListVal lv = getList(parts.get(1));
        if (lv == null) {
            return array(List.of());
        }
        int size = lv.list().size();
        int[] bounds = rangeBounds(size, parseLong(parts.get(2)), parseLong(parts.get(3)));
        List<byte[]> out = new ArrayList<>();
        if (bounds != null) {
            List<String> all = new ArrayList<>(lv.list());
            for (int i = bounds[0]; i <= bounds[1]; i++) {
                out.add(bulk(all.get(i)));
            }
        }
        return array(out);
    }

    /**
     * Inclusive [from, to] for a Redis range (negative indexes count from
     * the end), or null when the range selects nothing — e.g. start past
     * the end, or start after stop.
     */
    private static int[] rangeBounds(int size, long startArg, long stopArg) {
        long start = startArg < 0 ? size + startArg : startArg;
        long stop = stopArg < 0 ? size + stopArg : stopArg;
        if (start < 0) {
            start = 0;
        }
        if (stop >= size) {
            stop = size - 1;
        }
        if (start >= size || stop < 0 || start > stop) {
            return null;
        }
        return new int[]{(int) start, (int) stop};
    }

    private byte[] pop(List<String> parts, boolean head, boolean fromMaster) {
        String key = parts.get(1);
        ListVal lv = getList(key);
        if (lv == null || lv.list().isEmpty()) {
            return parts.size() > 2 ? nullArray() : nullBulk();
        }
        if (parts.size() == 2) {
            String element = head ? lv.list().removeFirst() : lv.list().removeLast();
            afterListChange(key, lv);
            recordWrite(parts, fromMaster, key);
            return bulk(element);
        }
        long count = parseLong(parts.get(2));
        if (count < 0) {
            throw new RedisError("ERR value is out of range, must be positive");
        }
        List<byte[]> out = new ArrayList<>();
        while (count-- > 0 && !lv.list().isEmpty()) {
            out.add(bulk(head ? lv.list().removeFirst() : lv.list().removeLast()));
        }
        afterListChange(key, lv);
        recordWrite(parts, fromMaster, key);
        return array(out);
    }

    private byte[] lrem(List<String> parts, boolean fromMaster) {
        String key = parts.get(1);
        ListVal lv = getList(key);
        if (lv == null) {
            return integer(0);
        }
        long count = parseLong(parts.get(2));
        String element = parts.get(3);
        int removed = 0;
        if (count == 0) {
            Iterator<String> it = lv.list().iterator();
            while (it.hasNext()) {
                if (it.next().equals(element)) {
                    it.remove();
                    removed++;
                }
            }
        } else if (count > 0) {
            Iterator<String> it = lv.list().iterator();
            while (it.hasNext() && removed < count) {
                if (it.next().equals(element)) {
                    it.remove();
                    removed++;
                }
            }
        } else {
            Iterator<String> it = lv.list().descendingIterator();
            while (it.hasNext() && removed < -count) {
                if (it.next().equals(element)) {
                    it.remove();
                    removed++;
                }
            }
        }
        if (removed > 0) {
            afterListChange(key, lv);
            recordWrite(parts, fromMaster, key);
        }
        return integer(removed);
    }

    /** Removes the key entirely once its list is empty (real Redis does this). */
    private void afterListChange(String key, ListVal lv) {
        if (lv.list().isEmpty()) {
            store.remove(key, lv);
        }
    }

    /**
     * BLPOP — blocks (releasing the store lock) until an element appears on
     * one of the keys, or the timeout (seconds, 0 = forever) expires.
     */
    private byte[] blpop(List<String> parts, boolean fromMaster) {
        double timeoutSeconds = Double.parseDouble(parts.get(parts.size() - 1));
        if (timeoutSeconds < 0) {
            throw new RedisError("ERR timeout is negative");
        }
        long deadline = timeoutSeconds == 0 ? Long.MAX_VALUE
                : System.currentTimeMillis() + (long) (timeoutSeconds * 1000);
        List<String> keys = parts.subList(1, parts.size() - 1);
        try {
            while (true) {
                for (String key : keys) {
                    ListVal lv = getList(key);
                    if (lv != null && !lv.list().isEmpty()) {
                        String element = lv.list().removeFirst();
                        afterListChange(key, lv);
                        // Propagate as the equivalent LPOP so replicas stay in sync.
                        recordWrite(List.of("LPOP", key), fromMaster, key);
                        return array(List.of(bulk(key), bulk(element)));
                    }
                }
                long remaining = deadline - System.currentTimeMillis();
                if (remaining <= 0) {
                    return nullArray();
                }
                lock.wait(Math.min(remaining, 1000));
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return nullArray();
        }
    }

    // ------------------------------------------------------- stream commands

    private StreamVal getStream(String key) {
        Value v = getLive(key);
        if (v == null) {
            return null;
        }
        if (!(v instanceof StreamVal sv)) {
            throw new RedisError(WRONGTYPE);
        }
        return sv;
    }

    private StreamVal getOrCreateStream(String key) {
        StreamVal sv = getStream(key);
        if (sv == null) {
            sv = new StreamVal(new ArrayList<>(), 0);
            store.put(key, sv);
        }
        return sv;
    }

    private byte[] xadd(List<String> parts, boolean fromMaster) {
        String key = parts.get(1);
        String idArg = parts.get(2);
        if ((parts.size() - 3) % 2 != 0) {
            throw new RedisError("ERR wrong number of arguments for 'xadd' command");
        }
        StreamVal sv = getOrCreateStream(key);
        StreamEntry last = sv.entries().isEmpty() ? null : sv.entries().get(sv.entries().size() - 1);
        long ms, seq;
        if (idArg.equals("*")) {
            ms = System.currentTimeMillis();
            if (last == null) {
                seq = ms == 0 ? 1 : 0;
            } else if (ms > last.ms()) {
                seq = 0;
            } else {
                ms = last.ms();
                seq = last.seq() + 1;
            }
        } else if (idArg.endsWith("-*")) {
            ms = parseLong(idArg.substring(0, idArg.length() - 2));
            if (last == null) {
                seq = ms == 0 ? 1 : 0;
            } else if (ms > last.ms()) {
                seq = 0;
            } else if (ms == last.ms()) {
                seq = last.seq() + 1;
            } else {
                throw new RedisError("ERR The ID specified in XADD is equal or smaller than the target stream top item");
            }
        } else {
            long[] parsed = parseStreamId(idArg);
            ms = parsed[0];
            seq = parsed[1];
            if (ms == 0 && seq == 0) {
                throw new RedisError("ERR The ID specified in XADD must be greater than 0-0");
            }
            if (last != null && (ms < last.ms() || (ms == last.ms() && seq <= last.seq()))) {
                throw new RedisError("ERR The ID specified in XADD is equal or smaller than the target stream top item");
            }
        }
        List<String> fields = new ArrayList<>(parts.subList(3, parts.size()));
        sv.entries().add(new StreamEntry(ms, seq, fields));
        List<String> propagated = new ArrayList<>();
        propagated.add("XADD");
        propagated.add(key);
        propagated.add(ms + "-" + seq);
        propagated.addAll(fields);
        recordWrite(propagated, fromMaster, key);
        return bulk(ms + "-" + seq);
    }

    /** Parses "ms-seq" (or plain "ms", meaning "ms-0"). */
    private static long[] parseStreamId(String id) {
        int dash = id.indexOf('-');
        try {
            if (dash == -1) {
                return new long[]{Long.parseLong(id), 0};
            }
            return new long[]{Long.parseLong(id.substring(0, dash)), Long.parseLong(id.substring(dash + 1))};
        } catch (NumberFormatException e) {
            throw new RedisError("ERR Invalid stream ID specified as stream command argument");
        }
    }

    private byte[] xrange(List<String> parts, boolean reverse) {
        StreamVal sv = getStream(parts.get(1));
        if (sv == null) {
            return array(List.of());
        }
        long[] start = parseRangeBound(parts.get(2), true);
        long[] end = parseRangeBound(parts.get(3), false);
        int count = Integer.MAX_VALUE;
        if (parts.size() == 6) {
            if (!parts.get(4).equalsIgnoreCase("COUNT")) {
                throw new RedisError("ERR syntax error");
            }
            count = (int) parseLong(parts.get(5));
        }
        List<StreamEntry> entries = new ArrayList<>(sv.entries());
        if (reverse) {
            Collections.reverse(entries);
        }
        List<byte[]> out = new ArrayList<>();
        for (StreamEntry e : entries) {
            if (compareId(e.ms(), e.seq(), start[0], start[1]) < 0) {
                continue;
            }
            if (compareId(e.ms(), e.seq(), end[0], end[1]) > 0) {
                continue;
            }
            out.add(encodeStreamEntry(e));
            if (out.size() >= count) {
                break;
            }
        }
        return array(out);
    }

    /** Parses an XRANGE bound: "-" = minimum, "+" = maximum, "ms" / "ms-seq" otherwise. */
    private static long[] parseRangeBound(String s, boolean isStart) {
        if (s.equals("-")) {
            return new long[]{0, 0};
        }
        if (s.equals("+")) {
            return new long[]{Long.MAX_VALUE, Long.MAX_VALUE};
        }
        long[] id = parseStreamId(s);
        if (s.indexOf('-') == -1) {
            // Bare milliseconds: start means ms-0, end means ms-<max>.
            id[1] = isStart ? 0 : Long.MAX_VALUE;
        }
        return id;
    }

    private static int compareId(long ms1, long seq1, long ms2, long seq2) {
        int c = Long.compare(ms1, ms2);
        return c != 0 ? c : Long.compare(seq1, seq2);
    }

    private static byte[] encodeStreamEntry(StreamEntry e) {
        List<byte[]> fields = new ArrayList<>();
        for (String f : e.fields()) {
            fields.add(bulk(f));
        }
        return array(List.of(bulk(e.id()), array(fields)));
    }

    private byte[] xread(List<String> parts, boolean fromMaster) {
        int i = 1;
        int count = Integer.MAX_VALUE;
        long blockMillis = -1; // -1 = don't block
        while (i < parts.size() && !parts.get(i).equalsIgnoreCase("STREAMS")) {
            String opt = parts.get(i).toUpperCase(Locale.ROOT);
            if (opt.equals("COUNT")) {
                if (++i >= parts.size()) {
                    throw new RedisError("ERR syntax error");
                }
                count = (int) parseLong(parts.get(i));
            } else if (opt.equals("BLOCK")) {
                if (++i >= parts.size()) {
                    throw new RedisError("ERR syntax error");
                }
                blockMillis = parseLong(parts.get(i));
                if (blockMillis < 0) {
                    throw new RedisError("ERR timeout is negative");
                }
            } else {
                throw new RedisError("ERR syntax error");
            }
            i++;
        }
        if (i >= parts.size()) {
            throw new RedisError("ERR syntax error");
        }
        i++; // skip STREAMS
        int remaining = parts.size() - i;
        if (remaining == 0 || remaining % 2 != 0) {
            throw new RedisError("ERR syntax error");
        }
        int nStreams = remaining / 2;
        List<String> keys = parts.subList(i, i + nStreams);
        List<String> ids = parts.subList(i + nStreams, parts.size());
        long deadline = blockMillis < 0 ? 0 : System.currentTimeMillis() + blockMillis;
// Resolve "$" once, up front: it means "only entries added after this call".
// (Resolving it again after every wait would silently skip entries.)
List<String> resolvedIds = new ArrayList<>(ids);
for (int s = 0; s < keys.size(); s++) {
if (resolvedIds.get(s).equals("$")) {
StreamVal sv = getStream(keys.get(s));
resolvedIds.set(s, sv == null || sv.entries().isEmpty()
? "0-0"
: sv.entries().get(sv.entries().size() - 1).id());
}
}
        try {
            while (true) {
                byte[] result = xreadOnce(keys, resolvedIds, count);
                if (result != null) {
                    return result;
                }
                if (blockMillis < 0) {
                    return nullArray();
                }
                long waitFor = deadline - System.currentTimeMillis();
                if (waitFor <= 0) {
                    return nullArray();
                }
                lock.wait(Math.min(waitFor, 1000));
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return nullArray();
        }
    }

    /**
     * One XREAD pass. Returns null when no stream has new entries.
     * A "$" id means "only entries added after this call" — it is resolved
     * against the stream's current top entry on the first pass.
     */
    private byte[] xreadOnce(List<String> keys, List<String> ids, int count) {
        List<byte[]> out = new ArrayList<>();
        for (int s = 0; s < keys.size(); s++) {
            String key = keys.get(s);
            String idArg = ids.get(s);
            StreamVal sv = getStream(key);
            if (sv == null || sv.entries().isEmpty()) {
                continue;
            }
            long[] after;
            if (idArg.equals("$")) {
                StreamEntry top = sv.entries().get(sv.entries().size() - 1);
                after = new long[]{top.ms(), top.seq()};
            } else {
                after = parseStreamId(idArg);
            }
            List<byte[]> entries = new ArrayList<>();
            for (StreamEntry e : sv.entries()) {
                if (compareId(e.ms(), e.seq(), after[0], after[1]) > 0) {
                    entries.add(encodeStreamEntry(e));
                    if (entries.size() >= count) {
                        break;
                    }
                }
            }
            if (!entries.isEmpty()) {
                out.add(array(List.of(bulk(key), array(entries))));
            }
        }
        return out.isEmpty() ? null : array(out);
    }

    // ------------------------------------------------------- sorted set commands

    private ZSetVal getZSet(String key) {
        Value v = getLive(key);
        if (v == null) {
            return null;
        }
        if (!(v instanceof ZSetVal zv)) {
            throw new RedisError(WRONGTYPE);
        }
        return zv;
    }

    private ZSetVal getOrCreateZSet(String key) {
        ZSetVal zv = getZSet(key);
        if (zv == null) {
            zv = new ZSetVal(new HashMap<>(), 0);
            store.put(key, zv);
        }
        return zv;
    }

    /** Members ordered by (score, member) — the sorted-set order. */
    private static List<Map.Entry<String, Double>> sortedMembers(ZSetVal zv) {
        List<Map.Entry<String, Double>> members = new ArrayList<>(zv.scores().entrySet());
        members.sort((a, b) -> {
            int c = Double.compare(a.getValue(), b.getValue());
            return c != 0 ? c : a.getKey().compareTo(b.getKey());
        });
        return members;
    }

    private byte[] zadd(List<String> parts, boolean fromMaster) {
        if ((parts.size() - 2) % 2 != 0) {
            throw new RedisError("ERR syntax error");
        }
        ZSetVal zv = getOrCreateZSet(parts.get(1));
        int added = 0;
        for (int i = 2; i < parts.size(); i += 2) {
            double score;
            try {
                score = Double.parseDouble(parts.get(i));
            } catch (NumberFormatException e) {
                throw new RedisError("ERR value is not a valid float");
            }
            if (zv.scores().put(parts.get(i + 1), score) == null) {
                added++;
            }
        }
        if (added > 0 || true) { // a score update is also a change; re-apply is idempotent
            recordWrite(parts, fromMaster, parts.get(1));
        }
        return integer(added);
    }

    private byte[] zrank(List<String> parts) {
        ZSetVal zv = getZSet(parts.get(1));
        if (zv == null) {
            return nullBulk();
        }
        List<Map.Entry<String, Double>> members = sortedMembers(zv);
        for (int i = 0; i < members.size(); i++) {
            if (members.get(i).getKey().equals(parts.get(2))) {
                return integer(i);
            }
        }
        return nullBulk();
    }

    private byte[] zrange(List<String> parts) {
        ZSetVal zv = getZSet(parts.get(1));
        if (zv == null) {
            return array(List.of());
        }
        List<Map.Entry<String, Double>> members = sortedMembers(zv);
        int size = members.size();
        int[] bounds = rangeBounds(size, parseLong(parts.get(2)), parseLong(parts.get(3)));
        boolean withScores = parts.size() == 5 && parts.get(4).equalsIgnoreCase("WITHSCORES");
        if (parts.size() == 5 && !withScores) {
            throw new RedisError("ERR syntax error");
        }
        List<byte[]> out = new ArrayList<>();
        if (bounds != null) {
            for (int i = bounds[0]; i <= bounds[1]; i++) {
                Map.Entry<String, Double> e = members.get(i);
                out.add(bulk(e.getKey()));
                if (withScores) {
                    out.add(bulk(formatScore(e.getValue())));
                }
            }
        }
        return array(out);
    }

    private byte[] zrem(List<String> parts, boolean fromMaster) {
        ZSetVal zv = getZSet(parts.get(1));
        if (zv == null) {
            return integer(0);
        }
        int removed = 0;
        for (int i = 2; i < parts.size(); i++) {
            if (zv.scores().remove(parts.get(i)) != null) {
                removed++;
            }
        }
        if (removed > 0) {
            if (zv.scores().isEmpty()) {
                store.remove(parts.get(1), zv);
            }
            recordWrite(parts, fromMaster, parts.get(1));
        }
        return integer(removed);
    }

    /** Formats a score the way Redis does: integral scores have no decimal point. */
    private static String formatScore(double score) {
        if (score == Math.rint(score) && Math.abs(score) < 1e15) {
            return Long.toString((long) score);
        }
        return Double.toString(score);
    }

    // ------------------------------------------------------- transactions

    private byte[] doMulti(ClientCtx ctx) {
        if (ctx.inMulti) {
            throw new RedisError("ERR MULTI calls can not be nested");
        }
        ctx.inMulti = true;
        ctx.queue = new ArrayList<>();
        ctx.multiDirty = false;
        return simple("OK");
    }

    private byte[] doExec(ClientCtx ctx) {
        if (!ctx.inMulti) {
            throw new RedisError("ERR EXEC without MULTI");
        }
        ctx.inMulti = false;
        if (ctx.multiDirty) {
            ctx.queue.clear();
            ctx.watched.clear();
            throw new RedisError("EXECABORT Transaction discarded because of previous errors.");
        }
        // Optimistic locking: abort (nil) if any watched key changed.
        for (Map.Entry<String, Long> w : ctx.watched.entrySet()) {
            long current = versions.getOrDefault(w.getKey(), 0L);
            if (current != w.getValue()) {
                ctx.queue.clear();
                ctx.watched.clear();
                return nullArray();
            }
        }
        ctx.watched.clear();
        List<List<String>> queued = ctx.queue;
        ctx.queue = new ArrayList<>();
        List<byte[]> replies = new ArrayList<>(queued.size());
        for (List<String> cmd : queued) {
            replies.add(execute(ctx, cmd, false));
        }
        return array(replies);
    }

    private byte[] doDiscard(ClientCtx ctx) {
        if (!ctx.inMulti) {
            throw new RedisError("ERR DISCARD without MULTI");
        }
        ctx.inMulti = false;
        ctx.queue.clear();
        ctx.multiDirty = false;
        ctx.watched.clear();
        return simple("OK");
    }

    private byte[] doWatch(ClientCtx ctx, List<String> parts) {
        if (ctx.inMulti) {
            throw new RedisError("ERR WATCH inside MULTI is not allowed");
        }
        for (int i = 1; i < parts.size(); i++) {
            ctx.watched.put(parts.get(i), versions.getOrDefault(parts.get(i), 0L));
        }
        return simple("OK");
    }

    // ------------------------------------------------------- pub / sub

    private byte[] publish(List<String> parts) {
        String channel = parts.get(1);
        String message = parts.get(2);
        CopyOnWriteArraySet<ClientCtx> subs = channels.get(channel);
        if (subs == null || subs.isEmpty()) {
            return integer(0);
        }
        byte[] delivery = array(List.of(bulk("message"), bulk(channel), bulk(message)));
        int delivered = 0;
        for (ClientCtx sub : subs) {
            try {
                sub.write(delivery);
                delivered++;
            } catch (IOException e) {
                subs.remove(sub); // subscriber went away; drop it
            }
        }
        return integer(delivered);
    }

    private byte[] subscribe(ClientCtx ctx, List<String> parts) {
        List<byte[]> replies = new ArrayList<>();
        for (int i = 1; i < parts.size(); i++) {
            String channel = parts.get(i);
            channels.computeIfAbsent(channel, k -> new CopyOnWriteArraySet<>()).add(ctx);
            ctx.subscribed.add(channel);
            replies.add(array(List.of(bulk("subscribe"), bulk(channel), integer(ctx.subscribed.size()))));
        }
        return concat(replies.toArray(new byte[0][]));
    }

    private byte[] unsubscribe(ClientCtx ctx, List<String> parts) {
        List<String> targets = parts.size() == 1
                ? new ArrayList<>(ctx.subscribed)
                : parts.subList(1, parts.size());
        List<byte[]> replies = new ArrayList<>();
        for (String channel : targets) {
            CopyOnWriteArraySet<ClientCtx> subs = channels.get(channel);
            if (subs != null) {
                subs.remove(ctx);
            }
            ctx.subscribed.remove(channel);
            replies.add(array(List.of(bulk("unsubscribe"), bulk(channel), integer(ctx.subscribed.size()))));
        }
        return concat(replies.toArray(new byte[0][]));
    }

    private void unsubscribeAll(ClientCtx ctx) {
        for (String channel : new ArrayList<>(ctx.subscribed)) {
            CopyOnWriteArraySet<ClientCtx> subs = channels.get(channel);
            if (subs != null) {
                subs.remove(ctx);
            }
        }
        ctx.subscribed.clear();
    }

    // ------------------------------------------------------- info / config

    private String info(String section) {
        StringBuilder sb = new StringBuilder();
        if (section == null || section.equalsIgnoreCase("server")) {
            sb.append("# Server\r\n");
            sb.append("redis_version:7.2.0\r\n");
            sb.append("tcp_port:").append(cfg.port).append("\r\n");
            sb.append("executable:java\r\n");
        }
        if (section == null || section.equalsIgnoreCase("replication")) {
            if (sb.length() > 0) {
                sb.append("\r\n");
            }
            sb.append("# Replication\r\n");
            if (isReplica) {
                sb.append("role:slave\r\n");
                sb.append("master_host:").append(cfg.replicaHost).append("\r\n");
                sb.append("master_port:").append(cfg.replicaPort).append("\r\n");
                sb.append("master_link_status:").append(upstreamUp ? "up" : "down").append("\r\n");
                sb.append("master_replid:").append(masterReplId).append("\r\n");
                sb.append("slave_repl_offset:").append(replOffset.get()).append("\r\n");
            } else {
                sb.append("role:master\r\n");
                sb.append("connected_slaves:").append(replicas.size()).append("\r\n");
                sb.append("master_replid:").append(REPL_ID).append("\r\n");
                sb.append("master_repl_offset:").append(replOffset.get()).append("\r\n");
            }
        }
        return sb.toString();
    }

    private byte[] config(List<String> parts) {
        if (!parts.get(1).equalsIgnoreCase("GET")) {
            throw new RedisError("ERR Only CONFIG GET is supported");
        }
        List<byte[]> out = new ArrayList<>();
        for (int i = 2; i < parts.size(); i++) {
            String param = parts.get(i).toLowerCase(Locale.ROOT);
            String value = switch (param) {
                case "dir" -> cfg.dir;
                case "dbfilename" -> cfg.dbfilename;
                case "appendonly" -> cfg.appendonly ? "yes" : "no";
                case "port" -> Integer.toString(cfg.port);
                default -> null;
            };
            if (value != null) {
                out.add(bulk(param));
                out.add(bulk(value));
            }
        }
        return array(out);
    }

    private byte[] client(List<String> parts) {
        String sub = parts.get(1).toUpperCase(Locale.ROOT);
        return switch (sub) {
            case "SETINFO", "SETNAME" -> simple("OK");
            case "GETNAME" -> nullBulk();
            case "ID" -> integer(0);
            default -> simple("OK");
        };
    }

    private byte[] hello(List<String> parts) {
        // RESP2-style reply (array of key/value pairs), enough for redis-cli.
        return array(List.of(
                bulk("server"), bulk("redis"),
                bulk("version"), bulk("7.2.0"),
                bulk("proto"), integer(2),
                bulk("id"), integer(0),
                bulk("mode"), bulk("standalone"),
                bulk("role"), bulk(isReplica ? "slave" : "master"),
                bulk("modules"), array(List.of())));
    }

    // ------------------------------------------------------- replication

    private byte[] replconf(ClientCtx ctx, List<String> parts) {
        String sub = parts.get(1).toUpperCase(Locale.ROOT);
        if (sub.equals("GETACK")) {
            // A replica answers GETACK with its offset. (The CodeCrafters
            // tester also sends GETACK to the master and expects an ACK
            // carrying the master's offset, so we answer on both sides.)
            return resp("REPLCONF", "ACK", Long.toString(replOffset.get()));
        }
        if (sub.equals("ACK")) {
            return simple("OK");
        }
        return simple("OK"); // listening-port, capa, ...
    }

    private byte[] psync(ClientCtx ctx, List<String> parts) {
        if (!parts.get(1).equals("?") || !parts.get(2).equals("-1")) {
            throw new RedisError("ERR unknown PSYNC form (only PSYNC ? -1 is supported)");
        }
        byte[] rdb = Rdb.buildEmpty();
        byte[] header = ("+FULLRESYNC " + REPL_ID + " 0\r\n").getBytes(StandardCharsets.UTF_8);
        ReplicaLink link = new ReplicaLink(ctx.socket, ctx.out, ctx.outLock);
        ctx.link = link;
        ctx.becameReplicaLink = true;
        return concat(header,
                ("$" + rdb.length + "\r\n").getBytes(StandardCharsets.UTF_8), rdb);
    }

    private byte[] waitForReplicas(List<String> parts) {
        int numReplicas;
        long timeoutMillis;
        try {
            numReplicas = Integer.parseInt(parts.get(1));
            timeoutMillis = Long.parseLong(parts.get(2));
        } catch (NumberFormatException e) {
            throw new RedisError("ERR value is not an integer or out of range");
        }
        if (numReplicas == 0 || replicas.isEmpty()) {
            return integer(0);
        }
        long targetOffset = replOffset.get();
        byte[] getack = resp("REPLCONF", "GETACK", "*");
        for (ReplicaLink link : replicas) {
            link.send(getack);
        }
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (true) {
            int caughtUp = 0;
            for (ReplicaLink link : replicas) {
                if (!link.closed && link.ackedOffset >= targetOffset) {
                    caughtUp++;
                }
            }
            if (caughtUp >= numReplicas || System.currentTimeMillis() >= deadline) {
                return integer(caughtUp);
            }
            try {
                lock.wait(10); // releases the store lock while waiting
                } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return integer(caughtUp);
                }
        }
    }

    /**
     * Replica mode: connects to the master, performs the handshake
     * (PING, REPLCONF listening-port, REPLCONF capa psync2, PSYNC ? -1),
     * loads the RDB the master sends, then applies every propagated
     * command to the local store. Reconnects after a disconnect.
     */
    private void runReplicaUpstream() {
        while (true) {
            try (Socket socket = new Socket(cfg.replicaHost, cfg.replicaPort)) {
                CountingInputStream counting = new CountingInputStream(socket.getInputStream());
                InputStream in = counting;
                OutputStream out = socket.getOutputStream();

                sendAndExpect(in, out, resp("PING"), "+PONG");
                sendAndExpect(in, out, resp("REPLCONF", "listening-port", Integer.toString(cfg.port)), "+OK");
                sendAndExpect(in, out, resp("REPLCONF", "capa", "psync2"), "+OK");
                out.write(resp("PSYNC", "?", "-1"));
                out.flush();
                String fullResync = readLine(in);
                if (!fullResync.startsWith("+FULLRESYNC")) {
                    throw new IOException("unexpected PSYNC reply: " + fullResync);
                }
                String[] fields = fullResync.substring(1).split(" ");
                if (fields.length >= 2) {
                    masterReplId = fields[1];
                }
                byte[] rdb = readBulk(in);
                if (rdb != null && rdb.length > 0) {
                    synchronized (lock) {
                        store.clear();
                        store.putAll(Rdb.parse(rdb));
                    }
                }
                replOffset.set(0);
                upstreamUp = true;
                System.out.println("replica: synced with master " + cfg.replicaHost + ":" + cfg.replicaPort);

                while (true) {
                    long before = counting.count;
                    List<String> command = readCommand(in);
                    if (command == null || command.isEmpty()) {
                        throw new IOException("master closed the connection");
                    }
                    long bytes = counting.count - before;
                    String name = command.get(0) == null ? "" : command.get(0).toUpperCase(Locale.ROOT);
                    if (name.equals("REPLCONF") && command.size() >= 3
                            && command.get(1).equalsIgnoreCase("GETACK")) {
                        // Answer with the offset so far (not counting this GETACK itself).
                        out.write(resp("REPLCONF", "ACK", Long.toString(replOffset.get())));
                        out.flush();
                    } else {
                        execute(MASTER_CTX, command, true);
                        replOffset.addAndGet(bytes);
                    }
                }
            } catch (Exception e) {
                upstreamUp = false;
                System.out.println("replica: lost master (" + e.getMessage() + "), retrying in 1s");
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
    }

    private static void sendAndExpect(InputStream in, OutputStream out, byte[] command, String expected)
            throws IOException {
        out.write(command);
        out.flush();
        String reply = readLine(in);
        if (!reply.equals(expected)) {
            throw new IOException("unexpected reply: " + reply + " (expected " + expected + ")");
        }
    }

    // ------------------------------------------------------- RDB

    /**
     * Minimal RDB reader/writer: enough to load the string keys (with
     * expiry) that real Redis writes, and to build the empty RDB a master
     * sends to a replica after PSYNC.
     *
     * <p>Format (see the RDB spec): {@code REDIS} + 4-digit version, then
     * opcodes — 0xFA AUX, 0xFE SELECTDB, 0xFB RESIZEDB, 0xFD expiry
     * (seconds), 0xFC expiry (ms), value-type byte (0 = string), key,
     * value, 0xFF EOF + 8-byte CRC64. Lengths use the standard
     * length-encoding (6-bit / 14-bit / 32-bit / special integer encoding).
     */
    private static final class Rdb {

        static Map<String, Value> parse(byte[] data) {
            Map<String, Value> result = new LinkedHashMap<>();
            if (data == null || data.length < 9) {
                return result;
            }
            String magic = new String(data, 0, 5, StandardCharsets.US_ASCII);
            if (!magic.equals("REDIS")) {
                throw new IllegalArgumentException("not an RDB file (bad magic)");
            }
            Reader r = new Reader(data, 9);
            long expiresAt = 0;
            while (r.pos < data.length) {
                int op = r.u8();
                switch (op) {
                    case 0xFF -> { return result; } // EOF (8-byte CRC follows)
                    case 0xFE -> r.readLength(); // SELECTDB
                    case 0xFB -> { r.readLength(); r.readLength(); } // RESIZEDB
                    case 0xFA -> { r.readString(); r.readString(); } // AUX field
                    case 0xFD -> expiresAt = r.u32le() * 1000L;
                    case 0xFC -> expiresAt = r.u64le();
                    default -> {
                        int type = op;
                        if (type != 0) {
                            System.out.println("RDB: unsupported value type " + type + ", stopping load");
                            return result;
                        }
                        String key = r.readString();
                        String value = r.readString();
                        if (expiresAt == 0 || expiresAt > System.currentTimeMillis()) {
                            result.put(key, new StrVal(value, expiresAt));
                        }
                        expiresAt = 0;
                    }
                }
            }
            return result;
        }

        /** Builds a valid empty RDB (header + AUX fields + EOF + CRC64). */
        static byte[] buildEmpty() {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            try {
                out.write("REDIS0011".getBytes(StandardCharsets.US_ASCII));
                writeAux(out, "redis-ver", "7.2.0");
                writeAux(out, "redis-bits", new byte[]{(byte) 0xC0, 64}); // int-encoded 64
                writeAux(out, "ctime", int32Encoded((int) (System.currentTimeMillis() / 1000)));
                writeAux(out, "used-mem", int32Encoded(1024 * 1024));
writeAux(out, "aof-base", new byte[]{(byte) 0xC0, 0}); // int-encoded 0
                out.write(0xFF);
                byte[] body = out.toByteArray();
                long crc = crc64(body);
                for (int i = 0; i < 8; i++) {
                    out.write((int) (crc & 0xFF));
                    crc >>>= 8;
                }
            } catch (IOException impossible) {
                throw new AssertionError(impossible);
            }
            return out.toByteArray();
        }

        private static void writeAux(ByteArrayOutputStream out, String key, String value) throws IOException {
            out.write(0xFA);
            writeLength(out, key.length());
            out.write(key.getBytes(StandardCharsets.US_ASCII));
            writeLength(out, value.length());
            out.write(value.getBytes(StandardCharsets.US_ASCII));
        }

        private static void writeAux(ByteArrayOutputStream out, String key, byte[] encodedValue) throws IOException {
            out.write(0xFA);
            writeLength(out, key.length());
            out.write(key.getBytes(StandardCharsets.US_ASCII));
            out.write(encodedValue);
        }

        /** Special integer encoding: 0xC2 + 4 bytes little-endian. */
        private static byte[] int32Encoded(int value) {
            return new byte[]{(byte) 0xC2,
                    (byte) value, (byte) (value >>> 8), (byte) (value >>> 16), (byte) (value >>> 24)};
        }

        private static void writeLength(ByteArrayOutputStream out, long len) {
            if (len < 64) {
                out.write((int) len);
            } else if (len < 16384) {
                out.write((int) (0x40 | (len >> 8)));
                out.write((int) (len & 0xFF));
            } else {
                out.write(0x80);
                out.write((int) (len >>> 24));
                out.write((int) (len >>> 16));
                out.write((int) (len >>> 8));
                out.write((int) len);
            }
        }

        /** CRC-64 with the Jones polynomial, reflected, init 0 — the checksum Redis
         * uses for RDB files. Verified against the empty RDB the CodeCrafters
         * tester sends (its last 8 bytes are this CRC of the preceding bytes). */
        static long crc64(byte[] data) {
            long crc = 0;
            for (byte b : data) {
                crc = CRC64_TABLE[(int) ((crc ^ b) & 0xFF)] ^ (crc >>> 8);
            }
            return crc;
        }

        private static final long[] CRC64_TABLE = buildCrc64Table();

        private static long[] buildCrc64Table() {
            final long poly = 0x95AC9329AC4BC9B5L; // bit-reversed Jones polynomial (reflected CRC-64)
            long[] table = new long[256];
            for (int i = 0; i < 256; i++) {
                long crc = i;
                for (int j = 0; j < 8; j++) {
                    crc = (crc & 1) != 0 ? (crc >>> 1) ^ poly : crc >>> 1;
                }
                table[i] = crc;
            }
            return table;
        }

        /** Cursor over the RDB bytes. */
        private static final class Reader {
            final byte[] data;
            int pos;

            Reader(byte[] data, int pos) {
                this.data = data;
                this.pos = pos;
            }

            int u8() {
                return data[pos++] & 0xFF;
            }

            long u32le() {
                long v = 0;
                for (int i = 0; i < 4; i++) {
                    v |= (long) (data[pos++] & 0xFF) << (8 * i);
                }
                return v;
            }

            long u64le() {
                long v = 0;
                for (int i = 0; i < 8; i++) {
                    v |= (long) (data[pos++] & 0xFF) << (8 * i);
                }
                return v;
            }

            /** Reads a length; returns -1 for special (integer) encodings and sets {@link #special}. */
            int special = -1;

            long readLength() {
                int b = u8();
                int type = (b & 0xC0) >> 6;
                switch (type) {
                    case 0 -> { return b & 0x3F; }
                    case 1 -> { return ((b & 0x3F) << 8) | u8(); }
                    case 2 -> {
                        long v = 0;
                        for (int i = 0; i < 4; i++) {
                            v = (v << 8) | u8();
                        }
                        return v;
                    }
                    default -> {
                        special = b & 0x3F;
                        return -1;
                    }
                }
            }

            String readString() {
                long len = readLength();
                if (len == -1) {
                    // Integer-encoded string.
                    switch (special) {
                        case 0 -> { return Byte.toString(data[pos++]); }
                        case 1 -> {
                            int v = (data[pos] & 0xFF) | ((data[pos + 1] & 0xFF) << 8);
                            pos += 2;
                            return Short.toString((short) v);
                        }
                        case 2 -> {
                            long v = 0;
                            for (int i = 0; i < 4; i++) {
                                v |= (long) (data[pos++] & 0xFF) << (8 * i);
                            }
                            return Integer.toString((int) v);
                        }
                        default -> throw new IllegalArgumentException("unsupported string encoding: " + special);
                    }
                }
                if (len > Integer.MAX_VALUE || pos + len > data.length) {
                    throw new IllegalArgumentException("RDB string length out of bounds: " + len);
                }
                String s = new String(data, pos, (int) len, StandardCharsets.UTF_8);
                pos += (int) len;
                return s;
            }
        }
    }
}
