import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;

/**
 * A minimal but real Redis clone in pure Java (standard library only).
 *
 * <p>Listens on TCP port 6379 and speaks the RESP protocol (the same wire
 * protocol real Redis uses), so it can be driven by redis-cli or any RESP
 * client. Each client connection is handled on its own virtual thread
 * (Java 21), so many clients can talk to the server at the same time.
 *
 * <p>Supported commands:
 * <ul>
 *   <li>PING [message] — replies +PONG (or echoes the message)</li>
 *   <li>ECHO message — replies with the message as a bulk string</li>
 *   <li>SET key value [EX seconds | PX milliseconds] — stores a value,
 *       optionally with an expiry; replies +OK</li>
 *   <li>GET key — returns the value, or a null bulk string if the key is
 *       missing or expired</li>
 *   <li>DEL key [key ...] — deletes keys, replies with the number deleted</li>
 *   <li>EXISTS key [key ...] — replies with the number of existing keys</li>
 *   <li>TTL key — seconds until expiry, -1 = no expiry, -2 = missing</li>
 * </ul>
 *
 * <p>Data is held in memory only and is lost when the server stops.
 * Expired keys are removed lazily on access and by a background sweeper
 * that runs every second.
 *
 * <p>Run: {@code java src/RedisServer.java} (Java 21+, no build step)
 * or compile first: {@code javac -d out src/RedisServer.java && java -cp out RedisServer}
 */
public class RedisServer {

    public static final int PORT = 6379;

    /** A stored value plus the epoch-millis at which it expires (0 = never). */
    private record Entry(String value, long expiresAtMillis) {
        boolean isExpired() {
            return expiresAtMillis != 0 && System.currentTimeMillis() >= expiresAtMillis;
        }
    }

    private final ConcurrentHashMap<String, Entry> store = new ConcurrentHashMap<>();

    public static void main(String[] args) throws IOException {
        int port = args.length > 0 ? Integer.parseInt(args[0]) : PORT;
        new RedisServer().start(port);
    }

    private void start(int port) throws IOException {
        // Background sweeper: purge expired keys once a second.
        Thread sweeper = new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep(1000);
                    store.entrySet().removeIf(e -> e.getValue().isExpired());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }, "expiry-sweeper");
        sweeper.setDaemon(true);
        sweeper.start();

        try (ServerSocket server = new ServerSocket(port)) {
            System.out.println("redis-clone listening on port " + port + " (RESP, Java " + Runtime.version().feature() + ")");
            System.out.println("Try:  printf '*1\\r\\n$4\\r\\nPING\\r\\n' | nc localhost " + port);
            ExecutorService clients = Executors.newVirtualThreadPerTaskExecutor();
            while (true) {
                Socket socket = server.accept();
                clients.execute(() -> handleClient(socket));
            }
        }
    }

    private void handleClient(Socket socket) {
        try (socket;
             InputStream in = socket.getInputStream();
             OutputStream out = socket.getOutputStream()) {
            while (true) {
                List<String> command = readCommand(in);
                if (command == null) {
                    return; // client disconnected
                }
                if (command.isEmpty()) {
                    continue;
                }
                out.write(execute(command));
                out.flush();
            }
        } catch (IOException ignored) {
            // Client went away mid-conversation; nothing to do.
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

    // ------------------------------------------------------------ commands

    private byte[] execute(List<String> parts) {
        String cmd = parts.get(0).toUpperCase(Locale.ROOT);
        try {
            return switch (cmd) {
                case "PING" -> parts.size() > 1
                        ? bulk(parts.get(1))
                        : simple("PONG");
                case "ECHO" -> {
                    requireArgs(parts, 2, "ECHO");
                    yield bulk(parts.get(1));
                }
                case "SET" -> doSet(parts);
                case "GET" -> {
                    requireArgs(parts, 2, "GET");
                    yield get(parts.get(1));
                }
                case "DEL" -> {
                    requireArgs(parts, 2, "DEL");
                    int deleted = 0;
                    for (int i = 1; i < parts.size(); i++) {
                        if (store.remove(parts.get(i)) != null) {
                            deleted++;
                        }
                    }
                    yield integer(deleted);
                }
                case "EXISTS" -> {
                    requireArgs(parts, 2, "EXISTS");
                    int found = 0;
                    for (int i = 1; i < parts.size(); i++) {
                        if (getEntry(parts.get(i)) != null) {
                            found++;
                        }
                    }
                    yield integer(found);
                }
                case "TTL" -> {
                    requireArgs(parts, 2, "TTL");
                    Entry e = getEntry(parts.get(1));
                    if (e == null) {
                        yield integer(-2);
                    }
                    if (e.expiresAtMillis() == 0) {
                        yield integer(-1);
                    }
                    long seconds = (e.expiresAtMillis() - System.currentTimeMillis() + 999) / 1000;
                    yield integer((int) Math.max(0, seconds));
                }
                case "COMMAND" -> simple("OK"); // lets redis-cli handshake succeed
                case "QUIT" -> simple("OK");
                default -> error("ERR unknown command '" + parts.get(0) + "'");
            };
        } catch (IllegalArgumentException e) {
            return error(e.getMessage());
        }
    }

    private byte[] doSet(List<String> parts) {
        requireArgs(parts, 3, "SET");
        String key = parts.get(1);
        String value = parts.get(2);
        long expiresAt = 0;
        for (int i = 3; i < parts.size(); i++) {
            String opt = parts.get(i).toUpperCase(Locale.ROOT);
            if (i + 1 >= parts.size()) {
                throw new IllegalArgumentException("ERR syntax error");
            }
            long amount;
            try {
                amount = Long.parseLong(parts.get(i + 1));
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("ERR value is not an integer or out of range");
            }
            switch (opt) {
                case "EX" -> expiresAt = System.currentTimeMillis() + amount * 1000;
                case "PX" -> expiresAt = System.currentTimeMillis() + amount;
                default -> throw new IllegalArgumentException("ERR syntax error");
            }
            i++;
        }
        store.put(key, new Entry(value, expiresAt));
        return simple("OK");
    }

    /** Returns the live entry for a key, deleting it first if it expired. */
    private Entry getEntry(String key) {
        Entry e = store.get(key);
        if (e != null && e.isExpired()) {
            store.remove(key, e);
            return null;
        }
        return e;
    }

    private byte[] get(String key) {
        Entry e = getEntry(key);
        return e == null ? nullBulk() : bulk(e.value());
    }

    private static void requireArgs(List<String> parts, int min, String cmd) {
        if (parts.size() < min) {
            throw new IllegalArgumentException(
                    "ERR wrong number of arguments for '" + cmd.toLowerCase(Locale.ROOT) + "' command");
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
        byte[] header = ("$" + bytes.length + "\r\n").getBytes(StandardCharsets.UTF_8);
        byte[] out = new byte[header.length + bytes.length + 2];
        System.arraycopy(header, 0, out, 0, header.length);
        System.arraycopy(bytes, 0, out, header.length, bytes.length);
        out[out.length - 2] = '\r';
        out[out.length - 1] = '\n';
        return out;
    }

    private static byte[] nullBulk() {
        return "$-1\r\n".getBytes(StandardCharsets.UTF_8);
    }
}
