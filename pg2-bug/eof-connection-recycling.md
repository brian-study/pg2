# pg2 Pool: Dead Connections Recycled After Server-Side TCP Close

## Summary

When PostgreSQL closes a TCP connection (idle timeout, server restart, or network interruption), the pg2 connection pool cannot detect the dead connection. It passes all existing validation checks and is recycled indefinitely. Every subsequent operation that reads a server response — `execute`, `begin`, `commit`, `query` — fails with `BufferUnderflowException` on the same dead connection in an infinite loop.

## Symptoms

```
java.nio.BufferUnderflowException:

java.nio.Buffer.nextGetIndex
java.nio.HeapByteBuffer.get
org.pg.Connection.readMessage  Connection.java: 553
org.pg.Connection.interact     Connection.java: 927
org.pg.Connection.begin        Connection.java:1465
pg.core/begin                  core.clj: 566
```

The error repeats on every pool borrow attempt, producing a high volume of identical log entries. Any caller that catches the exception and retries will borrow the same dead connection and fail identically — the pool never discards it.

## Root Cause

### How `readMessage` fails on a closed connection

`Connection.readMessage()` reads from the input stream in two steps:

```java
// Line 550-554
final byte[] bufHeader = IOTool.readNBytes(inStream, 5);
final ByteBuffer bbHeader = ByteBuffer.wrap(bufHeader);
final char tag = (char) bbHeader.get();    // ← BufferUnderflowException
final int bodySize = bbHeader.getInt() - 4;
```

`InputStream.readNBytes(5)` is documented to return **fewer bytes than requested** when the stream reaches EOF. It does **not** throw an IOException — it returns a short array (often 0 bytes). The code then wraps this short array in a `ByteBuffer` and calls `.get()`, which throws `BufferUnderflowException` because the buffer has no remaining bytes.

### Why the dead connection is recycled

After the exception, `pg/with-connection`'s `finally` block calls `pool.returnConnection(conn)`. The return path checks:

| Check | Result | Why |
|-------|--------|-----|
| `conn.isTxError()` | `false` | The transaction was never started — `BEGIN` failed before any state transition. |
| `conn.isTransaction()` | `false` | Same reason — no transaction state. |
| `conn.isClosed()` | `false` | `isClosed` is a local flag, only set by `Connection.close()` or `closeIO()`. The server closed the TCP connection, but the client never called `close()`. |

**All three checks pass.** The dead connection is added back to the free queue and cycles through the pool indefinitely.

### Connection lifecycle of the bug

```
1. PostgreSQL closes TCP connection (timeout, restart, network)
2. Connection sits in pool free queue with a dead socket
3. Caller borrows connection — pool has no way to detect the dead socket
4. Caller begins a transaction or executes a query
   → sendQuery("BEGIN") → may succeed (half-open TCP) or silently fail
   → interact() → readMessage() → readNBytes(5) returns byte[0]
   → ByteBuffer.wrap(byte[0]).get() → BufferUnderflowException
5. Exception propagates up through pg/with-connection
6. finally block calls pool.returnConnection(conn)
   → isTxError()=false, isTransaction()=false, isClosed()=false
   → All checks pass → connection added back to free queue
7. GOTO 3 (infinite loop — same dead connection, same error, every poll interval)
```

## Trigger Conditions

This bug manifests when the PostgreSQL server closes a pooled connection. Common causes:

- **`idle_session_timeout`** — PostgreSQL 14+ setting that terminates idle connections
- **`idle_in_transaction_session_timeout`** — terminates connections idle in a transaction
- **Server restart/failover** — all connections are terminated
- **Network interruption** — TCP connection drops silently
- **Load balancer/proxy timeout** — e.g., PgBouncer, HAProxy, AWS RDS Proxy, cloud SQL proxies with connection timeouts
- **`tcp_keepalive_time` expiry** — OS-level TCP keepalive detects dead peer

The bug is more likely with:
- Long-lived pools (production servers running for hours/days)
- Small pool sizes (fewer connections = higher chance the dead one is borrowed)
- Single-threaded worker/event loops that borrow one connection per iteration

## Reproduction

### Minimal reproduction

Simulate a server-side TCP close by shutting down the socket's input stream via reflection. This causes `readNBytes` to return EOF (empty arrays) without pg2's `isClosed` flag being set — exactly what happens when the server drops the connection.

See `pg2-bug/eof_repro.clj` for the full runnable script. Core logic:

```clojure
(require '[pg.core :as pg]
         '[pg.pool :as pool])
(import '[java.lang.reflect Field])

(defn get-private-field [obj field-name]
  (let [^Field field (-> (.getClass obj)
                         (.getDeclaredField field-name))]
    (.setAccessible field true)
    (.get field obj)))

(defn shutdown-socket-silently!
  "Shut down the underlying socket's input stream without calling pg2's
  close(). Simulates a server-side TCP close: reads return EOF but
  isClosed remains false."
  [conn]
  (let [io-channel (get-private-field conn "ioChannel")
        socket     (get-private-field io-channel "socket")]
    (.shutdownInput socket)))

(def pool-config
  {:host "127.0.0.1" :port 5432
   :user "test" :password "test" :database "test"
   :pool-min-size 1 :pool-max-size 1})

(def pool (pool/pool pool-config))

;; Warm the pool
(pg/with-connection [c pool]
  (pg/execute c "SELECT 1"))

;; Silently kill the socket (simulates server-side close)
(pg/with-connection [c pool]
  (shutdown-socket-silently! c))

;; Every subsequent borrow returns the same dead connection
(dotimes [i 3]
  (try
    (pg/with-connection [c pool]
      (println "isClosed:" (.isClosed c))  ;; => false (always)
      (pg/execute c "SELECT 1"))
    (catch Exception e
      (println "Attempt" (inc i) (.getName (.getClass e))))))

(.close pool)
```

Output:

```
isClosed: false
Attempt 1 java.nio.BufferUnderflowException
isClosed: false
Attempt 2 java.nio.BufferUnderflowException
isClosed: false
Attempt 3 java.nio.BufferUnderflowException
```

The dead connection is borrowed, fails, returned to the pool, and borrowed again — indefinitely.

### In production

Any application loop that borrows from a pool inside a retry loop will hit this:

```clojure
;; Typical event/worker loop — catches, logs, retries
(while running
  (try
    (pg/with-connection [c pool]
      (pg/with-tx [c]
        (process-events! c)))
    (catch Exception e
      (log/error e "Loop error")))
  (Thread/sleep poll-interval))
```

Every iteration borrows the same dead connection and fails with `BufferUnderflowException`. The error repeats indefinitely until the process is restarted.

## Proposed Fix

### 1. `Connection.readMessage` — detect EOF (primary fix)

Add a short-read check after `readNBytes` for both the header and the body. When a short read is detected, set `isClosed = true` so that `returnConnection` discards the connection instead of recycling it.

```java
private IServerMessage readMessage (final boolean skipMode) {

    final byte[] bufHeader = IOTool.readNBytes(inStream, 5);

    // Detect server-side connection close: readNBytes returns fewer bytes
    // than requested when the stream is at EOF. Without this check, wrapping
    // a short/empty array in ByteBuffer and calling .get() throws
    // BufferUnderflowException with no indication of the actual cause.
    if (bufHeader.length < 5) {
        isClosed = true;
        throw new PGError(
            "Connection closed by server: expected 5 header bytes, got %d. " +
            "This typically indicates the PostgreSQL server terminated the " +
            "connection (idle timeout, server restart, or network interruption).",
            bufHeader.length
        );
    }

    final ByteBuffer bbHeader = ByteBuffer.wrap(bufHeader);

    final char tag = (char) bbHeader.get();
    final int bodySize = bbHeader.getInt() - 4;

    if (skipMode) {
        if (tag == 'D' || tag == 'd') {
            IOTool.skip(inStream, bodySize);
            return SkippedMessage.INSTANCE;
        }
    }

    byte[] bufBody = IOTool.readNBytes(inStream, bodySize);

    // Also check the body read — the connection may close mid-message
    if (bufBody.length < bodySize) {
        isClosed = true;
        throw new PGError(
            "Connection closed by server: expected %d body bytes for " +
            "message '%c', got %d.",
            bodySize, tag, bufBody.length
        );
    }

    ByteBuffer bbBody = ByteBuffer.wrap(bufBody);
    // ... rest of switch on tag
```

**Why this works:** Setting `isClosed = true` before throwing causes `Pool.returnConnection` to hit the `isClosed()` check (line 245 in upstream `Pool.java`) and remove the connection from the pool. The pool then spawns a fresh connection on the next borrow. The dead connection is never recycled.

**Thread safety:** `readMessage` is always called from within the connection's `TryLock` (via `interact` → `begin`/`execute`), and `isClosed()` acquires the same lock. The exception unwinds through the lock's try-with-resources, releasing it before `returnConnection` re-acquires it.

### 2. `IOTool.readNBytes` — fail on short reads (alternative approach)

Instead of checking in `readMessage`, the check could be pushed down into `IOTool`:

```java
public static byte[] readNBytes (final InputStream inputStream, final int len) {
    try {
        final byte[] result = inputStream.readNBytes(len);
        if (result.length < len) {
            throw new IOException(
                String.format("Short read: expected %d bytes, got %d (stream closed or EOF)",
                    len, result.length));
        }
        return result;
    }
    catch (final IOException e) {
        throw new PGErrorIO(e, "Could not read %s byte(s), cause: %s", len, e.getMessage());
    }
}
```

This is a broader fix that would catch short reads everywhere, but it would throw `PGErrorIO` (an IO exception) rather than setting `isClosed`. Fix 1 is preferred because it provides a clear diagnostic message and ensures the connection is marked as closed for proper pool cleanup.

## Impact

Without the fix:
- One server-side connection close poisons the pool for that connection slot permanently
- Event loops enter infinite error loops producing high log volume
- Events queue up in the database but are never processed
- Manual intervention (restart) is required to recover

With fix 1:
- Dead connections are detected on first use and discarded
- The pool spawns a replacement connection automatically
- The event loop recovers after a single error + retry cycle
- No manual intervention needed

## Environment

- pg2 version: 0.1.44 (upstream `igrishaev/pg2`, confirmed present on `master` at 0.1.45-SNAPSHOT)
- PostgreSQL: 13, 16, 17
- Java: 21+
