/* Node test harness for demo/redis.js — the pure-JS Redis engine.
 * Run: node tests/test_redis_js.js   (exit 0 = all pass)
 * Exercises every command the live page advertises, including a
 * MULTI/EXEC block and an EXPIRE/TTL check (with a real short expiry).
 */
'use strict';
const RedisEngine = require('../demo/redis.js');

let passed = 0, failed = 0;
const failures = [];
function eq(actual, expected, label) {
  const a = JSON.stringify(actual), e = JSON.stringify(expected);
  if (a === e) { passed++; }
  else { failed++; failures.push(`${label}\n  expected: ${e}\n  actual:   ${a}`); }
}
function run(db, line) { const r = db.execute(RedisEngine.tokenize(line)); return r.error ? '(error) ' + r.error : r.value; }
function runStr(db, line) { return db.runLine(line); }

const db = new RedisEngine(null); // in-memory, no storage

// --- connection ---
eq(run(db, 'PING'), 'PONG', 'PING');
eq(run(db, 'PING hello'), 'hello', 'PING with arg');
eq(run(db, 'ECHO hello'), 'hello', 'ECHO');
eq(runStr(db, 'ECHO "hello world"'), '"hello world"', 'ECHO quoted display');

// --- strings ---
eq(run(db, 'SET name luna'), 'OK', 'SET');
eq(run(db, 'GET name'), 'luna', 'GET');
eq(run(db, 'GET missing'), null, 'GET missing -> nil');
eq(run(db, 'SET counter 10'), 'OK', 'SET counter');
eq(run(db, 'INCR counter'), 11, 'INCR');
eq(run(db, 'DECR counter'), 10, 'DECR');
eq(run(db, 'INCRBY counter 5'), 15, 'INCRBY');
eq(run(db, 'DECRBY counter 3'), 12, 'DECRBY');
eq(run(db, 'SET nxkey a NX'), 'OK', 'SET NX new');
eq(run(db, 'SET nxkey b NX'), null, 'SET NX existing -> nil');
eq(run(db, 'SET nxkey b XX'), 'OK', 'SET XX existing');
eq(run(db, 'SET missing2 b XX'), null, 'SET XX missing -> nil');
eq(run(db, 'SET name luna2 GET'), 'luna', 'SET GET returns old');
eq(run(db, 'GET name'), 'luna2', 'SET GET stored new');
eq(run(db, 'SET bad notanint'), 'OK', 'SET non-int');
eq(run(db, 'INCR bad'), '(error) ERR value is not an integer or out of range', 'INCR non-int errors');
eq(run(db, 'DEL name counter nxkey missing2 bad'), 4, 'DEL multiple (missing2 was never created: SET XX on missing key is a no-op)');
eq(run(db, 'EXISTS name counter'), 0, 'EXISTS after DEL');
eq(run(db, 'TYPE nosuchkey'), 'none', 'TYPE missing');
eq(run(db, 'SET typed str'), 'OK', 'SET typed');
eq(run(db, 'TYPE typed'), 'string', 'TYPE string');

// --- expiry ---
eq(run(db, 'SET temp v EX 100'), 'OK', 'SET EX');
eq(run(db, 'TTL temp') >= 0 && run(db, 'TTL temp') <= 100, true, 'TTL temp in range');
eq(run(db, 'EXPIRE typed 100'), 1, 'EXPIRE existing');
eq(run(db, 'TTL typed') >= 0 && run(db, 'TTL typed') <= 100, true, 'TTL typed in range');
eq(run(db, 'TTL nosuch'), -2, 'TTL missing = -2');
eq(run(db, 'SET noexp x'), 'OK', 'SET noexp');
eq(run(db, 'TTL noexp'), -1, 'TTL no expiry = -1');
eq(run(db, 'EXPIRE nosuch 10'), 0, 'EXPIRE missing = 0');
eq(run(db, 'PERSIST typed'), 1, 'PERSIST removes expiry');
eq(run(db, 'TTL typed'), -1, 'TTL after PERSIST = -1');
eq(run(db, 'PERSIST noexp'), 0, 'PERSIST no expiry = 0');

// --- lists ---
eq(run(db, 'RPUSH mylist a b c'), 3, 'RPUSH');
eq(run(db, 'LPUSH mylist z'), 4, 'LPUSH');
eq(run(db, 'LRANGE mylist 0 -1'), ['z', 'a', 'b', 'c'], 'LRANGE all');
eq(run(db, 'LRANGE mylist 1 2'), ['a', 'b'], 'LRANGE slice');
eq(run(db, 'LLEN mylist'), 4, 'LLEN');
eq(run(db, 'LPOP mylist'), 'z', 'LPOP');
eq(run(db, 'RPOP mylist'), 'c', 'RPOP');
eq(run(db, 'LRANGE mylist 0 -1'), ['a', 'b'], 'LRANGE after pops');
eq(run(db, 'LLEN nosuchlist'), 0, 'LLEN missing = 0');
eq(run(db, 'LPOP nosuchlist'), null, 'LPOP missing -> nil');

// --- sets ---
eq(run(db, 'SADD myset a b c'), 3, 'SADD');
eq(run(db, 'SADD myset b d'), 1, 'SADD dup adds 1');
eq(run(db, 'SMEMBERS myset'), ['a', 'b', 'c', 'd'], 'SMEMBERS sorted');
eq(run(db, 'SCARD myset'), 4, 'SCARD');
eq(run(db, 'SREM myset b x'), 1, 'SREM');
eq(run(db, 'SMEMBERS myset'), ['a', 'c', 'd'], 'SMEMBERS after SREM');

// --- hashes ---
eq(run(db, 'HSET user name luna age 3'), 2, 'HSET new fields');
eq(run(db, 'HSET user age 4'), 0, 'HSET existing field = 0');
eq(run(db, 'HGET user name'), 'luna', 'HGET');
eq(run(db, 'HGET user missing'), null, 'HGET missing field -> nil');
eq(run(db, 'HGETALL user'), ['name', 'luna', 'age', '4'], 'HGETALL');
eq(run(db, 'HLEN user'), 2, 'HLEN');
eq(run(db, 'HDEL user age'), 1, 'HDEL');

// --- sorted sets ---
eq(run(db, 'ZADD scores 10 alice 20 bob 15 carol'), 3, 'ZADD');
eq(run(db, 'ZRANGE scores 0 -1'), ['alice', 'carol', 'bob'], 'ZRANGE by score');
eq(run(db, 'ZRANGE scores 0 -1 WITHSCORES'), ['alice', '10', 'carol', '15', 'bob', '20'], 'ZRANGE WITHSCORES');
eq(run(db, 'ZRANK scores bob'), 2, 'ZRANK bob');
eq(run(db, 'ZRANK scores alice'), 0, 'ZRANK alice');
eq(run(db, 'ZRANK scores nosuch'), null, 'ZRANK missing -> nil');
eq(run(db, 'ZCARD scores'), 3, 'ZCARD');
eq(run(db, 'ZSCORE scores carol'), '15', 'ZSCORE');
eq(run(db, 'ZREM scores carol'), 1, 'ZREM');
eq(run(db, 'ZRANGE scores 0 -1'), ['alice', 'bob'], 'ZRANGE after ZREM');

// --- wrong type ---
eq(run(db, 'LPUSH typed x'), '(error) WRONGTYPE Operation against a key holding the wrong kind of value', 'WRONGTYPE list on string');
eq(run(db, 'GET mylist'), '(error) WRONGTYPE Operation against a key holding the wrong kind of value', 'WRONGTYPE string on list');

// --- keyspace / server ---
eq(run(db, 'KEYS *').includes('typed'), true, 'KEYS * contains typed');
eq(run(db, 'KEYS my*'), ['mylist', 'myset'], 'KEYS glob my*');
eq(run(db, 'DBSIZE'), db.dbsize(), 'DBSIZE consistent');
eq(typeof run(db, 'INFO'), 'string', 'INFO returns string');
eq(run(db, 'INFO').includes('redis_version'), true, 'INFO has version');
eq(run(db, 'FLUSHDB'), 'OK', 'FLUSHDB');
eq(run(db, 'DBSIZE'), 0, 'DBSIZE 0 after FLUSHDB');
eq(run(db, 'GET typed'), null, 'GET after FLUSHDB -> nil');

// --- MULTI / EXEC ---
eq(run(db, 'SET a 1'), 'OK', 'MULTI prep SET');
eq(run(db, 'MULTI'), 'OK', 'MULTI');
eq(run(db, 'SET b 2'), 'QUEUED', 'MULTI queues SET');
eq(run(db, 'INCR a'), 'QUEUED', 'MULTI queues INCR');
eq(run(db, 'GET b'), 'QUEUED', 'MULTI queues GET');
eq(run(db, 'EXEC'), ['OK', 2, '2'], 'EXEC runs queued (run() unwraps the exec result)');
eq(run(db, 'GET b'), '2', 'EXEC applied SET b');
eq(run(db, 'EXEC'), '(error) ERR EXEC without MULTI', 'EXEC without MULTI errors');
eq(run(db, 'DISCARD'), '(error) ERR DISCARD without MULTI', 'DISCARD without MULTI errors');
eq(run(db, 'MULTI'), 'OK', 'MULTI again');
eq(run(db, 'SET c 3'), 'QUEUED', 'queue before DISCARD');
eq(run(db, 'DISCARD'), 'OK', 'DISCARD');
eq(run(db, 'GET c'), null, 'DISCARD dropped queued SET');
eq(run(db, 'MULTI'), 'OK', 'MULTI nested prep');
eq(run(db, 'MULTI'), '(error) ERR MULTI calls can not be nested', 'nested MULTI errors');
eq(run(db, 'DISCARD'), 'OK', 'DISCARD after nested error');
eq(runStr(db, 'MULTI'), 'OK', 'runLine MULTI');
eq(runStr(db, 'SET d hello'), 'QUEUED', 'runLine queued');
eq(runStr(db, 'EXEC'), '1) OK', 'runLine EXEC display');

// --- unknown command / arity ---
eq(run(db, 'NOSUCHCMD'), "(error) ERR unknown command 'nosuchcmd'", 'unknown command');
eq(run(db, 'GET'), "(error) ERR wrong number of arguments for 'get' command", 'GET arity');

// --- persistence round-trip (in-memory storage stub) ---
const mem = { data: {}, getItem(k) { return this.data[k] || null; }, setItem(k, v) { this.data[k] = v; }, removeItem(k) { delete this.data[k]; } };
const db2 = new RedisEngine(mem);
run(db2, 'SET persistkey keepme');
run(db2, 'RPUSH plist x y');
run(db2, 'SADD pset m1 m2');
const db3 = new RedisEngine(mem); // reload from same storage
eq(run(db3, 'GET persistkey'), 'keepme', 'persisted string survives reload');
eq(run(db3, 'LRANGE plist 0 -1'), ['x', 'y'], 'persisted list survives reload');
eq(run(db3, 'SMEMBERS pset'), ['m1', 'm2'], 'persisted set survives reload');

// --- real expiry (short TTL, waits ~1.2s) ---
const db4 = new RedisEngine(null);
run(db4, 'SET short lived PX 300');
eq(run(db4, 'GET short'), 'lived', 'short-lived key present before expiry');
setTimeout(() => {
  eq(run(db4, 'GET short'), null, 'short-lived key expired after 300ms');
  eq(run(db4, 'TTL short'), -2, 'TTL expired key = -2');
  eq(run(db4, 'EXPIRE short 10'), 0, 'EXPIRE on expired key = 0');

  console.log(`\n${passed} passed, ${failed} failed`);
  if (failures.length) { console.log('\nFAILURES:\n' + failures.join('\n---\n')); process.exit(1); }
  console.log('All JS Redis engine tests passed.');
}, 1200);
