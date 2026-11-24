/* Pure-JavaScript Redis engine — 100% in-browser, no backend, no CDN.
 *
 * Mirrors the command surface of src/RedisServer.java (the Java RESP server
 * in this repo) for the commands it shares, and adds the hashes / sets the
 * Java tutorial deliberately leaves out, so the live demo can show them too.
 * Data persists in localStorage (browser) or in memory (Node tests).
 * Output formatting follows redis-cli style: "quoted" strings, (nil),
 * (integer) N, 1) "a" lists, (error) ERR ... messages.
 *
 * Works in a browser as window.RedisEngine and in Node via module.exports.
 */
(function (global) {
  'use strict';

  var WRONGTYPE = 'WRONGTYPE Operation against a key holding the wrong kind of value';
  var STORAGE_KEY = 'redis-live-db-v1';

  function isIntStr(s) { return /^-?\d+$/.test(s); }

  function nowMs() { return Date.now(); }

  /* ---------------- tokeniser: handles "double quotes", 'single', \escapes --- */
  function tokenize(line) {
    var tokens = [], cur = '', inD = false, inS = false, esc = false, has = false;
    for (var i = 0; i < line.length; i++) {
      var c = line[i];
      if (esc) { cur += (c === 'n' ? '\n' : c === 't' ? '\t' : c === 'r' ? '\r' : c); esc = false; has = true; continue; }
      if (c === '\\' && (inD || inS)) { esc = true; continue; }
      if (c === '"' && !inS) { inD = !inD; has = true; continue; }
      if (c === "'" && !inD) { inS = !inS; has = true; continue; }
      if (!inD && !inS && (c === ' ' || c === '\t')) {
        if (has || cur.length) { tokens.push(cur); cur = ''; has = false; }
        continue;
      }
      cur += c; has = true;
    }
    if (has || cur.length) tokens.push(cur);
    return tokens;
  }

  /* ---------------- display formatting (redis-cli style) ---------------- */
  function fmtStr(s) { return '"' + s + '"'; }
  function fmt(v) {
    if (v === null || v === undefined) return '(nil)';
    if (typeof v === 'number') return '(integer) ' + v;
    if (typeof v === 'string') return v === 'OK' || v === 'PONG' || v === 'QUEUED' || v === 'RESET'
      ? v : fmtStr(v);
    if (Array.isArray(v)) {
      if (!v.length) return '(empty array)';
      return v.map(function (e, i) { return (i + 1) + ') ' + fmt(e); }).join('\n');
    }
    return String(v);
  }
  function fmtError(msg) { return '(error) ' + msg; }

  function globToRegExp(pat) {
    var re = '^', i, c;
    for (i = 0; i < pat.length; i++) {
      c = pat[i];
      if (c === '*') re += '.*';
      else if (c === '?') re += '.';
      else if (c === '[') {
        var j = pat.indexOf(']', i);
        if (j === -1) re += '\\[';
        else { re += pat.slice(i, j + 1); i = j; }
      } else if (c === '\\' && i + 1 < pat.length) { re += '\\' + pat[++i]; }
      else re += c.replace(/[.+^${}()|]/g, '\\$&');
    }
    return new RegExp(re + '$');
  }

  function normIndex(idx, len) {
    if (idx < 0) idx = len + idx;
    return Math.max(0, Math.min(idx, len - 1));
  }

  /* ---------------- engine ---------------- */
  function RedisEngine(storage) {
    this.storage = storage || null; // {getItem,setItem,removeItem} or null
    this.store = new Map(); // key -> {type, value, expiresAt} expiresAt 0 = never
    this.inMulti = false;
    this.queue = [];
    this.load();
  }

  RedisEngine.prototype._live = function (key) {
    var e = this.store.get(key);
    if (!e) return null;
    if (e.expiresAt && nowMs() >= e.expiresAt) { this.store.delete(key); return null; }
    return e;
  };
  RedisEngine.prototype._get = function (key) { return this._live(key); };
  RedisEngine.prototype._setEntry = function (key, type, value, expiresAt) {
    this.store.set(key, { type: type, value: value, expiresAt: expiresAt || 0 });
  };
  RedisEngine.prototype._ttlMs = function (key) {
    var e = this._live(key);
    if (!e) return -2;
    if (!e.expiresAt) return -1;
    return Math.max(0, e.expiresAt - nowMs());
  };
  RedisEngine.prototype._wrongType = function (e, type) {
    if (e && e.type !== type) throw new Error(WRONGTYPE);
    return e;
  };
  RedisEngine.prototype._str = function (key) {
    var e = this._live(key);
    if (!e) return null;
    if (e.type !== 'string') throw new Error(WRONGTYPE);
    return e;
  };

  /* ---- persistence ---- */
  RedisEngine.prototype.save = function () {
    if (!this.storage) return;
    try {
      var obj = {};
      this.store.forEach(function (e, k) {
        var v = e.value;
        if (e.type === 'set') v = Array.from(v);
        else if (e.type === 'hash' || e.type === 'zset') v = Array.from(v.entries());
        obj[k] = { type: e.type, value: v, expiresAt: e.expiresAt };
      });
      this.storage.setItem(STORAGE_KEY, JSON.stringify(obj));
    } catch (err) { /* storage full / unavailable: keep running in memory */ }
  };
  RedisEngine.prototype.load = function () {
    if (!this.storage) return;
    var raw;
    try { raw = this.storage.getItem(STORAGE_KEY); } catch (err) { return; }
    if (!raw) return;
    try {
      var obj = JSON.parse(raw), self = this;
      Object.keys(obj).forEach(function (k) {
        var e = obj[k], v = e.value;
        if (e.type === 'set') v = new Set(v);
        else if (e.type === 'hash' || e.type === 'zset') v = new Map(v);
        else if (e.type === 'list') v = v.slice();
        self.store.set(k, { type: e.type, value: v, expiresAt: e.expiresAt || 0 });
      });
    } catch (err) { /* corrupt snapshot: start empty */ }
  };
  RedisEngine.prototype._persist = function () { this.save(); };

  RedisEngine.prototype.keys = function () {
    var out = [], self = this;
    this.store.forEach(function (e, k) { if (self._live(k)) out.push(k); });
    return out.sort();
  };
  RedisEngine.prototype.typeOf = function (key) {
    var e = this._live(key);
    return e ? e.type : 'none';
  };
  RedisEngine.prototype.dbsize = function () { return this.keys().length; };

  /* ---- command execution: returns {value} or {error} ---- */
  RedisEngine.prototype.execute = function (parts) {
    if (!parts || !parts.length) return { error: 'ERR empty command' };
    var cmd = String(parts[0]).toUpperCase();
    var self = this;

    // MULTI queueing (matches Java: queue everything except EXEC/DISCARD/MULTI)
    if (this.inMulti && cmd !== 'EXEC' && cmd !== 'DISCARD' && cmd !== 'MULTI') {
      this.queue.push(parts.slice());
      return { value: 'QUEUED' };
    }

    try {
      var r = this._exec(parts);
      if (cmd === 'EXEC' && r && r._isExec) return { value: r.results, _exec: true };
      return { value: r };
    } catch (err) {
      return { error: err.message || String(err) };
    }
  };

  RedisEngine.prototype._need = function (parts, n, cmd) {
    if (parts.length < n) throw new Error("ERR wrong number of arguments for '" + cmd.toLowerCase() + "' command");
  };
  RedisEngine.prototype._num = function (s) {
    if (!isIntStr(String(s))) throw new Error('ERR value is not an integer or out of range');
    var n = Number(s);
    if (!Number.isSafeInteger(n)) throw new Error('ERR value is not an integer or out of range');
    return n;
  };

  RedisEngine.prototype._exec = function (parts) {
    var cmd = String(parts[0]).toUpperCase();
    var self = this;
    switch (cmd) {
      /* ---- connection ---- */
      case 'PING': return parts.length > 1 ? parts[1] : 'PONG';
      case 'ECHO': this._need(parts, 2, cmd); return parts[1];
      case 'QUIT': return 'OK';
      case 'RESET': this.inMulti = false; this.queue = []; return 'RESET';
      case 'SELECT': this._need(parts, 2, cmd); if (parts[1] !== '0') throw new Error('ERR DB index is out of range'); return 'OK';
      case 'COMMAND': return 'OK';

      /* ---- strings ---- */
      case 'SET': {
        this._need(parts, 3, cmd);
        var key = parts[1], val = parts[2], i, ex = 0, nx = false, xx = false, get = false, keepttl = false;
        for (i = 3; i < parts.length; i++) {
          var o = parts[i].toUpperCase();
          if (o === 'EX') ex = this._num(parts[++i]) * 1000;
          else if (o === 'PX') ex = this._num(parts[++i]);
          else if (o === 'NX') nx = true;
          else if (o === 'XX') xx = true;
          else if (o === 'GET') get = true;
          else if (o === 'KEEPTTL') keepttl = true;
          else throw new Error('ERR syntax error');
        }
        var old = this._str(key);
        if (nx && old) return get ? (old ? old.value : null) : null;
        if (xx && !old) return get ? null : null;
        var oldVal = old ? old.value : null;
        var expiresAt = keepttl && old ? old.expiresAt : (ex ? nowMs() + ex : 0);
        this._setEntry(key, 'string', val, expiresAt);
        this._persist();
        return get ? oldVal : 'OK';
      }
      case 'GET': this._need(parts, 2, cmd); { var g = this._str(parts[1]); return g ? g.value : null; }
      case 'DEL': {
        this._need(parts, 2, cmd);
        var n = 0;
        for (var d = 1; d < parts.length; d++) if (this._live(parts[d])) { this.store.delete(parts[d]); n++; }
        if (n) this._persist();
        return n;
      }
      case 'EXISTS': {
        this._need(parts, 2, cmd);
        var c = 0;
        for (var x = 1; x < parts.length; x++) if (this._live(parts[x])) c++;
        return c;
      }
      case 'TYPE': this._need(parts, 2, cmd); return this.typeOf(parts[1]);
      case 'INCR': case 'DECR': case 'INCRBY': case 'DECRBY': {
        var by = 1;
        if (cmd === 'DECR') by = -1;
        if (cmd === 'INCRBY' || cmd === 'DECRBY') { this._need(parts, 3, cmd); by = this._num(parts[2]) * (cmd === 'DECRBY' ? -1 : 1); }
        else this._need(parts, 2, cmd);
        var cur = this._str(parts[1]);
        var curN = 0;
        if (cur) {
          if (!isIntStr(cur.value)) throw new Error('ERR value is not an integer or out of range');
          curN = Number(cur.value);
        }
        var nv = curN + by;
        this._setEntry(parts[1], 'string', String(nv), cur ? cur.expiresAt : 0);
        this._persist();
        return nv;
      }
      case 'EXPIRE': case 'PEXPIRE': {
        this._need(parts, 3, cmd);
        var e = this._live(parts[1]);
        if (!e) return 0;
        var ms = this._num(parts[2]) * (cmd === 'EXPIRE' ? 1000 : 1);
        e.expiresAt = nowMs() + ms;
        this._persist();
        return 1;
      }
      case 'PERSIST': {
        this._need(parts, 2, cmd);
        var p = this._live(parts[1]);
        if (!p || !p.expiresAt) return 0;
        p.expiresAt = 0; this._persist(); return 1;
      }
      case 'TTL': this._need(parts, 2, cmd); { var t = this._ttlMs(parts[1]); return t < 0 ? t : Math.floor((t + 999) / 1000); }
      case 'PTTL': this._need(parts, 2, cmd); return this._ttlMs(parts[1]);

      /* ---- lists ---- */
      case 'LPUSH': case 'RPUSH': {
        this._need(parts, 3, cmd);
        var le = this._wrongType(this._live(parts[1]), 'list');
        if (!le) { le = { type: 'list', value: [], expiresAt: 0 }; this.store.set(parts[1], le); }
        var vals = parts.slice(2);
        if (cmd === 'LPUSH') for (var li = 0; li < vals.length; li++) le.value.unshift(vals[li]);
        else for (var ri = 0; ri < vals.length; ri++) le.value.push(vals[ri]);
        this._persist();
        return le.value.length;
      }
      case 'LPOP': case 'RPOP': {
        this._need(parts, 2, cmd);
        var pe = this._wrongType(this._live(parts[1]), 'list');
        if (!pe || !pe.value.length) return null;
        var count = parts.length > 2 ? this._num(parts[2]) : 1;
        if (count <= 0) throw new Error('ERR value is out of range, must be positive');
        var popped = [];
        while (count-- > 0 && pe.value.length) popped.push(cmd === 'LPOP' ? pe.value.shift() : pe.value.pop());
        if (!pe.value.length) this.store.delete(parts[1]);
        this._persist();
        return parts.length > 2 ? popped : popped[0];
      }
      case 'LRANGE': {
        this._need(parts, 4, cmd);
        var re = this._wrongType(this._live(parts[1]), 'list');
        if (!re) return [];
        var start = this._num(parts[2]), stop = this._num(parts[3]), len = re.value.length;
        if (start < 0) start += len; if (stop < 0) stop += len;
        start = Math.max(0, start); stop = Math.min(len - 1, stop);
        return start > stop ? [] : re.value.slice(start, stop + 1);
      }
      case 'LLEN': {
        this._need(parts, 2, cmd);
        var lle = this._wrongType(this._live(parts[1]), 'list');
        return lle ? lle.value.length : 0;
      }
      case 'LREM': {
        this._need(parts, 4, cmd);
        var lre = this._wrongType(this._live(parts[1]), 'list');
        if (!lre) return 0;
        var cnt = this._num(parts[2]), target = parts[3], removed = 0, arr = lre.value;
        if (cnt === 0) { var kept = arr.filter(function (v) { if (v === target) { removed++; return false; } return true; }); lre.value = kept; }
        else if (cnt > 0) { var out = []; for (var a = 0; a < arr.length; a++) { if (arr[a] === target && removed < cnt) removed++; else out.push(arr[a]); } lre.value = out; }
        else { var out2 = []; for (var b = arr.length - 1; b >= 0; b--) { if (arr[b] === target && removed < -cnt) removed++; else out2.unshift(arr[b]); } lre.value = out2; }
        if (!lre.value.length) this.store.delete(parts[1]);
        if (removed) this._persist();
        return removed;
      }

      /* ---- sets ---- */
      case 'SADD': {
        this._need(parts, 3, cmd);
        var se = this._wrongType(this._live(parts[1]), 'set');
        if (!se) { se = { type: 'set', value: new Set(), expiresAt: 0 }; this.store.set(parts[1], se); }
        var added = 0;
        for (var si = 2; si < parts.length; si++) if (!se.value.has(parts[si])) { se.value.add(parts[si]); added++; }
        if (added) this._persist();
        return added;
      }
      case 'SREM': {
        this._need(parts, 3, cmd);
        var sre = this._wrongType(this._live(parts[1]), 'set');
        if (!sre) return 0;
        var rem = 0;
        for (var sr = 2; sr < parts.length; sr++) if (sre.value.delete(parts[sr])) rem++;
        if (!sre.value.size) this.store.delete(parts[1]);
        if (rem) this._persist();
        return rem;
      }
      case 'SMEMBERS': {
        this._need(parts, 2, cmd);
        var sme = this._wrongType(this._live(parts[1]), 'set');
        return sme ? Array.from(sme.value).sort() : [];
      }
      case 'SCARD': {
        this._need(parts, 2, cmd);
        var sce = this._wrongType(this._live(parts[1]), 'set');
        return sce ? sce.value.size : 0;
      }

      /* ---- hashes ---- */
      case 'HSET': {
        this._need(parts, 4, cmd);
        if ((parts.length - 2) % 2 !== 0) throw new Error("ERR wrong number of arguments for 'hset' command");
        var he = this._wrongType(this._live(parts[1]), 'hash');
        if (!he) { he = { type: 'hash', value: new Map(), expiresAt: 0 }; this.store.set(parts[1], he); }
        var hadded = 0;
        for (var hi = 2; hi < parts.length; hi += 2) if (!he.value.has(parts[hi])) hadded++;
        for (var hj = 2; hj < parts.length; hj += 2) he.value.set(parts[hj], parts[hj + 1]);
        this._persist();
        return hadded;
      }
      case 'HGET': {
        this._need(parts, 3, cmd);
        var hge = this._wrongType(this._live(parts[1]), 'hash');
        if (!hge) return null;
        return hge.value.has(parts[2]) ? hge.value.get(parts[2]) : null;
      }
      case 'HGETALL': {
        this._need(parts, 2, cmd);
        var hga = this._wrongType(this._live(parts[1]), 'hash');
        if (!hga) return [];
        var flat = [];
        hga.value.forEach(function (v, k) { flat.push(k, v); });
        return flat;
      }
      case 'HDEL': {
        this._need(parts, 3, cmd);
        var hde = this._wrongType(this._live(parts[1]), 'hash');
        if (!hde) return 0;
        var hd = 0;
        for (var hdi = 2; hdi < parts.length; hdi++) if (hde.value.delete(parts[hdi])) hd++;
        if (!hde.value.size) this.store.delete(parts[1]);
        if (hd) this._persist();
        return hd;
      }
      case 'HLEN': {
        this._need(parts, 2, cmd);
        var hle = this._wrongType(this._live(parts[1]), 'hash');
        return hle ? hle.value.size : 0;
      }

      /* ---- sorted sets ---- */
      case 'ZADD': {
        this._need(parts, 4, cmd);
        var ze = this._wrongType(this._live(parts[1]), 'zset');
        if (!ze) { ze = { type: 'zset', value: new Map(), expiresAt: 0 }; this.store.set(parts[1], ze); }
        var zadded = 0;
        for (var zi = 2; zi + 1 < parts.length; zi += 2) {
          var sc = Number(parts[zi]);
          if (isNaN(sc)) throw new Error('ERR value is not a valid float');
          if (!ze.value.has(parts[zi + 1])) zadded++;
          ze.value.set(parts[zi + 1], sc);
        }
        this._persist();
        return zadded;
      }
      case 'ZRANGE': {
        this._need(parts, 4, cmd);
        var zre = this._wrongType(this._live(parts[1]), 'zset');
        if (!zre) return [];
        var sorted = Array.from(zre.value.entries()).sort(function (a, b) {
          return a[1] - b[1] || (a[0] < b[0] ? -1 : a[0] > b[0] ? 1 : 0);
        });
        var zs = this._num(parts[2]), ze2 = this._num(parts[3]), zl = sorted.length;
        if (zs < 0) zs += zl; if (ze2 < 0) ze2 += zl;
        zs = Math.max(0, zs); ze2 = Math.min(zl - 1, ze2);
        var slice = zs > ze2 ? [] : sorted.slice(zs, ze2 + 1);
        var withScores = parts.length > 4 && parts[4].toUpperCase() === 'WITHSCORES';
        var out = [];
        slice.forEach(function (e) { out.push(e[0]); if (withScores) out.push(String(e[1])); });
        return out;
      }
      case 'ZRANK': {
        this._need(parts, 3, cmd);
        var zrk = this._wrongType(this._live(parts[1]), 'zset');
        if (!zrk || !zrk.value.has(parts[2])) return null;
        var srt = Array.from(zrk.value.entries()).sort(function (a, b) {
          return a[1] - b[1] || (a[0] < b[0] ? -1 : a[0] > b[0] ? 1 : 0);
        });
        for (var zri = 0; zri < srt.length; zri++) if (srt[zri][0] === parts[2]) return zri;
        return null;
      }
      case 'ZCARD': {
        this._need(parts, 2, cmd);
        var zce = this._wrongType(this._live(parts[1]), 'zset');
        return zce ? zce.value.size : 0;
      }
      case 'ZSCORE': {
        this._need(parts, 3, cmd);
        var zse = this._wrongType(this._live(parts[1]), 'zset');
        if (!zse || !zse.value.has(parts[2])) return null;
        return String(zse.value.get(parts[2]));
      }
      case 'ZREM': {
        this._need(parts, 3, cmd);
        var zrm = this._wrongType(this._live(parts[1]), 'zset');
        if (!zrm) return 0;
        var zr = 0;
        for (var zri2 = 2; zri2 < parts.length; zri2++) if (zrm.value.delete(parts[zri2])) zr++;
        if (!zrm.value.size) this.store.delete(parts[1]);
        if (zr) this._persist();
        return zr;
      }

      /* ---- transactions ---- */
      case 'MULTI':
        if (this.inMulti) throw new Error('ERR MULTI calls can not be nested');
        this.inMulti = true; this.queue = []; return 'OK';
      case 'DISCARD':
        if (!this.inMulti) throw new Error('ERR DISCARD without MULTI');
        this.inMulti = false; this.queue = []; return 'OK';
      case 'EXEC': {
        if (!this.inMulti) throw new Error('ERR EXEC without MULTI');
        var queued = this.queue.slice();
        this.inMulti = false; this.queue = [];
        var results = [];
        for (var q = 0; q < queued.length; q++) {
          var qr = this.execute(queued[q]); // inMulti is false now, so runs for real
          results.push(qr.error ? fmtError(qr.error) : qr.value);
        }
        this._persist();
        return { _isExec: true, results: results };
      }

      /* ---- server / keyspace ---- */
      case 'KEYS': {
        this._need(parts, 2, cmd);
        var re = globToRegExp(parts[1]);
        return this.keys().filter(function (k) { return re.test(k); });
      }
      case 'FLUSHDB': case 'FLUSHALL':
        this.store.clear(); this._persist(); return 'OK';
      case 'DBSIZE': return this.dbsize();
      case 'INFO': {
        var section = parts.length > 1 ? parts[1].toLowerCase() : '';
        var lines = ['# Server', 'redis_version:7.2.0-js (in-browser)', 'redis_mode:standalone',
          'os:browser (pure JavaScript, no backend)', 'tcp_port:6379 (virtual)',
          '# Keyspace', 'db0:keys=' + this.dbsize() + ',expires=0',
          '# Replication', 'role:master', 'connected_slaves:0',
          '# Persistence', 'rdb:localStorage (key ' + STORAGE_KEY + ')',
          '# Note', 'note:this is the JS engine; the Java server also has streams, pub/sub and replication'];
        if (section === 'replication') lines = lines.slice(0, 2).concat(lines.slice(8, 10));
        if (section === 'keyspace') lines = lines.slice(0, 2).concat(lines.slice(6, 8));
        if (section === 'server') lines = lines.slice(0, 6);
        return lines.join('\r\n');
      }

      default: throw new Error("ERR unknown command '" + parts[0].toLowerCase() + "'");
    }
  };

  /* Run one line of input; returns the display string. */
  RedisEngine.prototype.runLine = function (line) {
    var parts = tokenize(line);
    if (!parts.length) return '';
    var r = this.execute(parts);
    if (r.error) return fmtError(r.error);
    if (r._exec) {
      if (!r.value.length) return '(empty array)';
      return r.value.map(function (v, i) { return (i + 1) + ') ' + fmt(v); }).join('\n');
    }
    return fmt(r.value);
  };

  /* Key-browser snapshot: [{key, type, ttl, preview}] */
  RedisEngine.prototype.inspect = function () {
    var self = this, out = [];
    this.keys().forEach(function (k) {
      var e = self._live(k), ttl = self._ttlMs(k), preview = '';
      if (e.type === 'string') preview = e.value;
      else if (e.type === 'list') preview = e.value.join(', ');
      else if (e.type === 'set') preview = Array.from(e.value).sort().join(', ');
      else if (e.type === 'hash') { var f = []; e.value.forEach(function (v, kk) { f.push(kk + '=' + v); }); preview = f.join(', '); }
      else if (e.type === 'zset') { var z = []; e.value.forEach(function (v, kk) { z.push(kk + ':' + v); }); preview = z.join(', '); }
      out.push({ key: k, type: e.type, ttl: ttl, preview: preview.slice(0, 80) });
    });
    return out;
  };

  RedisEngine.tokenize = tokenize;
  RedisEngine.format = fmt;
  global.RedisEngine = RedisEngine;
  if (typeof module !== 'undefined' && module.exports) module.exports = RedisEngine;
})(typeof window !== 'undefined' ? window : globalThis);
