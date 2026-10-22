// Room API for สนามสอบ ม.ต้น.
// State lives in Upstash Redis (REST API). Vercel's Upstash integration sets
// KV_REST_API_URL / KV_REST_API_TOKEN; a direct Upstash setup uses the
// UPSTASH_REDIS_REST_* names. Either works.
import crypto from "node:crypto";

const REDIS_URL = (process.env.UPSTASH_REDIS_REST_URL || process.env.KV_REST_API_URL || "").replace(/\/$/, "");
const REDIS_TOKEN = process.env.UPSTASH_REDIS_REST_TOKEN || process.env.KV_REST_API_TOKEN || "";

const TTL = 60 * 60 * 3;      // rooms expire 3 hours after the last write
const ONLINE_MS = 30000;      // a player who has not polled for 30 s is offline
const HOST_GRACE_MS = 15000;  // others may take over after the host is gone 15 s
const ROOM_LIST_MS = 60000;   // open-room list shows rooms whose host polled in the last minute
const SUBJECTS = ["math", "sci", "thai", "eng", "soc"];
const CODE_CHARS = "abcdefghjkmnpqrstuvwxyz23456789";

const metaKey = code => `sq:${code}:meta`;
const plKey = code => `sq:${code}:pl`;
const ROOMS = "sq:rooms";

async function redis(cmds) {
  const r = await fetch(REDIS_URL + "/pipeline", {
    method: "POST",
    headers: { Authorization: `Bearer ${REDIS_TOKEN}`, "Content-Type": "application/json" },
    body: JSON.stringify(cmds),
  });
  if (!r.ok) throw new Error("redis http " + r.status);
  const out = await r.json();
  return out.map(x => {
    if (x.error) throw new Error("redis: " + x.error);
    return x.result;
  });
}

const num = v => (typeof v === "number" && isFinite(v) ? v : 0);
const clamp = (v, lo, hi) => Math.max(lo, Math.min(hi, num(v)));
const cleanNick = s => String(s || "").replace(/[\u0000-\u001f\u007f​-‏‪-‮⁠-⁯]/g, "").trim().slice(0, 20);
const pubId = pid => crypto.createHash("sha256").update(pid).digest("hex").slice(0, 10);
const rid = () => crypto.randomBytes(6).toString("hex");
const newCode = () => Array.from(crypto.randomBytes(5), b => CODE_CHARS[b % CODE_CHARS.length]).join("");

function cleanProgress(p) {
  if (!p || typeof p !== "object") return null;
  return {
    round: String(p.round || "").slice(0, 16),
    score: clamp(p.score, 0, 1500),
    idx: clamp(p.idx, 0, 10),
    correct: clamp(p.correct, 0, 10),
    time: clamp(p.time, 0, 600000),
    done: !!p.done,
    marks: String(p.marks || "").replace(/[^01]/g, "").slice(0, 10),
  };
}

function pairs(flat) {
  const o = {};
  if (Array.isArray(flat)) for (let i = 0; i + 1 < flat.length; i += 2) o[flat[i]] = flat[i + 1];
  return o;
}

function parseJSON(s) {
  try { return JSON.parse(s); } catch { return null; }
}

function buildState(code, meta, all, pid, now) {
  const players = [];
  const hostSeen = num(Number(all["s:" + meta.host]));
  for (const f in all) {
    if (!f.startsWith("p:")) continue;
    const id = f.slice(2);
    const rec = parseJSON(all[f]);
    if (!rec) continue;
    const seen = Number(all["s:" + id]) || 0;
    const me = id === pid;
    const online = me || now - seen < ONLINE_MS;
    const doneThisRound = rec.p && meta.round && rec.p.round === meta.round && rec.p.done;
    if (!online && !doneThisRound) continue;
    players.push({ id: pubId(id), nick: rec.nick, color: rec.color, p: rec.p, host: id === meta.host, me, online });
  }
  return {
    code,
    meta: { v: meta.v, g: meta.g, s: meta.s, phase: meta.phase, round: meta.round },
    players,
    you: { host: meta.host === pid, member: ("p:" + pid) in all },
    hostOnline: meta.host === pid || now - hostSeen < HOST_GRACE_MS,
  };
}

async function readRoom(code) {
  const [m, flat] = await redis([["GET", metaKey(code)], ["HGETALL", plKey(code)]]);
  const meta = m ? parseJSON(m) : null;
  return { meta, all: pairs(flat) };
}

async function writeMeta(code, meta) {
  meta.v = num(meta.v) + 1;
  await redis([["SET", metaKey(code), JSON.stringify(meta), "EX", TTL], ["EXPIRE", plKey(code), TTL]]);
}

async function upsertPlayer(code, pid, rec, now, isHost) {
  const cmds = [
    ["HSET", plKey(code), "p:" + pid, JSON.stringify(rec), "s:" + pid, String(now)],
    ["EXPIRE", plKey(code), TTL],
    ["EXPIRE", metaKey(code), TTL],
  ];
  if (isHost) cmds.push(["ZADD", ROOMS, now, code]);
  await redis(cmds);
}

async function listRooms(now) {
  const [codes] = await redis([["ZRANGEBYSCORE", ROOMS, now - ROOM_LIST_MS, "+inf", "LIMIT", 0, 20]]);
  if (!codes || !codes.length) return { rooms: [] };
  const res = await redis(codes.flatMap(c => [["GET", metaKey(c)], ["HLEN", plKey(c)]]));
  const rooms = [];
  codes.forEach((code, i) => {
    const meta = parseJSON(res[i * 2]);
    if (!meta) return;
    rooms.push({ code, g: meta.g, s: meta.s, phase: meta.phase, n: Math.max(1, Math.floor(num(res[i * 2 + 1]) / 2)) });
  });
  return { rooms };
}

export default async function handler(req, res) {
  res.setHeader("Cache-Control", "no-store");
  const send = (status, body) => res.status(status).json(body);
  if (req.method !== "POST") return send(405, { error: "method_not_allowed" });
  if (!REDIS_URL || !REDIS_TOKEN) return send(500, { error: "no_storage" });

  let body = req.body;
  if (typeof body === "string") body = parseJSON(body);
  if (!body || typeof body !== "object") return send(400, { error: "bad_body" });

  const now = Date.now();
  const action = String(body.action || "");

  try {
    if (action === "rooms") return send(200, await listRooms(now));

    const pid = String(body.pid || "");
    if (!/^[a-z0-9]{16,40}$/.test(pid)) return send(400, { error: "bad_pid" });

    if (action === "create") {
      const nick = cleanNick(body.nick);
      if (!nick) return send(400, { error: "bad_nick" });
      const g = [1, 2, 3].includes(body.g) ? body.g : 1;
      const s = SUBJECTS.includes(body.s) ? body.s : "math";
      let code = null, meta = null;
      for (let i = 0; i < 6 && !code; i++) {
        const c = newCode();
        const m = { v: 1, host: pid, g, s, phase: "lobby", round: null, created: now };
        const [ok] = await redis([["SET", metaKey(c), JSON.stringify(m), "NX", "EX", TTL]]);
        if (ok === "OK") { code = c; meta = m; }
      }
      if (!code) return send(503, { error: "busy" });
      const rec = { nick, color: clamp(body.color, 0, 7) | 0, p: null };
      await upsertPlayer(code, pid, rec, now, true);
      await redis([["ZREMRANGEBYSCORE", ROOMS, "-inf", now - TTL * 1000]]);
      return send(200, buildState(code, meta, { ["p:" + pid]: JSON.stringify(rec), ["s:" + pid]: String(now) }, pid, now));
    }

    const code = String(body.code || "").toLowerCase();
    if (!/^[a-z0-9]{5}$/.test(code)) return send(400, { error: "bad_code" });
    const { meta, all } = await readRoom(code);
    if (!meta) return send(404, { error: "room_not_found" });
    const member = ("p:" + pid) in all;
    const isHost = meta.host === pid;

    switch (action) {
      case "state": {
        if (member) {
          const cmds = [["HSET", plKey(code), "s:" + pid, String(now)]];
          if (isHost) cmds.push(["ZADD", ROOMS, now, code]);
          await redis(cmds);
          all["s:" + pid] = String(now);
        }
        return send(200, buildState(code, meta, all, pid, now));
      }
      case "join": {
        const nick = cleanNick(body.nick);
        if (!nick) return send(400, { error: "bad_nick" });
        const old = member ? parseJSON(all["p:" + pid]) : null;
        const rec = { nick, color: clamp(body.color, 0, 7) | 0, p: old ? old.p : null };
        await upsertPlayer(code, pid, rec, now, isHost);
        all["p:" + pid] = JSON.stringify(rec);
        all["s:" + pid] = String(now);
        return send(200, buildState(code, meta, all, pid, now));
      }
      case "progress": {
        if (!member) return send(403, { error: "not_member" });
        const old = parseJSON(all["p:" + pid]) || {};
        const rec = { nick: cleanNick(body.nick) || old.nick || "ผู้เล่น", color: clamp(body.color ?? old.color, 0, 7) | 0, p: cleanProgress(body.p) };
        await upsertPlayer(code, pid, rec, now, isHost);
        return send(200, { ok: true });
      }
      case "leave": {
        await redis([["HDEL", plKey(code), "p:" + pid, "s:" + pid]]);
        return send(200, { ok: true });
      }
      case "config":
      case "start":
      case "newgame": {
        if (!isHost) return send(403, { error: "not_host" });
        if (action === "config") {
          if (meta.phase !== "lobby") return send(409, { error: "playing" });
          if ([1, 2, 3].includes(body.g)) meta.g = body.g;
          if (SUBJECTS.includes(body.s)) meta.s = body.s;
        } else if (action === "start") {
          meta.phase = "play";
          meta.round = rid();
        } else {
          meta.phase = "lobby";
          meta.round = null;
        }
        await writeMeta(code, meta);
        return send(200, buildState(code, meta, all, pid, now));
      }
      case "takeover": {
        if (!member) return send(403, { error: "not_member" });
        const st = buildState(code, meta, all, pid, now);
        if (!isHost && st.hostOnline) return send(409, { error: "host_online" });
        meta.host = pid;
        await writeMeta(code, meta);
        return send(200, buildState(code, meta, all, pid, now));
      }
      default:
        return send(400, { error: "bad_action" });
    }
  } catch (e) {
    console.error(e);
    return send(500, { error: "server_error" });
  }
}
