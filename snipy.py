"""Discord vanity sniper — single-file build."""
from __future__ import annotations

import argparse
import asyncio
import base64
import collections
import hashlib
import json
import logging
import os
import random
import re
import signal
import sqlite3
import statistics
import threading
import time
from dataclasses import dataclass, field, fields
from datetime import date
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from logging.handlers import RotatingFileHandler
from pathlib import Path

from curl_cffi.requests import AsyncSession
from curl_cffi.requests.errors import RequestsError

API = "https://discord.com/api/v10"

ENV_MAP = {
    "SNIPY_GUILD_ID": "guild_id",
    "SNIPY_TOKENS": "tokens",
    "SNIPY_CODES": "codes",
    "SNIPY_PROXIES": "proxies",
    "SNIPY_WEBHOOK_URL": "webhook_url",
    "SNIPY_CACHE_PATH": "cache_path",
    "SNIPY_HEADLESS": "headless",
    "SNIPY_DRY_RUN": "dry_run",
    "SNIPY_WATCH": "watch",
    "SNIPY_POLL_INTERVAL": "poll_interval",
    "SNIPY_MAX_INFLIGHT": "max_inflight",
    "SNIPY_STATUS_ROTATE": "status_rotate",
    "SNIPY_DAILY_BUDGET": "daily_request_budget",
}
LIST_KEYS = {"tokens", "codes", "proxies"}
BOOL_KEYS = {"headless", "dry_run", "watch", "status_rotate"}
NUM_KEYS = {"poll_interval", "max_inflight", "daily_request_budget"}

SCHEMA = """
CREATE TABLE IF NOT EXISTS codes(
  code TEXT PRIMARY KEY,
  state TEXT NOT NULL DEFAULT 'unknown',
  last_checked REAL NOT NULL DEFAULT 0,
  next_check REAL NOT NULL DEFAULT 0,
  checks INTEGER NOT NULL DEFAULT 0,
  frees INTEGER NOT NULL DEFAULT 0,
  last_free REAL NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS free_events(code TEXT, ts REAL);
CREATE TABLE IF NOT EXISTS proxies(
  endpoint TEXT PRIMARY KEY,
  alive INTEGER NOT NULL DEFAULT 1,
  score REAL NOT NULL DEFAULT 1.0,
  fails INTEGER NOT NULL DEFAULT 0,
  uses INTEGER NOT NULL DEFAULT 0,
  last_ok REAL NOT NULL DEFAULT 0,
  next_probe REAL NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS ix_due ON codes(next_check);
CREATE INDEX IF NOT EXISTS ix_events ON free_events(code, ts);
"""

_VERSIONS = ["131.0.0.0", "132.0.0.0", "133.0.0.0", "134.0.0.0"]
_LOCALES = [
    ("en-US", "America/New_York"),
    ("en-US", "America/Chicago"),
    ("en-US", "America/Los_Angeles"),
    ("en-GB", "Europe/London"),
    ("en-CA", "America/Toronto"),
    ("de-DE", "Europe/Berlin"),
    ("nl-NL", "Europe/Amsterdam"),
]
_UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
       "(KHTML, like Gecko) Chrome/{v} Safari/537.36")

STATUS_TEXTS = [
    "afk", "in un posto", "fuori casa", "musica", "notte lunga",
    "studio", "pausa", "in giro", "btw", "a dopo", "sotto esame",
    "in viaggio", "weekend", "back later", "sleeping", "coffee",
    "be right back", "chilling", "offline soon",
]
STATUS_EMOJI = ["🌙", "☕", "🎧", "🌧️", "📚", "🍂", "🔥", "🧊", "🌊", "🕯️"]
PRESENCE = ["online", "online", "online", "online", "idle", "dnd"]


# ------------------------------------------------------------------ config

@dataclass(slots=True)
class Config:
    guild_id: str = ""
    tokens: list[str] = field(default_factory=list)
    codes: list[str] = field(default_factory=list)
    proxies: list[str] = field(default_factory=list)

    poll_interval: float = 1.5
    jitter: float = 0.55
    min_interval: float = 0.30
    max_interval: float = 45.0
    max_inflight: int = 12
    warmup_seconds: float = 120.0
    daily_request_budget: int = 0

    cache_path: str = "cache.sqlite3"
    cookie_dir: str = "cookies"
    taken_ttl: float = 55.0

    claim_concurrency: int = 3
    claim_retries: int = 2
    watch: bool = False
    watch_cooldown: float = 20.0
    dry_run: bool = False
    audit_reason: str = "vanity sniper"

    status_rotate: bool = True
    status_min_interval: float = 480.0
    status_max_interval: float = 1500.0
    status_restore: bool = True

    impersonate: str = "chrome"
    client_build_number: int = 354000
    timeout: float = 15.0
    proxy_health_url: str = f"{API}/gateway"
    proxy_health_interval: float = 45.0
    proxy_reprobe_interval: float = 60.0

    webhook_url: str = ""
    log_file: str = "sniper.log"
    headless: bool = False
    serve: bool = False

    @classmethod
    def load(cls, path: Path | None) -> "Config":
        raw: dict = {}
        if path and path.exists():
            raw = json.loads(path.read_text())

        for env, key in ENV_MAP.items():
            value = os.environ.get(env)
            if not value:
                continue
            if key in LIST_KEYS:
                raw[key] = [p.strip() for p in re.split(r"[,\n]", value) if p.strip()]
            elif key in BOOL_KEYS:
                raw[key] = value.lower() in ("1", "true", "yes", "on")
            elif key in NUM_KEYS:
                raw[key] = float(value)
            else:
                raw[key] = value

        if "proxies_file" in raw:
            p = Path(raw.pop("proxies_file"))
            if not p.is_absolute() and path:
                p = path.parent / p
            if p.exists():
                raw["proxies"] = [
                    ln.strip() for ln in p.read_text().splitlines()
                    if ln.strip() and not ln.startswith("#")
                ]

        volume = os.environ.get("RAILWAY_VOLUME_MOUNT_PATH")
        if volume:
            raw.setdefault("cache_path", str(Path(volume) / "cache.sqlite3"))
            raw.setdefault("cookie_dir", str(Path(volume) / "cookies"))

        known = {f.name for f in fields(cls)}
        unknown = set(raw) - known
        if unknown:
            raise SystemExit(f"unknown config keys: {', '.join(sorted(unknown))}")

        raw["guild_id"] = str(raw.get("guild_id", "")).strip()
        raw["codes"] = [c.lower() for c in raw.get("codes", [])]
        raw["daily_request_budget"] = int(raw.get("daily_request_budget", 0))
        raw["max_inflight"] = int(raw.get("max_inflight", 12))

        if not raw["guild_id"].isdigit():
            raise SystemExit("guild_id must be a snowflake")
        if not raw["tokens"]:
            raise SystemExit("no tokens (SNIPY_TOKENS)")
        if not raw["codes"]:
            raise SystemExit("no codes (SNIPY_CODES)")

        bad = [c for c in raw["codes"]
               if not c.replace("-", "").isalnum() or not 2 <= len(c) <= 32]
        if bad:
            raise SystemExit(f"invalid codes: {', '.join(bad)}")
        if raw["status_max_interval"] < raw["status_min_interval"]:
            raise SystemExit("status_max_interval < status_min_interval")

        Path(raw["cookie_dir"]).mkdir(parents=True, exist_ok=True)
        return cls(**raw)

    def masked(self, token: str) -> str:
        core = token[4:] if token.startswith("Bot ") else token
        if len(core) <= 12:
            return core[:4] + "…"
        return f"{core[:6]}…{core[-4:]}"


# ------------------------------------------------------------------- cache

@dataclass(slots=True)
class CodeRow:
    code: str
    state: str
    last_checked: float
    checks: int
    frees: int
    last_free: float


class Cache:
    def __init__(self, path: str, codes: list[str], proxies: list[str]) -> None:
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(path, check_same_thread=False, timeout=10)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=NORMAL")
        self.db.execute("PRAGMA busy_timeout=5000")
        self.db.executescript(SCHEMA)
        now = time.time()
        for code in codes:
            self.db.execute(
                "INSERT OR IGNORE INTO codes(code, next_check) VALUES(?,?)", (code, now))
        for endpoint in proxies:
            self.db.execute("INSERT OR IGNORE INTO proxies(endpoint) VALUES(?)", (endpoint,))
        self.db.commit()

    def interval_for(self, row: CodeRow, cfg, mult: float = 1.0) -> float:
        if row.state in ("free", "claiming", "owned"):
            return cfg.min_interval
        if row.frees == 0:
            return min(cfg.max_interval, cfg.poll_interval * 6) * mult
        recency = max(0.0, 1.0 - (time.time() - row.last_free) / 1800.0)
        boost = 1.0 + min(row.frees, 10) * 0.35
        base = (cfg.poll_interval * (1.0 - 0.8 * recency)) / boost
        return max(cfg.min_interval, base * mult)

    def _reschedule(self, code: str, cfg, state: str, mult: float) -> None:
        interval = self.interval_for(self.get(code), cfg, mult)
        if state == "taken":
            interval = max(interval, cfg.taken_ttl)
        interval *= 1.0 + random.uniform(0, cfg.jitter)
        self.db.execute("UPDATE codes SET state=?, next_check=? WHERE code=?",
                        (state, time.time() + interval, code))
        self.db.commit()

    def get(self, code: str) -> CodeRow:
        r = self.db.execute("SELECT * FROM codes WHERE code=?", (code,)).fetchone()
        return CodeRow(r["code"], r["state"], r["last_checked"],
                       r["checks"], r["frees"], r["last_free"])

    def all_codes(self) -> list[CodeRow]:
        rows = self.db.execute("SELECT * FROM codes ORDER BY code").fetchall()
        return [CodeRow(r["code"], r["state"], r["last_checked"],
                        r["checks"], r["frees"], r["last_free"]) for r in rows]

    def due(self, exclude: set[str], limit: int) -> list[str]:
        rows = self.db.execute(
            "SELECT code FROM codes WHERE next_check<=? "
            "ORDER BY frees DESC, next_check ASC", (time.time(),)).fetchall()
        return [r["code"] for r in rows if r["code"] not in exclude][:limit]

    def mark_taken(self, code: str, cfg, mult: float = 1.0) -> None:
        self.db.execute("UPDATE codes SET checks=checks+1, last_checked=? WHERE code=?",
                        (time.time(), code))
        self._reschedule(code, cfg, "taken", mult)

    def mark_free(self, code: str, cfg, mult: float = 1.0) -> None:
        now = time.time()
        self.db.execute(
            "UPDATE codes SET checks=checks+1, frees=frees+1, last_free=?, last_checked=? "
            "WHERE code=?", (now, now, code))
        self.db.execute("INSERT INTO free_events(code, ts) VALUES(?,?)", (code, now))
        self.db.execute(
            "DELETE FROM free_events WHERE code=? AND rowid NOT IN "
            "(SELECT rowid FROM free_events WHERE code=? ORDER BY ts DESC LIMIT 200)",
            (code, code))
        self._reschedule(code, cfg, "free", mult)

    def mark_error(self, code: str, cfg, mult: float = 1.0) -> None:
        self.db.execute("UPDATE codes SET checks=checks+1, last_checked=? WHERE code=?",
                        (time.time(), code))
        self._reschedule(code, cfg, "error", mult)

    def mark_owned(self, code: str, cfg) -> None:
        self.db.execute("UPDATE codes SET state='owned', next_check=? WHERE code=?",
                        (time.time() + cfg.min_interval, code))
        self.db.commit()

    def best_proxy(self, exclude: str | None = None) -> str | None:
        sql = "SELECT endpoint FROM proxies WHERE alive=1 AND score>=0.25"
        args: list = []
        if exclude:
            sql += " AND endpoint<>?"
            args.append(exclude)
        sql += " ORDER BY score DESC, uses ASC, RANDOM() LIMIT 1"
        row = self.db.execute(sql, args).fetchone()
        return row["endpoint"] if row else None

    def proxy_ok(self, endpoint: str) -> bool:
        r = self.db.execute("SELECT alive, score FROM proxies WHERE endpoint=?",
                            (endpoint,)).fetchone()
        return bool(r and r["alive"] and r["score"] >= 0.25)

    def proxy_use(self, endpoint: str) -> None:
        self.db.execute("UPDATE proxies SET uses=uses+1 WHERE endpoint=?", (endpoint,))
        self.db.commit()

    def proxy_recover(self, endpoint: str) -> None:
        self.db.execute(
            "UPDATE proxies SET alive=1, score=MIN(1.0, score+0.15), last_ok=?, next_probe=0 "
            "WHERE endpoint=?", (time.time(), endpoint))
        self.db.commit()

    def proxy_fail(self, endpoint: str) -> None:
        self.db.execute(
            "UPDATE proxies SET fails=fails+1, score=MAX(0.0, score-0.35) WHERE endpoint=?",
            (endpoint,))
        self.db.execute("UPDATE proxies SET alive=0 WHERE endpoint=? AND score<=0.05",
                        (endpoint,))
        self.db.commit()

    def benched(self) -> list[sqlite3.Row]:
        return self.db.execute(
            "SELECT * FROM proxies WHERE alive=0 AND next_probe<=? LIMIT 10",
            (time.time(),)).fetchall()

    def schedule_probe(self, endpoint: str, fails: int) -> None:
        delay = min(900.0, 30.0 * (2 ** min(fails, 5)))
        self.db.execute("UPDATE proxies SET next_probe=? WHERE endpoint=?",
                        (time.time() + delay, endpoint))
        self.db.commit()

    def proxies(self):
        return self.db.execute(
            "SELECT * FROM proxies ORDER BY alive DESC, score DESC").fetchall()

    def proxy_count(self) -> int:
        return self.db.execute("SELECT COUNT(*) c FROM proxies").fetchone()["c"]

    def churn(self) -> list[sqlite3.Row]:
        return self.db.execute(
            "SELECT code, state, checks, frees, last_free, last_checked FROM codes "
            "ORDER BY frees DESC, last_free DESC").fetchall()

    def free_timestamps(self, code: str) -> list[float]:
        return [r["ts"] for r in self.db.execute(
            "SELECT ts FROM free_events WHERE code=? ORDER BY ts", (code,))]

    def close(self) -> None:
        self.db.close()


# ---------------------------------------------------------------- identity

@dataclass(slots=True)
class Identity:
    user_agent: str
    sec_ch_ua: str
    locale: str
    timezone: str
    super_properties: str
    cookies: dict[str, str]
    impersonate: str

    def headers(self) -> dict[str, str]:
        return {
            "User-Agent": self.user_agent,
            "Accept": "*/*",
            "Accept-Language": f"{self.locale},en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br, zstd",
            "Origin": "https://discord.com",
            "Referer": "https://discord.com/channels/@me",
            "Sec-CH-UA": self.sec_ch_ua,
            "Sec-CH-UA-Mobile": "?0",
            "Sec-CH-UA-Platform": '"Windows"',
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
            "X-Discord-Locale": self.locale,
            "X-Discord-Timezone": self.timezone,
            "X-Super-Properties": self.super_properties,
            "X-Debug-Options": "bugReporterEnabled",
            "Priority": "u=1, i",
        }


def build_identity(token: str, impersonate: str, build_number: int) -> Identity:
    seed = token[4:] if token.startswith("Bot ") else token
    digest = hashlib.sha256(seed.encode()).digest()
    version = _VERSIONS[digest[0] % len(_VERSIONS)]
    major = version.split(".")[0]
    locale, tz = _LOCALES[digest[1] % len(_LOCALES)]
    ua = _UA.format(v=version)

    props = {
        "os": "Windows",
        "browser": "Chrome",
        "device": "",
        "system_locale": locale,
        "browser_user_agent": ua,
        "browser_version": version,
        "os_version": "10",
        "referrer": "",
        "referring_domain": "",
        "referrer_current": "",
        "referring_domain_current": "",
        "release_channel": "stable",
        "client_build_number": build_number,
        "client_event_source": None,
    }
    encoded = base64.b64encode(json.dumps(props, separators=(",", ":")).encode()).decode()

    return Identity(
        user_agent=ua,
        sec_ch_ua=f'"Chromium";v="{major}", "Google Chrome";v="{major}", "Not?A_Brand";v="24"',
        locale=locale,
        timezone=tz,
        super_properties=encoded,
        cookies={"__dcfduid": digest[:16].hex(), "__sdcfduid": digest[16:].hex()},
        impersonate=impersonate,
    )


# ------------------------------------------------------------------ status

class StatusRotator:
    def __init__(self, client: "AccountClient", cfg: Config, log) -> None:
        self.client = client
        self.cfg = cfg
        self.log = log
        self.original: dict | None = None
        self._stop = asyncio.Event()

    async def snapshot(self) -> None:
        r = await self.client.request("GET", "/users/@me/settings", retries=2)
        if r.status != 200:
            return
        try:
            data = json.loads(r.body)
        except Exception:
            return
        self.original = {
            "custom_status": data.get("custom_status"),
            "status": data.get("status") or "online",
        }

    async def _apply(self, custom: dict | None, presence: str) -> bool:
        r = await self.client.request("PATCH", "/users/@me/settings",
                                      json={"custom_status": custom, "status": presence},
                                      retries=2)
        return r.status == 200

    async def rotate(self) -> None:
        if random.random() < 0.15:
            custom = None
        else:
            custom = {
                "text": random.choice(STATUS_TEXTS),
                "emoji_name": random.choice(STATUS_EMOJI),
                "emoji_id": None,
                "expires_at": None,
            }
        await self._apply(custom, random.choice(PRESENCE))

    async def run(self) -> None:
        if self.client.is_bot or not self.cfg.status_rotate:
            return
        await asyncio.sleep(random.uniform(0, 75))
        await self.snapshot()
        while not self._stop.is_set():
            delay = random.uniform(self.cfg.status_min_interval,
                                   self.cfg.status_max_interval)
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=delay)
                return
            except asyncio.TimeoutError:
                pass
            if self.client.dead:
                return
            await self.rotate()

    async def restore(self) -> None:
        if not self.original or not self.cfg.status_restore or self.client.is_bot:
            return
        await self._apply(self.original.get("custom_status"),
                          self.original.get("status") or "online")


# ------------------------------------------------------------------- proxy

class ProxyPool:
    def __init__(self, cache: Cache, cfg: Config) -> None:
        self.cache = cache
        self.cfg = cfg
        self.log = lambda message: None
        self._bound: dict[str, str] = {}
        self._lock = asyncio.Lock()

    def __len__(self) -> int:
        return self.cache.proxy_count()

    def __bool__(self) -> bool:
        return len(self) > 0

    async def bind(self, token: str) -> str | None:
        async with self._lock:
            current = self._bound.get(token)
            if current and self.cache.proxy_ok(current):
                return current
            fresh = self.cache.best_proxy(exclude=current)
            if fresh:
                self._bound[token] = fresh
                self.cache.proxy_use(fresh)
            return fresh

    async def unbind(self, token: str, bad: str | None = None) -> None:
        async with self._lock:
            if bad:
                self.cache.proxy_fail(bad)
            self._bound.pop(token, None)

    async def health_loop(self, factory) -> None:
        while True:
            for row in self.cache.proxies():
                await self._probe(factory, row)
            await asyncio.sleep(self.cfg.proxy_health_interval)

    async def reprobe_loop(self, factory) -> None:
        while True:
            for row in self.cache.benched():
                endpoint = row["endpoint"]
                ok = await self._probe(factory, row)
                if ok:
                    self.log(f"proxy recovered: {endpoint}")
                else:
                    self.cache.schedule_probe(endpoint, row["fails"] + 1)
            await asyncio.sleep(self.cfg.proxy_reprobe_interval)

    async def _probe(self, factory, row) -> bool:
        probe = factory(row["endpoint"])
        try:
            r = await probe.request("GET", self.cfg.proxy_health_url, retries=1)
            ok = r.status == 200
        except Exception:
            ok = False
        finally:
            await probe.close()
        if ok:
            self.cache.proxy_recover(row["endpoint"])
        else:
            self.cache.proxy_fail(row["endpoint"])
        return ok


# ------------------------------------------------------------------ client

@dataclass(slots=True)
class Response:
    status: int
    body: str
    latency: float
    error: str = ""


class Bucket:
    def __init__(self, rate: float) -> None:
        self.rate = max(rate, 0.05)
        self._lock = asyncio.Lock()
        self._next = 0.0

    async def take(self) -> None:
        async with self._lock:
            now = time.monotonic()
            wait = max(0.0, self._next - now)
            self._next = max(now, self._next) + 1.0 / self.rate
        if wait:
            await asyncio.sleep(wait)


class AccountClient:
    def __init__(self, token: str, ident: Identity, pool: ProxyPool, cfg: Config,
                 stats: "Stats", forced_proxy: str | None = None) -> None:
        self.token = token
        self.is_bot = token.startswith("Bot ")
        self.ident = ident
        self.pool = pool
        self.cfg = cfg
        self.stats = stats
        self.forced_proxy = forced_proxy
        self.proxy: str | None = forced_proxy
        self.session: AsyncSession | None = None
        self.dead = False
        self.verified = True
        self.sent = 0
        self._bucket = Bucket(1.0 / cfg.poll_interval)
        self._cookie_path = (
            Path(cfg.cookie_dir) / f"{hashlib.sha256(token.encode()).hexdigest()[:16]}.json"
            if token else None
        )

    def budget_left(self) -> bool:
        if not self.cfg.daily_request_budget:
            return True
        return self.stats.today(self.token) < self.cfg.daily_request_budget

    def _load_cookies(self) -> dict:
        if not self._cookie_path or not self._cookie_path.exists():
            return {}
        try:
            return json.loads(self._cookie_path.read_text())
        except Exception:
            return {}

    def _save_cookies(self) -> None:
        if not self._cookie_path or self.session is None:
            return
        try:
            jar = {c.name: c.value for c in self.session.cookies.jar}
            if jar:
                self._cookie_path.parent.mkdir(parents=True, exist_ok=True)
                self._cookie_path.write_text(json.dumps(jar))
        except Exception:
            pass

    async def _ensure(self) -> bool:
        if self.forced_proxy is None and self.pool:
            proxy = await self.pool.bind(self.token)
            if proxy is None:
                return False
        else:
            proxy = self.forced_proxy

        if self.session is not None and proxy == self.proxy:
            return True

        await self.close()
        self.proxy = proxy
        proxies = {"http": proxy, "https": proxy} if proxy else None
        self.session = AsyncSession(
            impersonate=self.ident.impersonate,
            proxies=proxies,
            timeout=self.cfg.timeout,
            allow_redirects=False,
        )
        jar = self.ident.cookies | self._load_cookies()
        for name, value in jar.items():
            try:
                self.session.cookies.set(name, value, domain="discord.com")
            except Exception:
                pass
        return True

    async def request(self, method: str, path: str, retries: int = 4, **kw) -> Response:
        if self.dead:
            return Response(0, "", 0.0, "dead")
        if not self.budget_left():
            return Response(0, "", 0.0, "budget")

        await self._bucket.take()

        headers = self.ident.headers()
        if self.token:
            headers["Authorization"] = self.token
        headers.update(kw.pop("headers", {}))
        url = path if path.startswith("http") else API + path

        last = Response(0, "", 0.0, "no-attempt")
        for _ in range(retries):
            if not await self._ensure():
                last = Response(0, "", 0.0, "no-proxy")
                await asyncio.sleep(0.4)
                continue

            started = time.perf_counter()
            try:
                r = await self.session.request(method, url, headers=headers, **kw)
            except RequestsError as exc:
                self.stats.error(self.token)
                last = Response(0, "", 0.0, str(exc)[:80])
                await self.pool.unbind(self.token, self.proxy)
                self.proxy = None
                await asyncio.sleep(0.25)
                continue

            latency = (time.perf_counter() - started) * 1000
            self.stats.request(self.token, latency)
            self.sent += 1
            if self.sent % 20 == 0:
                self._save_cookies()

            if r.status_code == 429:
                try:
                    delay = float(json.loads(r.text).get("retry_after", 1.0))
                except Exception:
                    delay = 1.0
                self.stats.rate_limited(self.token)
                await asyncio.sleep(min(delay, 60.0) + 0.05)
                last = Response(429, r.text, latency)
                continue

            if r.status_code == 401:
                self.dead = True
            elif r.status_code == 400 and "captcha_key" in r.text:
                self.verified = False
                self.dead = True

            return Response(r.status_code, r.text, latency)

        return last

    async def close(self) -> None:
        if self.session is not None:
            self._save_cookies()
            try:
                await self.session.close()
            except Exception:
                pass
            self.session = None


# ------------------------------------------------------------------ engine

class Stats:
    def __init__(self) -> None:
        self.started = time.time()
        self.requests = 0
        self.errors = 0
        self.limited = 0
        self.claims = 0
        self.latencies: collections.deque[float] = collections.deque(maxlen=200)
        self.per_token: dict[str, dict] = {}
        self._day = date.today()
        self._today: dict[str, int] = {}

    def _roll(self) -> None:
        if date.today() != self._day:
            self._day = date.today()
            self._today.clear()

    def slot(self, token: str) -> dict:
        return self.per_token.setdefault(token, {"req": 0, "err": 0, "rl": 0})

    def today(self, token: str) -> int:
        self._roll()
        return self._today.get(token, 0)

    def request(self, token: str, latency: float) -> None:
        self._roll()
        self.requests += 1
        self.slot(token)["req"] += 1
        self._today[token] = self._today.get(token, 0) + 1
        self.latencies.append(latency)

    def error(self, token: str) -> None:
        self.errors += 1
        self.slot(token)["err"] += 1

    def rate_limited(self, token: str) -> None:
        self.limited += 1
        self.slot(token)["rl"] += 1

    @property
    def avg_latency(self) -> float:
        return sum(self.latencies) / len(self.latencies) if self.latencies else 0.0


class Engine:
    def __init__(self, cfg: Config, cache: Cache, pool: ProxyPool) -> None:
        self.cfg = cfg
        self.cache = cache
        self.pool = pool
        self.pool.log = self.log
        self.stats = Stats()
        self.stop = asyncio.Event()
        self.paused = False
        self.clients: list[AccountClient] = []
        self.rotators: list[StatusRotator] = []
        self.inflight: set[str] = set()
        self.state: dict[str, str] = {c: "unknown" for c in cfg.codes}
        self.log_lines: collections.deque[str] = collections.deque(maxlen=400)
        self._rr = 0
        self._locks = {c: asyncio.Lock() for c in cfg.codes}
        self._claimed: dict[str, float] = {}
        self._warm_until = time.time() + cfg.warmup_seconds

    def log(self, message: str) -> None:
        line = f"{time.strftime('%H:%M:%S')}  {message}"
        self.log_lines.append(line)
        print(line, flush=True)

    def warm_mult(self) -> float:
        return 3.0 if time.time() < self._warm_until else 1.0

    def _live(self) -> list[AccountClient]:
        return [c for c in self.clients if not c.dead and c.budget_left()]

    def _next_client(self) -> AccountClient | None:
        live = self._live()
        if not live:
            return None
        client = live[self._rr % len(live)]
        self._rr += 1
        return client

    def snapshot(self) -> dict:
        return {
            "uptime": int(time.time() - self.stats.started),
            "requests": self.stats.requests,
            "errors": self.stats.errors,
            "rate_limited": self.stats.limited,
            "claims": self.stats.claims,
            "avg_latency_ms": round(self.stats.avg_latency, 1),
            "accounts_alive": len(self._live()),
            "accounts_total": len(self.clients),
            "unverified": sum(1 for c in self.clients if not c.verified),
            "proxies": len(self.pool),
            "codes": {c: self.state.get(c) for c in self.cfg.codes},
        }

    async def preflight(self) -> None:
        if not self.clients:
            self.log("no tokens")
            self.stop.set()
            return

        probe = self.clients[0]
        r = await probe.request("GET", "/users/@me")
        if r.status == 200:
            try:
                self.log(f"auth ok: {json.loads(r.body).get('username')}")
            except Exception:
                self.log("auth ok")
        elif r.status == 401:
            self.log("ERROR: first token invalid")
        else:
            self.log(f"auth probe -> {r.status} {r.error}")

        r = await probe.request("GET", f"/guilds/{self.cfg.guild_id}/vanity-url")
        if r.status == 200:
            current = json.loads(r.body).get("code")
            self.log(f"current vanity: {current or '(none)'}")
            for code in self.cfg.codes:
                if code == current:
                    self.state[code] = "owned"
                    self._claimed[code] = time.monotonic()
        elif r.status == 403:
            self.log("WARN: token lacks Manage Server")
        elif r.status == 404:
            self.log("WARN: guild has no vanity (Boost Level 3 required)")

        if not self.pool:
            self.log("WARN: no proxies, single host IP")
        if self.cfg.dry_run:
            self.log("dry run active")
        if self.cfg.warmup_seconds:
            self.log(f"warm-up {self.cfg.warmup_seconds:.0f}s at 1/3 rate")

    async def scheduler(self) -> None:
        while not self.stop.is_set():
            if self.paused:
                await asyncio.sleep(0.1)
                continue
            capacity = self.cfg.max_inflight - len(self.inflight)
            if capacity > 0:
                for code in self.cache.due(self.inflight, capacity):
                    self.inflight.add(code)
                    asyncio.create_task(self.check(code))
            await asyncio.sleep(0.03)

    async def check(self, code: str) -> None:
        mult = self.warm_mult()
        try:
            client = self._next_client()
            if client is None:
                if not any(not c.dead for c in self.clients):
                    self.log("all accounts dead")
                    self.stop.set()
                return

            r = await client.request("GET", f"/invites/{code}")
            if r.status == 404:
                self.state[code] = "free"
                self.cache.mark_free(code, self.cfg, mult)
                await self.on_free(code)
            elif r.status == 200:
                if self.state[code] != "owned":
                    self.state[code] = "taken"
                self.cache.mark_taken(code, self.cfg, mult)
            elif r.status == 429 or r.error:
                self.cache.mark_error(code, self.cfg, mult)
            else:
                self.state[code] = f"http {r.status}"
                self.cache.mark_error(code, self.cfg, mult)
        finally:
            self.inflight.discard(code)

    async def on_free(self, code: str) -> None:
        lock = self._locks[code]
        if lock.locked():
            return
        async with lock:
            seen = self._claimed.get(code)
            if seen is not None:
                if not self.cfg.watch:
                    return
                if time.monotonic() - seen < self.cfg.watch_cooldown:
                    return
            await self.claim(code)

    async def claim(self, code: str) -> None:
        self.stats.claims += 1
        self.state[code] = "claiming"
        self.log(f"[{code}] free, claiming")
        await self.notify(f"`{code}` is free, claiming")

        if self.cfg.dry_run:
            self._claimed[code] = time.monotonic()
            self.cache.mark_owned(code, self.cfg)
            self.state[code] = "owned (dry)"
            return

        live = self._live()
        if not live:
            return

        start = self._rr % len(live)
        picked = (live[start:] + live[:start])[: self.cfg.claim_concurrency]
        tasks = [asyncio.create_task(self._claim_one(code, c)) for c in picked]

        pending, winner = set(tasks), None
        while pending:
            done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                if not task.cancelled() and task.result():
                    winner = task.result()
            if winner:
                for task in pending:
                    task.cancel()
                break

        if winner:
            self._claimed[code] = time.monotonic()
            self.cache.mark_owned(code, self.cfg)
            self.state[code] = "OWNED"
            self.log(f"[{code}] CLAIMED as {winner.get('code')}")
            await self.notify(f"claimed `{code}`")
        else:
            self.state[code] = "lost race"
            self.log(f"[{code}] claim failed")
            await self.notify(f"failed to claim `{code}`")

    async def _claim_one(self, code: str, client: AccountClient) -> dict | None:
        for _ in range(self.cfg.claim_retries):
            r = await client.request(
                "PATCH", f"/guilds/{self.cfg.guild_id}/vanity-url",
                json={"code": code},
                headers={"X-Audit-Log-Reason": self.cfg.audit_reason[:512]})
            if r.status == 200:
                try:
                    return json.loads(r.body)
                except Exception:
                    return {"code": code}
            if r.status in (400, 403):
                try:
                    self.log(f"[{code}] rejected: {json.loads(r.body).get('message')}")
                except Exception:
                    pass
                return None
            await asyncio.sleep(0.2 + random.random() * 0.3)
        return None

    async def notify(self, message: str) -> None:
        if not self.cfg.webhook_url:
            return
        try:
            async with AsyncSession(impersonate="chrome") as session:
                await session.post(self.cfg.webhook_url,
                                   json={"content": message[:1900],
                                         "username": "vanity sniper"})
        except Exception as exc:
            self.log(f"webhook error: {exc}")

    async def run(self) -> None:
        for token in self.cfg.tokens:
            ident = build_identity(token, self.cfg.impersonate, self.cfg.client_build_number)
            client = AccountClient(token, ident, self.pool, self.cfg, self.stats)
            self.clients.append(client)
            self.rotators.append(StatusRotator(client, self.cfg, self.log))

        await self.preflight()
        self.log(f"watching {len(self.cfg.codes)} code(s) with {len(self.clients)} account(s)")

        probes: list[AccountClient] = []

        def factory(proxy: str) -> AccountClient:
            ident = build_identity("probe", self.cfg.impersonate, self.cfg.client_build_number)
            client = AccountClient("", ident, self.pool, self.cfg, self.stats,
                                   forced_proxy=proxy)
            probes.append(client)
            return client

        tasks = [asyncio.create_task(self.scheduler())]
        tasks += [asyncio.create_task(r.run()) for r in self.rotators]
        if self.pool:
            tasks.append(asyncio.create_task(self.pool.health_loop(factory)))
            tasks.append(asyncio.create_task(self.pool.reprobe_loop(factory)))

        await self.stop.wait()

        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        for rotator in self.rotators:
            try:
                await rotator.restore()
            except Exception:
                pass
        for client in self.clients + probes:
            await client.close()
        self.log("stopped")


# ------------------------------------------------------------------ health

def _make_handler(engine: Engine):
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path == "/healthz":
                body, status = b"ok", 200
            elif self.path == "/status":
                body, status = json.dumps(engine.snapshot()).encode(), 200
            else:
                body, status = b"not found", 404
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args):
            pass

    return Handler


class HealthServer:
    def __init__(self, engine: Engine, port: int) -> None:
        self._server = ThreadingHTTPServer(("0.0.0.0", port), _make_handler(engine))

    def start(self) -> None:
        threading.Thread(target=self._server.serve_forever, daemon=True).start()

    def stop(self) -> None:
        self._server.shutdown()


# ------------------------------------------------------------------ report

def report(path: str, as_json: bool = False) -> None:
    from rich.console import Console
    from rich.table import Table

    cache = Cache(path, [], [])
    now = time.time()
    data = []

    for row in cache.churn():
        stamps = cache.free_timestamps(row["code"])
        gaps = [b - a for a, b in zip(stamps, stamps[1:])]
        median = statistics.median(gaps) if gaps else None
        if row["frees"] >= 3 and median is not None and median < 1800:
            verdict, style = "hot", "bold green"
        elif row["frees"] >= 1:
            verdict, style = "warm", "yellow"
        else:
            verdict, style = "cold", "dim"
        data.append({
            "code": row["code"],
            "state": row["state"],
            "checks": row["checks"],
            "frees": row["frees"],
            "median_gap_s": round(median, 1) if median else None,
            "last_free_s_ago": round(now - row["last_free"], 1) if row["last_free"] else None,
            "verdict": verdict,
            "_style": style,
        })

    if as_json:
        for item in data:
            item.pop("_style", None)
        print(json.dumps(data, indent=2))
        return

    table = Table(title=f"churn report — {len(data)} code(s)", box=None)
    for column in ("code", "state", "checks", "frees", "median gap", "last free", "verdict"):
        table.add_column(column)
    for item in data:
        table.add_row(item["code"], item["state"], str(item["checks"]), str(item["frees"]),
                      f"{item['median_gap_s']}s" if item["median_gap_s"] else "—",
                      f"{item['last_free_s_ago']}s ago" if item["last_free_s_ago"] else "—",
                      f"[{item['_style']}]{item['verdict']}[/]")
    Console().print(table)


# ---------------------------------------------------------------------- ui

def _run_ui(engine: Engine) -> None:
    from rich.console import Group
    from rich.layout import Layout
    from rich.live import Live
    from rich.panel import Panel
    from rich.table import Table
    from rich.text import Text

    styles = {
        "free": "bold green", "claiming": "bold yellow", "OWNED": "bold green",
        "owned": "green", "owned (dry)": "green", "taken": "dim",
        "unknown": "dim", "lost race": "red",
    }

    def header() -> Panel:
        s = engine.stats
        up = int(time.time() - s.started)
        text = (
            f"uptime {up // 3600:02d}:{up % 3600 // 60:02d}:{up % 60:02d}   "
            f"req {s.requests}   429 {s.limited}   err {s.errors}   claims {s.claims}   "
            f"avg {s.avg_latency:.0f}ms   accounts {len(engine._live())}/"
            f"{len(engine.clients)}   proxies {len(engine.pool)}"
        )
        style = "black on yellow" if engine.paused else "black on cyan"
        return Panel(Text(text, style=style), height=3)

    def codes_panel() -> Panel:
        table = Table(expand=True, box=None, pad_edge=False)
        for name, kw in (("code", {"style": "bold"}), ("state", {}),
                         ("checks", {"justify": "right"}), ("frees", {"justify": "right"}),
                         ("age", {"justify": "right"})):
            table.add_column(name, **kw)
        now = time.time()
        for row in engine.cache.all_codes():
            age = "—" if not row.last_checked else f"{now - row.last_checked:.1f}s"
            state = engine.state.get(row.code, row.state)
            table.add_row(row.code, Text(state, style=styles.get(state, "white")),
                          str(row.checks), str(row.frees), age)
        return Panel(table, title="codes", height=min(len(engine.cfg.codes) + 4, 16))

    def accounts_panel() -> Panel:
        table = Table(expand=True, box=None, pad_edge=False)
        for name in ("account", "alive", "req", "429", "proxy"):
            table.add_column(name)
        for client in engine.clients:
            slot = engine.stats.slot(client.token)
            flag = "dead" if client.dead else ("unverified" if not client.verified else "yes")
            table.add_row(engine.cfg.masked(client.token),
                          Text(flag, style="green" if flag == "yes" else "red"),
                          str(slot["req"]), str(slot["rl"]), (client.proxy or "—")[:30])
        return Panel(table, title="accounts", height=min(len(engine.clients) + 4, 12))

    def proxies_panel() -> Panel:
        table = Table(expand=True, box=None, pad_edge=False)
        for name in ("endpoint", "up", "score", "fails"):
            table.add_column(name)
        for row in engine.cache.proxies()[:12]:
            table.add_row(row["endpoint"][:36],
                          Text("up", style="green") if row["alive"] else Text("down", style="red"),
                          f"{row['score']:.2f}", str(row["fails"]))
        return Panel(table, title="proxy pool", height=12)

    def log_panel() -> Panel:
        return Panel(Text("\n".join(list(engine.log_lines)[-16:]), style="grey70"), title="log")

    def render() -> Layout:
        layout = Layout()
        layout.split_column(Layout(header(), size=3), Layout(name="body"),
                            Layout(log_panel(), size=18))
        layout["body"].split_row(Layout(codes_panel()),
                                 Layout(Group(accounts_panel(), proxies_panel())))
        return layout

    async def drive() -> None:
        with Live(render(), screen=True, refresh_per_second=8) as live:
            task = asyncio.create_task(engine.run())
            while not task.done():
                live.update(render())
                await asyncio.sleep(0.12)
            await task

    asyncio.run(drive())


# -------------------------------------------------------------------- main

async def main_async(cfg: Config) -> None:
    cache = Cache(cfg.cache_path, cfg.codes, cfg.proxies)
    pool = ProxyPool(cache, cfg)
    engine = Engine(cfg, cache, pool)

    server = None
    if cfg.serve:
        port = int(os.environ.get("PORT", "8080"))
        server = HealthServer(engine, port)
        server.start()
        engine.log(f"health server on :{port}")

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, engine.stop.set)
        except NotImplementedError:
            pass

    try:
        if cfg.headless:
            await engine.run()
        else:
            from rich.console import Console  # noqa: F401
            await _run_ui_async(engine)
    finally:
        if server:
            server.stop()
        cache.close()


async def _run_ui_async(engine: Engine) -> None:
    await engine.run()


def main() -> None:
    parser = argparse.ArgumentParser(prog="snipy")
    parser.add_argument("config", nargs="?", default="config.json")
    parser.add_argument("--headless", action="store_true")
    parser.add_argument("--serve", action="store_true")
    parser.add_argument("--report", action="store_true")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    path = Path(args.config)
    cfg = Config.load(path if path.exists() else None)

    if args.report:
        report(cfg.cache_path, args.json)
        return

    cfg.headless = cfg.headless or args.headless
    cfg.serve = cfg.serve or args.serve

    handler = RotatingFileHandler(cfg.log_file, maxBytes=2_000_000, backupCount=3)
    logging.basicConfig(level=logging.INFO, handlers=[handler],
                        format="%(asctime)s %(levelname)s %(message)s")

    try:
        asyncio.run(main_async(cfg))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()