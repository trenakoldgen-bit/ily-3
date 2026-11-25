from __future__ import annotations

import random
import sqlite3
import time
from dataclasses import dataclass

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

    # ---------- scheduling ----------

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

    # ---------- proxies ----------

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

    # ---------- report ----------

    def churn(self) -> list[sqlite3.Row]:
        return self.db.execute(
            "SELECT code, state, checks, frees, last_free, last_checked FROM codes "
            "ORDER BY frees DESC, last_free DESC").fetchall()

    def free_timestamps(self, code: str) -> list[float]:
        return [r["ts"] for r in self.db.execute(
            "SELECT ts FROM free_events WHERE code=? ORDER BY ts", (code,))]

    def close(self) -> None:
        self.db.close()