from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass, field, fields
from pathlib import Path

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
    cookie_dir: str = ""
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
    proxy_health_url: str = "https://discord.com/api/v10/gateway"
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
        raw.setdefault("cookie_dir", "cookies")

        if not raw["guild_id"].isdigit():
            raise SystemExit("guild_id must be a snowflake")
        if not raw["tokens"]:
            raise SystemExit("no tokens (SNIPY_TOKENS or config)")
        if not raw["codes"]:
            raise SystemExit("no codes (SNIPY_CODES or config)")

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