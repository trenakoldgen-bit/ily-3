from __future__ import annotations

import asyncio
import hashlib
import json
import time
from dataclasses import dataclass
from pathlib import Path

from curl_cffi.requests import AsyncSession
from curl_cffi.requests.errors import RequestsError

from .identity import Identity

API = "https://discord.com/api/v10"


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
    def __init__(self, token, ident: Identity, pool, cfg, stats,
                 forced_proxy: str | None = None) -> None:
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