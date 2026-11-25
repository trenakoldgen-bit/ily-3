from __future__ import annotations

import asyncio
import collections
import json
import random
import time
from datetime import date

from curl_cffi.requests import AsyncSession

from .client import AccountClient
from .identity import build
from .status import StatusRotator


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
    def __init__(self, cfg, cache, pool) -> None:
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
            ident = build(token, self.cfg.impersonate, self.cfg.client_build_number)
            client = AccountClient(token, ident, self.pool, self.cfg, self.stats)
            self.clients.append(client)
            self.rotators.append(StatusRotator(client, self.cfg, self.log))

        await self.preflight()
        self.log(f"watching {len(self.cfg.codes)} code(s) with {len(self.clients)} account(s)")

        probes: list[AccountClient] = []

        def factory(proxy: str) -> AccountClient:
            ident = build("probe", self.cfg.impersonate, self.cfg.client_build_number)
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