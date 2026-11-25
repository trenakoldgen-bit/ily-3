from __future__ import annotations

import asyncio


class ProxyPool:
    def __init__(self, cache, cfg) -> None:
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