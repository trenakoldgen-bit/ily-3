from __future__ import annotations

import asyncio
import json
import random

TEXTS = [
    "afk", "in un posto", "fuori casa", "musica", "notte lunga",
    "studio", "pausa", "in giro", "btw", "a dopo", "sotto esame",
    "in viaggio", "weekend", "back later", "sleeping", "coffee",
    "be right back", "chilling", "offline soon",
]
EMOJI = ["🌙", "☕", "🎧", "🌧️", "📚", "🍂", "🔥", "🧊", "🌊", "🕯️"]
PRESENCE = ["online", "online", "online", "online", "idle", "dnd"]
CLEAR_CHANCE = 0.15


class StatusRotator:
    """Fa sembrare l'account un client web usato da una persona."""

    def __init__(self, client, cfg, log) -> None:
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
        body = {"custom_status": custom, "status": presence}
        r = await self.client.request("PATCH", "/users/@me/settings",
                                      json=body, retries=2)
        return r.status == 200

    async def rotate(self) -> None:
        if random.random() < CLEAR_CHANCE:
            custom = None
        else:
            custom = {
                "text": random.choice(TEXTS),
                "emoji_name": random.choice(EMOJI),
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

    def stop(self) -> None:
        self._stop.set()