from __future__ import annotations

import asyncio
import time

from rich.console import Group
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from .engine import Engine

STYLES = {
    "free": "bold green", "claiming": "bold yellow", "OWNED": "bold green",
    "owned": "green", "owned (dry)": "green", "taken": "dim",
    "unknown": "dim", "lost race": "red",
}


class Dashboard:
    def __init__(self, engine: Engine) -> None:
        self.engine = engine
        self.cfg = engine.cfg

    def _header(self) -> Panel:
        s = self.engine.stats
        up = int(time.time() - s.started)
        text = (
            f"uptime {up // 3600:02d}:{up % 3600 // 60:02d}:{up % 60:02d}   "
            f"req {s.requests}   429 {s.limited}   err {s.errors}   claims {s.claims}   "
            f"avg {s.avg_latency:.0f}ms   accounts {len(self.engine._live())}/"
            f"{len(self.engine.clients)}   proxies {len(self.engine.pool)}"
        )
        style = "black on yellow" if self.engine.paused else "black on cyan"
        return Panel(Text(text, style=style), height=3)

    def _codes(self) -> Panel:
        table = Table(expand=True, box=None, pad_edge=False)
        for name, kw in (("code", {"style": "bold"}), ("state", {}),
                         ("checks", {"justify": "right"}), ("frees", {"justify": "right"}),
                         ("age", {"justify": "right"})):
            table.add_column(name, **kw)
        now = time.time()
        for row in self.engine.cache.all_codes():
            age = "—" if not row.last_checked else f"{now - row.last_checked:.1f}s"
            state = self.engine.state.get(row.code, row.state)
            table.add_row(row.code, Text(state, style=STYLES.get(state, "white")),
                          str(row.checks), str(row.frees), age)
        return Panel(table, title="codes", height=min(len(self.cfg.codes) + 4, 16))

    def _accounts(self) -> Panel:
        table = Table(expand=True, box=None, pad_edge=False)
        for name in ("account", "alive", "req", "429", "proxy"):
            table.add_column(name)
        for client in self.engine.clients:
            slot = self.engine.stats.slot(client.token)
            flag = "dead" if client.dead else ("unverified" if not client.verified else "yes")
            table.add_row(self.cfg.masked(client.token),
                          Text(flag, style="green" if flag == "yes" else "red"),
                          str(slot["req"]), str(slot["rl"]), (client.proxy or "—")[:30])
        return Panel(table, title="accounts", height=min(len(self.engine.clients) + 4, 12))

    def _proxies(self) -> Panel:
        table = Table(expand=True, box=None, pad_edge=False)
        for name in ("endpoint", "up", "score", "fails"):
            table.add_column(name)
        for row in self.engine.cache.proxies()[:12]:
            table.add_row(row["endpoint"][:36],
                          Text("up", style="green") if row["alive"] else Text("down", style="red"),
                          f"{row['score']:.2f}", str(row["fails"]))
        return Panel(table, title="proxy pool", height=12)

    def _log(self) -> Panel:
        return Panel(Text("\n".join(list(self.engine.log_lines)[-16:]), style="grey70"),
                     title="log")

    def render(self) -> Layout:
        layout = Layout()
        layout.split_column(
            Layout(self._header(), size=3),
            Layout(name="body"),
            Layout(self._log(), size=18),
        )
        layout["body"].split_row(Layout(self._codes()), Layout(Group(self._accounts(),
                                                                    self._proxies())))
        return layout


async def run_ui(engine: Engine) -> None:
    dashboard = Dashboard(engine)
    with Live(dashboard.render(), screen=True, refresh_per_second=8) as live:
        task = asyncio.create_task(engine.run())
        while not task.done():
            live.update(dashboard.render())
            await asyncio.sleep(0.12)
        await task