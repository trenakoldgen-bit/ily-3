from __future__ import annotations

import argparse
import asyncio
import logging
import os
import signal
from logging.handlers import RotatingFileHandler
from pathlib import Path

from .cache import Cache
from .config import Config
from .engine import Engine
from .proxy import ProxyPool


async def main_async(cfg: Config) -> None:
    cache = Cache(cfg.cache_path, cfg.codes, cfg.proxies)
    pool = ProxyPool(cache, cfg)
    engine = Engine(cfg, cache, pool)

    server = None
    if cfg.serve:
        from .health import HealthServer
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
            from .ui import run_ui
            await run_ui(engine)
    finally:
        if server:
            server.stop()
        cache.close()


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
        from .report import run
        run(cfg.cache_path, args.json)
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