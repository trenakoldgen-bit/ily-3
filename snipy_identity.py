from __future__ import annotations

import base64
import hashlib
import json
from dataclasses import dataclass

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


def build(token: str, impersonate: str, build_number: int) -> Identity:
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