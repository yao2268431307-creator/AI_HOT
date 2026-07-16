from __future__ import annotations

import hashlib
import html
import re
import unicodedata
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit


TRACKING_KEYS = {"fbclid", "gclid", "mc_cid", "mc_eid", "ref", "source"}


def normalize_url(url: str) -> str:
    parts = urlsplit(url.strip())
    scheme = (parts.scheme or "https").lower()
    host = parts.netloc.lower()
    if host.startswith("www."):
        host = host[4:]
    query = [(k, v) for k, v in parse_qsl(parts.query, keep_blank_values=True) if not k.lower().startswith("utm_") and k.lower() not in TRACKING_KEYS]
    path = re.sub(r"/{2,}", "/", parts.path or "/")
    if path != "/":
        path = path.rstrip("/")
    return urlunsplit((scheme, host, path, urlencode(sorted(query)), ""))


def sanitize_external_text(text: str) -> str:
    """Normalize untrusted display text without interpreting markup or bidi controls."""
    value = unicodedata.normalize("NFKC", html.unescape(text))
    value = re.sub(r"<[^>]+>", " ", value)
    value = "".join(character for character in value if unicodedata.category(character) != "Cf")
    return re.sub(r"\s+", " ", value).strip()


def canonical_text(text: str) -> str:
    value = sanitize_external_text(text)
    value = re.sub(r"https?://\S+", " URL ", value)
    value = re.sub(r"\s+", " ", value).strip().lower()
    return value


def content_fingerprint(title: str | None, text: str, url: str) -> str:
    material = f"{canonical_text(title or '')}\n{canonical_text(text)}\n{normalize_url(url)}"
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


def language_hint(text: str) -> str:
    if not text:
        return "other"
    cjk = len(re.findall(r"[\u4e00-\u9fff]", text))
    latin = len(re.findall(r"[A-Za-z]", text))
    if cjk >= max(2, latin * 0.15):
        return "zh"
    if latin >= 3:
        return "en"
    return "other"
