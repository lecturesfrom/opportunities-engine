"""Substack hiring-roundup scraper.

Handles both text-rich posts (company + URL parsed from body markdown/HTML)
and image-rich posts (LLM vision extracts structured lists from screenshots).

Cache: ~/.opportunities-engine/substack-cache.json  (or
       ~/Library/Application Support/opportunities-engine/substack-cache.json on macOS)
keyed by post_url and image_hash so repeated runs don't re-process the same
content or burn vision API tokens twice.
"""
from __future__ import annotations

import base64
import hashlib
import json
import logging
import re
import time
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx

from opportunities_engine.config import SUBSTACK_HIRING_FEEDS, get_default_db_path, settings
from opportunities_engine.dedup.canonical import normalize_company, normalize_title

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_MAX_IMAGES_PER_POST = 10
_VISION_MODEL = "claude-haiku-4-5"
_MAX_IMAGE_BYTES = 5 * 1024 * 1024  # 5 MB

# ATS domains that indicate a URL is a real job posting.
_ATS_DOMAINS = {
    "greenhouse.io",
    "boards.greenhouse.io",
    "lever.co",
    "jobs.lever.co",
    "ashbyhq.com",
    "wellfound.com",
    "angellist.com",
}

# Regex patterns for text-body parsing.
# Pattern 1: Company — Role — https://...  (em-dash or regular dash)
_EM_DASH_PATTERN = re.compile(
    r"(?P<company>[^—\-]+?)\s*[—]\s*(?P<title>[^—]+?)\s*[—]\s*(?P<url>https?://\S+)",
    re.UNICODE,
)
# Pattern 2: Markdown link [Company — Role](https://...)
_MD_LINK_PATTERN = re.compile(
    r"\[(?P<company>[^—\]]+?)\s*[—]\s*(?P<title>[^\]]+?)\]\((?P<url>https?://[^)]+)\)",
    re.UNICODE,
)
# Pattern 3: Any line with a trailing ATS URL
_PLAIN_ATS_URL_PATTERN = re.compile(
    r"(?P<url>https?://(?:(?:boards\.greenhouse\.io|jobs\.lever\.co|ashbyhq\.com|wellfound\.com|lever\.co|greenhouse\.io|angellist\.com)/[^\s\"'<>]+))",
    re.IGNORECASE,
)

# Regex to extract image src from body HTML.
_IMG_SRC_PATTERN = re.compile(r'<img[^>]+src="([^"]+)"', re.IGNORECASE)


# ---------------------------------------------------------------------------
# Data class
# ---------------------------------------------------------------------------


@dataclass
class SubstackPost:
    post_url: str
    title: str
    published: str
    html_body: str
    image_urls: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# SubstackSource
# ---------------------------------------------------------------------------


class SubstackSource:
    """Scrape a list of Substack RSS feeds and extract job postings."""

    def __init__(
        self,
        feeds: list[str] | None = None,
        *,
        http: httpx.Client | None = None,
        anthropic_client: object | None = None,
        cache_path: Path | None = None,
        max_images_per_post: int = _MAX_IMAGES_PER_POST,
    ) -> None:
        self.feeds = list(feeds if feeds is not None else SUBSTACK_HIRING_FEEDS)
        self.http = http or httpx.Client(
            timeout=30,
            follow_redirects=True,
            headers={"User-Agent": "opportunities-engine/0.1"},
        )
        self._anthropic = anthropic_client
        self._cache_path = cache_path or (get_default_db_path().parent / "substack-cache.json")
        self._cache = self._load_cache()
        self._max_images = max_images_per_post

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def fetch(self) -> list[dict]:
        """Top-level entry. For each feed, pull new posts, parse text + images, return normalized dicts."""
        all_jobs: list[dict] = []
        for feed_url in self.feeds:
            try:
                posts = self._fetch_feed(feed_url)
            except Exception as exc:
                logger.warning("Substack feed %s failed: %s", feed_url, exc)
                continue
            for post in posts:
                if post.post_url in self._cache.get("posts", {}):
                    logger.debug("Substack post cached, skipping: %s", post.post_url)
                    continue
                jobs = self._process_post(post)
                self._cache.setdefault("posts", {})[post.post_url] = {
                    "jobs_extracted": len(jobs),
                    "processed_at": datetime.now(timezone.utc).isoformat(),
                }
                all_jobs.extend(jobs)
        self._save_cache()
        return self._dedupe(all_jobs)

    # ------------------------------------------------------------------
    # Feed fetching
    # ------------------------------------------------------------------

    def _fetch_feed(self, feed_url: str) -> list[SubstackPost]:
        """GET {feed_url}/feed (Substack's standard RSS path). Parse XML. Return list[SubstackPost]."""
        # Normalize: strip trailing slash then append /feed
        base = feed_url.rstrip("/")
        rss_url = f"{base}/feed"
        resp = self.http.get(rss_url)
        resp.raise_for_status()
        xml_text = resp.text

        try:
            root = ET.fromstring(xml_text)
        except ET.ParseError as exc:
            logger.warning("Substack: XML parse error for %s: %s", rss_url, exc)
            return []

        # Handle namespaces — content:encoded lives in the content namespace.
        ns = {"content": "http://purl.org/rss/1.0/modules/content/"}

        posts: list[SubstackPost] = []
        channel = root.find("channel")
        if channel is None:
            # Some feeds use root directly as channel
            items = root.findall("item")
        else:
            items = channel.findall("item")

        for item in items:
            link_el = item.find("link")
            title_el = item.find("title")
            pub_date_el = item.find("pubDate")
            # content:encoded has the full HTML body
            body_el = item.find("content:encoded", ns)
            if body_el is None:
                # Fallback: <description>
                body_el = item.find("description")

            post_url = (link_el.text or "").strip() if link_el is not None else ""
            title = (title_el.text or "").strip() if title_el is not None else ""
            published = (pub_date_el.text or "").strip() if pub_date_el is not None else ""
            html_body = (body_el.text or "").strip() if body_el is not None else ""

            if not post_url:
                continue

            # Extract image URLs from body HTML.
            raw_img_urls = _IMG_SRC_PATTERN.findall(html_body)
            # Cap to max_images
            image_urls = raw_img_urls[: self._max_images]

            posts.append(
                SubstackPost(
                    post_url=post_url,
                    title=title,
                    published=published,
                    html_body=html_body,
                    image_urls=image_urls,
                )
            )

        return posts

    # ------------------------------------------------------------------
    # Post processing
    # ------------------------------------------------------------------

    def _process_post(self, post: SubstackPost) -> list[dict]:
        """Parse text + images for a single post. Return normalized dicts."""
        jobs: list[dict] = []

        # Text path
        text_jobs = self._parse_text_body(post)
        jobs.extend(text_jobs)

        # Image path
        image_jobs = self._parse_images(post)
        jobs.extend(image_jobs)

        return jobs

    # ------------------------------------------------------------------
    # Text parsing
    # ------------------------------------------------------------------

    def _parse_text_body(self, post: SubstackPost) -> list[dict]:
        """Regex-based text extraction from post HTML body.

        Three patterns tried per line/block (first match wins):
          1. Company — Role — https://...  (em-dash separator)
          2. Markdown link: [Company — Role](https://...)
          3. Plain line with trailing ATS URL
        Dedupes within post by URL.
        """
        body = post.html_body
        # Strip HTML tags for cleaner text, but keep line structure.
        # Replace block-level tags with newlines.
        text = re.sub(r"<br\s*/?>", "\n", body, flags=re.IGNORECASE)
        text = re.sub(r"<p[^>]*>", "\n", text, flags=re.IGNORECASE)
        text = re.sub(r"</p>", "\n", text, flags=re.IGNORECASE)
        text = re.sub(r"<li[^>]*>", "\n", text, flags=re.IGNORECASE)
        text = re.sub(r"<[^>]+>", "", text)  # strip remaining tags

        seen_urls: set[str] = set()
        results: list[dict] = []

        for line in text.splitlines():
            line = line.strip()
            if not line:
                continue

            # Pattern 1: em-dash
            m = _EM_DASH_PATTERN.search(line)
            if m:
                company = m.group("company").strip()
                title = m.group("title").strip()
                url = m.group("url").strip().rstrip(".,;)")
                if url and url not in seen_urls:
                    seen_urls.add(url)
                    results.append(
                        {
                            "company": company,
                            "title": title,
                            "url": url,
                            "location": None,
                            "raw_text": line,
                            "_via": "text",
                        }
                    )
                continue

            # Pattern 2: markdown link
            m = _MD_LINK_PATTERN.search(line)
            if m:
                company = m.group("company").strip()
                title = m.group("title").strip()
                url = m.group("url").strip()
                if url and url not in seen_urls:
                    seen_urls.add(url)
                    results.append(
                        {
                            "company": company,
                            "title": title,
                            "url": url,
                            "location": None,
                            "raw_text": line,
                            "_via": "text",
                        }
                    )
                continue

            # Pattern 3: plain line with ATS URL
            m = _PLAIN_ATS_URL_PATTERN.search(line)
            if m:
                url = m.group("url").strip().rstrip(".,;)")
                if url and url not in seen_urls:
                    seen_urls.add(url)
                    # Company/title heuristic: everything before the URL
                    prefix = line[: m.start()].strip()
                    # Company is the first non-empty chunk; title is the rest
                    parts = [p.strip() for p in re.split(r"[—\-–|]", prefix) if p.strip()]
                    company = parts[0] if parts else ""
                    title_text = parts[1] if len(parts) > 1 else None
                    if not company:
                        continue
                    results.append(
                        {
                            "company": company,
                            "title": title_text,
                            "url": url,
                            "location": None,
                            "raw_text": line,
                            "_via": "text",
                        }
                    )

        # Normalize and filter
        normalized: list[dict] = []
        for entry in results:
            n = self._normalize(entry, post.post_url)
            if n is not None:
                normalized.append(n)

        return normalized

    # ------------------------------------------------------------------
    # Image parsing
    # ------------------------------------------------------------------

    def _parse_images(self, post: SubstackPost) -> list[dict]:
        """Download images (up to max_images) and call vision API for each."""
        results: list[dict] = []
        for img_url in post.image_urls:
            # Compute hash of the URL string (before downloading)
            image_hash = hashlib.sha256(img_url.encode()).hexdigest()[:16]

            # Check cache
            cached = self._cache.get("images", {}).get(image_hash)
            if cached is not None:
                logger.debug("Substack: image cache hit for hash %s", image_hash)
                for entry in cached.get("extracted", []):
                    n = self._normalize(dict(entry, _via="image"), post.post_url)
                    if n is not None:
                        results.append(n)
                continue

            # Download image
            image_bytes = self._download_image(img_url)
            if image_bytes is None:
                continue

            # Call vision
            extracted = self._call_vision(image_bytes, image_hash, post.post_url)

            # Cache the raw extraction result
            self._cache.setdefault("images", {})[image_hash] = {
                "extracted": extracted,
                "processed_at": datetime.now(timezone.utc).isoformat(),
            }

            for entry in extracted:
                n = self._normalize(dict(entry, _via="image"), post.post_url)
                if n is not None:
                    results.append(n)

        return results

    def _download_image(self, url: str) -> bytes | None:
        """Download image bytes, enforcing size and content-type guards.

        Returns None if the image should be skipped.
        """
        try:
            # HEAD first to check size and content-type
            head = self.http.head(url)
            content_type = head.headers.get("content-type", "")
            if not content_type.startswith("image/"):
                logger.debug("Substack: skipping non-image URL (content-type=%s): %s", content_type, url)
                return None
            content_length = int(head.headers.get("content-length", 0) or 0)
            if content_length > _MAX_IMAGE_BYTES:
                logger.debug("Substack: skipping oversized image (%d bytes): %s", content_length, url)
                return None
        except Exception as exc:
            logger.debug("Substack: HEAD request failed for %s: %s", url, exc)
            # Fall through to GET — some servers don't support HEAD

        try:
            resp = self.http.get(url)
            resp.raise_for_status()
            content_type = resp.headers.get("content-type", "")
            if not content_type.startswith("image/"):
                logger.debug("Substack: skipping non-image content-type=%s: %s", content_type, url)
                return None
            if len(resp.content) > _MAX_IMAGE_BYTES:
                logger.debug("Substack: skipping oversized image (%d bytes): %s", len(resp.content), url)
                return None
            return resp.content
        except Exception as exc:
            logger.debug("Substack: GET failed for image %s: %s", url, exc)
            return None

    # ------------------------------------------------------------------
    # Vision call
    # ------------------------------------------------------------------

    def _call_vision(self, image_bytes: bytes, image_hash: str, post_url: str) -> list[dict]:
        """Call Claude haiku vision with the image bytes. Return parsed list of job dicts."""
        if not image_bytes:
            return []

        # Resolve anthropic client
        client = self._anthropic
        if client is None:
            api_key = settings.anthropic_api_key
            if not api_key:
                logger.warning("Substack: anthropic_api_key not configured; skipping vision for %s", image_hash)
                return []
            try:
                import anthropic  # noqa: PLC0415
                client = anthropic.Anthropic(api_key=api_key)
            except Exception as exc:
                logger.warning("Substack: failed to create Anthropic client: %s", exc)
                return []

        # Encode image as base64
        b64_image = base64.standard_b64encode(image_bytes).decode("ascii")
        # Determine media type from first bytes (simple heuristic)
        media_type = _sniff_media_type(image_bytes)

        prompt = (
            "This image is from a newsletter that posts roundups of companies hiring.\n"
            "Extract every distinct job posting you can see. Return ONLY a JSON array,\n"
            "no prose, of objects with these keys (use null for missing values):\n"
            '[{"company": str, "title": str|null, "url": str|null, "location": str|null}].\n'
            "If the image is not a hiring list (a banner, author photo, unrelated meme, etc.),\n"
            "return [].\n"
            "IMPORTANT: return ONLY the JSON array — no code fences, no explanations."
        )

        try:
            message = client.messages.create(  # type: ignore[union-attr]
                model=_VISION_MODEL,
                max_tokens=2000,
                messages=[
                    {
                        "role": "user",
                        "content": [
                            {
                                "type": "image",
                                "source": {
                                    "type": "base64",
                                    "media_type": media_type,
                                    "data": b64_image,
                                },
                            },
                            {
                                "type": "text",
                                "text": prompt,
                            },
                        ],
                    }
                ],
            )
            raw_text = message.content[0].text.strip()
        except Exception as exc:
            logger.warning("Substack: vision API call failed for image_hash=%s post=%s: %s", image_hash, post_url, exc)
            return []

        # Parse JSON response
        try:
            result = json.loads(raw_text)
            if not isinstance(result, list):
                logger.warning("Substack: vision returned non-list for image_hash=%s; ignoring", image_hash)
                return []
            return result
        except json.JSONDecodeError as exc:
            logger.warning(
                "Substack: vision returned malformed JSON for image_hash=%s post=%s: %s | raw=%r",
                image_hash,
                post_url,
                exc,
                raw_text[:200],
            )
            return []

    # ------------------------------------------------------------------
    # Normalization
    # ------------------------------------------------------------------

    def _normalize(self, entry: dict, post_url: str) -> dict | None:
        """Build canonical job dict from a parsed entry.

        Returns None if the entry cannot be normalized (missing company).
        """
        company = (entry.get("company") or "").strip()
        if not company:
            return None

        title = (entry.get("title") or "").strip() or "(untitled)"
        url = (entry.get("url") or "").strip() or post_url
        location = entry.get("location")
        via = entry.get("_via", "text")

        # Build a short source_id from post_url hash + normalized company + normalized title
        post_url_hash = hashlib.sha256(post_url.encode()).hexdigest()[:12]
        norm_co = normalize_company(company)
        norm_ti = normalize_title(title)
        source_id = f"{post_url_hash}:{norm_co}:{norm_ti}"[:120]

        # Infer is_remote
        loc_lower = (location or "").lower()
        title_lower = title.lower()
        if "remote" in loc_lower or "remote" in title_lower:
            is_remote: bool | None = True
        else:
            is_remote = None

        return {
            "source": "substack",
            "source_id": source_id,
            "url": url,
            "title": title,
            "company": company,
            "location": location,
            "is_remote": is_remote,
            "description": entry.get("raw_text"),
            "metadata": {
                "post_url": post_url,
                "via": via,
            },
        }

    # ------------------------------------------------------------------
    # Dedup
    # ------------------------------------------------------------------

    def _dedupe(self, jobs: list[dict]) -> list[dict]:
        """Dedupe by (normalize_company, normalize_title) key."""
        seen: set[tuple[str, str]] = set()
        result: list[dict] = []
        for job in jobs:
            co_key = normalize_company(job.get("company") or "")
            ti_key = normalize_title(job.get("title") or "")
            pair = (co_key, ti_key)
            if pair in seen:
                continue
            seen.add(pair)
            result.append(job)
        return result

    # ------------------------------------------------------------------
    # Cache
    # ------------------------------------------------------------------

    def _load_cache(self) -> dict:
        """Load JSON cache from disk. Returns empty dict on missing or corrupt file."""
        if not self._cache_path.exists():
            return {}
        try:
            return json.loads(self._cache_path.read_text(encoding="utf-8"))
        except Exception as exc:
            logger.warning("Substack: cache corrupt or unreadable at %s, starting fresh: %s", self._cache_path, exc)
            return {}

    def _save_cache(self) -> None:
        """Persist cache to disk, creating parent directories as needed."""
        try:
            self._cache_path.parent.mkdir(parents=True, exist_ok=True)
            self._cache_path.write_text(
                json.dumps(self._cache, indent=2, ensure_ascii=False),
                encoding="utf-8",
            )
        except Exception as exc:
            logger.warning("Substack: failed to save cache to %s: %s", self._cache_path, exc)


# ---------------------------------------------------------------------------
# Module helpers
# ---------------------------------------------------------------------------


def _sniff_media_type(data: bytes) -> str:
    """Return a best-guess MIME type from magic bytes."""
    if data[:8] == b"\x89PNG\r\n\x1a\n":
        return "image/png"
    if data[:3] == b"\xff\xd8\xff":
        return "image/jpeg"
    if data[:6] in (b"GIF87a", b"GIF89a"):
        return "image/gif"
    if data[:4] == b"RIFF" and data[8:12] == b"WEBP":
        return "image/webp"
    # Default to jpeg — most newsletter images are JPEG
    return "image/jpeg"
