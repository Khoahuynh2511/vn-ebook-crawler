import asyncio
import random
import re
from pathlib import Path
from typing import Optional
from urllib.parse import urljoin

import aiofiles
from bs4 import BeautifulSoup
import httpx

GDRIVE_ID_RE = re.compile(r"(?:/d/|id=)([a-zA-Z0-9_-]{20,})")


def extract_gdrive_id(url: str) -> Optional[str]:
    """Extract Google Drive file ID from common sharing URL formats."""
    if "drive.google.com" not in url and "drive.usercontent.google.com" not in url:
        return None
    match = GDRIVE_ID_RE.search(url)
    return match.group(1) if match else None


def validate_file_header(file_path: Path, expected_ext: str) -> bool:
    """Validate file magic bytes and minimum size; delete invalid files."""
    file_path = Path(file_path)
    if not file_path.exists():
        return False
    size = file_path.stat().st_size
    if size <= 1024:
        file_path.unlink(missing_ok=True)
        return False

    ext = expected_ext.lower().lstrip(".")
    with open(file_path, "rb") as fh:
        header = fh.read(16)

    if ext == "pdf":
        # Some PDFs have leading whitespace/BOM before %PDF
        valid = b"%PDF" in header
    elif ext == "epub":
        valid = header.startswith(b"PK\x03\x04")
    else:
        valid = True

    if not valid:
        file_path.unlink(missing_ok=True)
    return valid


def _parse_retry_after(resp: httpx.Response, default_wait: float) -> float:
    """Parse HTTP Retry-After seconds safely (capped at 30s) or fall back to jittered backoff."""
    raw = resp.headers.get("Retry-After")
    if raw:
        try:
            sec = float(raw.strip())
            if sec > 0:
                return min(sec, 30.0) + random.uniform(0.1, 0.5)
        except ValueError:
            pass
    return default_wait + random.uniform(0.2, 0.8)


class FileDownloader:
    """Resolves cloud download links, streams files, and validates magic bytes."""

    async def resolve_download_url(
        self, client: httpx.AsyncClient, url: str
    ) -> Optional[str]:
        """Resolve Google Drive, MediaFire, or direct ebook URLs."""
        if not url:
            return None
        url = url.strip()

        # 1. Google Drive
        gdrive_id = extract_gdrive_id(url)
        if gdrive_id:
            direct_url = f"https://drive.google.com/uc?export=download&id={gdrive_id}"
            try:
                resp = await client.get(direct_url, timeout=30.0)
                ctype = resp.headers.get("content-type", "").lower()
                if "text/html" in ctype:
                    html_text = resp.text
                    # Look for confirmation token or usercontent download form
                    confirm_match = re.search(r"confirm=([0-9A-Za-z_-]+)", html_text)
                    uuid_match = re.search(r'name="uuid"\s+value="([^"]+)"', html_text)
                    if uuid_match:
                        uuid_val = uuid_match.group(1)
                        return (
                            f"https://drive.usercontent.google.com/download"
                            f"?id={gdrive_id}&export=download&confirm=t&uuid={uuid_val}"
                        )
                    if confirm_match:
                        token = confirm_match.group(1)
                        return (
                            f"https://drive.google.com/uc?export=download"
                            f"&confirm={token}&id={gdrive_id}"
                        )
                    return (
                        f"https://drive.usercontent.google.com/download"
                        f"?id={gdrive_id}&export=download&confirm=t"
                    )
                return str(resp.url)
            except Exception:
                return (
                    f"https://drive.usercontent.google.com/download"
                    f"?id={gdrive_id}&export=download&confirm=t"
                )

        # 2. MediaFire
        if "mediafire.com/file/" in url or "mediafire.com/?" in url:
            try:
                resp = await client.get(url, timeout=30.0)
                if resp.status_code == 200:
                    soup = BeautifulSoup(resp.text, "lxml")
                    btn = soup.select_one("a#downloadButton[href]")
                    if btn and btn.get("href"):
                        return urljoin(str(resp.url), btn["href"])
                    # Fallback regex for MediaFire download link
                    mf_match = re.search(
                        r'https?://download\d+\.mediafire\.com/[^\s"\'<>]+',
                        resp.text,
                    )
                    if mf_match:
                        return mf_match.group(0)
            except Exception:
                return None
            return None

        # 3. Direct URL or site download route
        return url

    async def download_book_file(
        self,
        client: httpx.AsyncClient,
        url: str,
        dest_path: Path,
        expected_ext: str,
        referer: Optional[str] = None,
        retries: int = 3,
    ) -> bool:
        """Download file to .part temp file with Retry-After/backoff, validate magic bytes, and rename."""
        dest_path = Path(dest_path)
        if dest_path.exists() and validate_file_header(dest_path, expected_ext):
            return True

        resolved_url = await self.resolve_download_url(client, url)
        if not resolved_url:
            return False

        dest_path.parent.mkdir(parents=True, exist_ok=True)
        part_path = dest_path.with_suffix(f".{expected_ext.lstrip('.')}.part")

        req_headers = {}
        if referer:
            req_headers["Referer"] = referer

        for attempt in range(retries + 1):
            try:
                async with client.stream(
                    "GET", resolved_url, headers=req_headers, timeout=90.0
                ) as resp:
                    if resp.status_code in (429, 500, 502, 503, 504):
                        if attempt < retries:
                            wait = _parse_retry_after(resp, 1.5 * (2**attempt))
                            await asyncio.sleep(wait)
                            continue
                        return False
                    if resp.status_code >= 400:
                        return False

                    async with aiofiles.open(part_path, "wb") as out_f:
                        async for chunk in resp.aiter_bytes(chunk_size=65536):
                            if chunk:
                                await out_f.write(chunk)

                if not validate_file_header(part_path, expected_ext):
                    return False

                if dest_path.exists():
                    dest_path.unlink()
                part_path.rename(dest_path)
                return True
            except (httpx.HTTPError, OSError):
                part_path.unlink(missing_ok=True)
                if attempt < retries:
                    await asyncio.sleep(1.5 * (2**attempt) + random.uniform(0.2, 0.8))
                    continue
                return False
            except Exception:
                part_path.unlink(missing_ok=True)
                return False
        return False
