import asyncio
import json
import random
import re
import unicodedata
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup
import httpx

from config import CrawlerConfig
from epub_builder import build_epub_from_chapters
from katana_runner import KatanaDiscovery
from resolvers import FileDownloader, _parse_retry_after, validate_file_header
from storage import CatalogDB

DIRECT_FILE_RE = re.compile(
    r'https?://[^\s"\'<>\\]+\.(?:epub|pdf)(?:\?[^\s"\'<>\\]*)?',
    re.I,
)
CLOUD_LINK_RE = re.compile(
    r'https?://(?:drive\.google\.com|drive\.usercontent\.google\.com|(?:www\.)?mediafire\.com)/[^\s"\'<>\\]+',
    re.I,
)
CAPTCHA_MARKERS = (
    "cf-turnstile",
    "turnstile",
    "g-recaptcha",
    "hcaptcha",
)


def slugify_filename(text: str, max_len: int = 90) -> str:
    """Create a Windows-safe ASCII/slug filename while preserving readability."""
    if not text:
        return "untitled"
    # Replace Vietnamese đ/Đ before stripping non-ASCII characters
    cleaned = text.replace("đ", "d").replace("Đ", "D")
    norm = unicodedata.normalize("NFKD", cleaned)
    ascii_str = norm.encode("ascii", "ignore").decode("ascii")
    slug = re.sub(r"[^a-zA-Z0-9]+", "-", ascii_str).strip("-").lower()
    if not slug:
        slug = "book"
    return slug[:max_len].rstrip("-") or "book"


def is_captcha_or_blocked(status_code: int, html_text: str) -> bool:
    """Detect Cloudflare/Turnstile/CAPTCHA challenge pages without attempting bypass."""
    low = (html_text or "").lower()
    if status_code in (403, 429, 503) and any(
        m in low for m in ("cloudflare", "just a moment", "cf-challenge", "turnstile", "captcha")
    ):
        return True
    if any(
        m in low
        for m in ("<title>just a moment", "cf-challenge", "xác thực capcha", "xác thực captcha")
    ):
        return True
    if len(low) < 40000 and any(marker in low for marker in CAPTCHA_MARKERS):
        return True
    return False


@dataclass
class BookData:
    url: str
    title: str
    author: str = "Khuyết danh"
    category: str = "tong-hop"
    description: str = ""
    epub_url: Optional[str] = None
    pdf_url: Optional[str] = None
    chapters: List[Tuple[str, str]] = field(default_factory=list)
    blocked_reason: Optional[str] = None


@dataclass
class FetchResult:
    url: str
    status_code: int
    text: str
    blocked: bool = False
    error: Optional[str] = None


class BaseEbookSpider:
    """Base asynchronous spider with sitemap discovery, category pagination, and file extraction."""

    name: str = "base"
    base_urls: List[str] = []
    sitemap_paths: List[str] = ["/sitemap.xml", "/sitemap_index.xml", "/wp-sitemap.xml"]
    seed_category_paths: List[str] = []
    crawl_delay: float = 0.0

    def __init__(self, config: CrawlerConfig, db: CatalogDB) -> None:
        self.config = config
        self.db = db
        self.active_base_url: Optional[str] = self.base_urls[0] if self.base_urls else None
        self.status_message: str = "Đang chờ"
        self.discovered_count: int = 0
        self.skipped_count: int = 0
        self.no_files_count: int = 0
        self.processed_count: int = 0
        self.epub_count: int = 0
        self.pdf_count: int = 0
        self.failed_count: int = 0
        self.adaptive_delay: float = 0.0

    def _effective_delay(self) -> float:
        base = max(self.config.request_delay, self.crawl_delay)
        if base <= 0 and self.adaptive_delay <= 0:
            return 0.0
        return base + self.adaptive_delay + random.uniform(0.05, 0.2)

    def _reached_discovery_cap(self) -> bool:
        if self.config.max_books_per_source <= 0:
            return False
        if (self.epub_count + self.pdf_count) >= self.config.max_books_per_source:
            return True
        cap = max(self.config.max_books_per_source * 5, self.config.max_books_per_source + 15)
        effective = max(0, self.discovered_count - self.no_files_count)
        return effective >= cap

    async def fetch_html(
        self,
        client: httpx.AsyncClient,
        url: str,
        retries: int = 2,
        referer: Optional[str] = None,
    ) -> FetchResult:
        """Fetch an HTML/XML document with adaptive polite delay, Retry-After backoff, and CAPTCHA detection."""
        headers = {"Referer": referer} if referer else {}
        last_err: Optional[str] = None
        for attempt in range(retries + 1):
            delay = self._effective_delay()
            if delay > 0:
                await asyncio.sleep(delay)
            try:
                resp = await client.get(
                    url, headers=headers, timeout=self.config.request_timeout
                )
                text = resp.text
                if is_captcha_or_blocked(resp.status_code, text):
                    return FetchResult(
                        url=str(resp.url),
                        status_code=resp.status_code,
                        text=text,
                        blocked=True,
                        error="blocked_by_captcha",
                    )
                if resp.status_code >= 400:
                    last_err = f"HTTP {resp.status_code}"
                    if resp.status_code in (429, 503):
                        self.adaptive_delay = min(self.adaptive_delay + 0.5, 5.0)
                    if attempt < retries and resp.status_code in (429, 500, 502, 503, 504):
                        wait = _parse_retry_after(resp, 1.0 * (2**attempt))
                        await asyncio.sleep(wait)
                        continue
                    return FetchResult(
                        url=str(resp.url),
                        status_code=resp.status_code,
                        text=text,
                        error=last_err,
                    )
                if self.adaptive_delay > 0:
                    self.adaptive_delay = max(0.0, self.adaptive_delay - 0.1)
                return FetchResult(
                    url=str(resp.url), status_code=resp.status_code, text=text
                )
            except Exception as exc:
                last_err = f"{type(exc).__name__}: {exc}"
                if attempt < retries:
                    await asyncio.sleep(1.0 * (2**attempt) + random.uniform(0.1, 0.5))
        return FetchResult(url=url, status_code=0, text="", error=last_err)

    async def resolve_active_mirror(self, client: httpx.AsyncClient) -> Optional[str]:
        """Find the first reachable mirror domain that is not blocked by CAPTCHA."""
        for candidate in self.base_urls:
            res = await self.fetch_html(client, candidate, retries=1)
            if res.blocked:
                self.status_message = "Bị chặn CAPTCHA/Cloudflare"
                continue
            if res.status_code == 200 and res.text:
                parsed = urlparse(res.url)
                self.active_base_url = f"{parsed.scheme}://{parsed.netloc}"
                return self.active_base_url
        if not self.status_message.startswith("Bị chặn"):
            self.status_message = "Không thể kết nối"
        return None

    def is_book_url(self, url: str) -> bool:
        return False

    def is_category_url(self, url: str) -> bool:
        return False

    def is_relevant_sitemap(self, sitemap_url: str) -> bool:
        return True

    def extract_book_links(self, soup: BeautifulSoup, base_url: str) -> List[str]:
        links: List[str] = []
        seen: Set[str] = set()
        for a in soup.find_all("a", href=True):
            abs_url = urljoin(base_url, a["href"]).split("#")[0]
            if abs_url not in seen and self.is_book_url(abs_url):
                seen.add(abs_url)
                links.append(abs_url)
        return links

    def extract_category_links(self, soup: BeautifulSoup, base_url: str) -> List[str]:
        links: List[str] = []
        seen: Set[str] = set()
        for a in soup.find_all("a", href=True):
            abs_url = urljoin(base_url, a["href"]).split("#")[0]
            if abs_url not in seen and self.is_category_url(abs_url):
                seen.add(abs_url)
                links.append(abs_url)
        return links

    def extract_pagination_links(self, soup: BeautifulSoup, base_url: str) -> List[str]:
        links: List[str] = []
        seen: Set[str] = set()
        for a in soup.select("a[rel='next'], .pagination a, .nav-links a, a.next, ul.page-numbers a"):
            href = a.get("href")
            if not href:
                continue
            abs_url = urljoin(base_url, href).split("#")[0]
            if abs_url not in seen and abs_url.startswith("http"):
                seen.add(abs_url)
                links.append(abs_url)
        return links

    def extract_ebook_links(
        self, soup: BeautifulSoup, page_url: str, html_text: str
    ) -> Dict[str, Optional[str]]:
        """Scan anchors, buttons, iframes, and raw HTML/JSON for EPUB & PDF URLs."""
        found: Dict[str, Optional[str]] = {"epub": None, "pdf": None}

        # 1. Raw regex scan for direct .epub / .pdf URLs
        for match in DIRECT_FILE_RE.findall(html_text):
            low = match.lower()
            if ".epub" in low and not found["epub"]:
                found["epub"] = match
            elif ".pdf" in low and not found["pdf"]:
                found["pdf"] = match

        # 2. Scan <a>, <button data-href>, <iframe src>
        for el in soup.find_all(["a", "button", "iframe"]):
            raw_url = el.get("href") or el.get("data-href") or el.get("data-url") or el.get("src") or ""
            if not raw_url or raw_url.startswith("#") or raw_url.startswith("javascript:"):
                continue
            abs_url = urljoin(page_url, raw_url)
            hint = " ".join(
                [
                    abs_url,
                    el.get_text(" ", strip=True),
                    " ".join(el.get("class", [])),
                    el.get("title", ""),
                ]
            ).lower()

            if (".epub" in abs_url.lower() or "epub" in hint) and not found["epub"]:
                if any(k in hint for k in ("tải", "download", ".epub", "btn", "/tai/")):
                    found["epub"] = abs_url
            if (".pdf" in abs_url.lower() or "pdf" in hint) and not found["pdf"]:
                if any(k in hint for k in ("tải", "download", ".pdf", "btn", "/tai/", "reader")):
                    found["pdf"] = abs_url

        # 3. Scan for Google Drive / MediaFire fallback if one format is still missing
        if not found["pdf"] or not found["epub"]:
            for cloud_url in CLOUD_LINK_RE.findall(html_text):
                if not found["pdf"]:
                    found["pdf"] = cloud_url
                elif not found["epub"]:
                    found["epub"] = cloud_url

        return found

    async def _enqueue_book(
        self,
        url: str,
        queue: asyncio.Queue,
        category: Optional[str] = None,
        queued_set: Optional[Set[str]] = None,
    ) -> bool:
        if queued_set is not None and url in queued_set:
            return False
        if self._reached_discovery_cap():
            return False
        if await self.db.is_book_already_done(url):
            if queued_set is not None:
                queued_set.add(url)
            self.skipped_count += 1
            return False
        await self.db.upsert_discovered_book(url, self.name, category=category)
        if queued_set is not None:
            queued_set.add(url)
        self.discovered_count += 1
        await queue.put((url, category or "tong-hop"))
        return True

    async def _scan_sitemap(
        self,
        client: httpx.AsyncClient,
        base_url: str,
        queue: asyncio.Queue,
        queued_set: Set[str],
        category_urls: List[str],
        seen_cats: Set[str],
    ) -> None:
        sitemap_queue: List[str] = [urljoin(base_url, p) for p in self.sitemap_paths]
        visited_sitemaps: Set[str] = set()

        while sitemap_queue:
            if self._reached_discovery_cap():
                return

            sm_url = sitemap_queue.pop(0)
            if sm_url in visited_sitemaps:
                continue
            visited_sitemaps.add(sm_url)

            res = await self.fetch_html(client, sm_url, retries=1)
            if res.status_code != 200 or not res.text:
                continue

            locs = re.findall(r"<loc>\s*(https?://[^<\s]+)\s*</loc>", res.text)
            if not locs:
                continue

            if "<sitemapindex" in res.text or sm_url.endswith("sitemap_index.xml"):
                for sub_loc in locs:
                    if sub_loc not in visited_sitemaps and self.is_relevant_sitemap(sub_loc):
                        sitemap_queue.append(sub_loc)
            else:
                for loc in locs:
                    if self.is_book_url(loc):
                        await self._enqueue_book(loc, queue, queued_set=queued_set)
                        if self._reached_discovery_cap():
                            return
                    elif self.is_category_url(loc) and loc not in seen_cats:
                        seen_cats.add(loc)
                        category_urls.append(loc)

    async def _crawl_listing_pages(
        self,
        client: httpx.AsyncClient,
        base_url: str,
        queue: asyncio.Queue,
        queued_set: Set[str],
        pages_to_visit: List[Tuple[str, str]],
        visited_listing: Set[str],
        seen_cats: Set[str],
        max_pages: int,
    ) -> None:
        """Visit category/listing pages BFS-style, enqueueing discovered book URLs."""
        pages_scanned = 0
        while pages_to_visit and pages_scanned < max_pages:
            if self._reached_discovery_cap():
                return
            page_url, cat_name = pages_to_visit.pop(0)
            if page_url in visited_listing:
                continue
            visited_listing.add(page_url)
            if (
                page_url != base_url
                and self.config.max_books_per_source == 0
                and await self.db.is_page_visited(page_url)
            ):
                continue

            pages_scanned += 1
            res = await self.fetch_html(client, page_url, retries=1)
            if res.blocked:
                self.status_message = "Bị chặn CAPTCHA/Cloudflare"
                return
            if res.status_code != 200 or not res.text:
                continue

            await self.db.mark_page_visited(page_url, self.name)
            soup = BeautifulSoup(res.text, "lxml")

            for book_url in self.extract_book_links(soup, res.url):
                await self._enqueue_book(
                    book_url, queue, category=cat_name, queued_set=queued_set
                )
                if self._reached_discovery_cap():
                    return

            for cat_url in self.extract_category_links(soup, res.url):
                if cat_url not in seen_cats:
                    seen_cats.add(cat_url)
                    pages_to_visit.append((cat_url, self._category_from_url(cat_url)))
            for next_url in self.extract_pagination_links(soup, res.url):
                if next_url not in visited_listing:
                    pages_to_visit.append((next_url, cat_name))

    async def discover_books(
        self, client: httpx.AsyncClient, queue: asyncio.Queue
    ) -> None:
        """Actively discover books via seed categories, Katana, sitemaps, and pagination."""
        self.status_message = "Đang tìm nguồn..."
        base_url = await self.resolve_active_mirror(client)
        if not base_url:
            return

        pending_cap = (
            max(self.config.max_books_per_source * 5, self.config.max_books_per_source + 15)
            if self.config.max_books_per_source > 0
            else 0
        )

        # Re-enqueue pending books from previous interrupted runs
        queued_set: Set[str] = set()
        pending = await self.db.get_pending_books(self.name, limit=pending_cap)
        for row in pending:
            url = row["url"]
            if url not in queued_set:
                queued_set.add(url)
                self.discovered_count += 1
                await queue.put((url, row.get("category") or "tong-hop"))

        category_urls: List[str] = []
        seen_cats: Set[str] = set()
        visited_listing: Set[str] = set()

        # Priority seed categories (before Katana bursts homepage links)
        if self.seed_category_paths and not self._reached_discovery_cap():
            self.status_message = "Đang quét danh mục ưu tiên..."
            seed_pages = [
                (urljoin(base_url, p), self._category_from_url(p))
                for p in self.seed_category_paths
            ]
            for sp_url, _ in seed_pages:
                seen_cats.add(sp_url)
            await self._crawl_listing_pages(
                client,
                base_url,
                queue,
                queued_set,
                seed_pages,
                visited_listing,
                seen_cats,
                max_pages=min(10, self.config.max_discovery_pages),
            )

        # Phase 0: ProjectDiscovery Katana high-speed Go discovery (skip on strict crawl_delay sites)
        katana = KatanaDiscovery(self.config.katana_bin)
        if (
            self.config.use_katana
            and self.crawl_delay < 2.0
            and katana.is_available
            and not self._reached_discovery_cap()
        ):
            self.status_message = "Đang quét bằng Katana..."
            try:
                max_raw = max(pending_cap * 30, 300) if pending_cap > 0 else 0
                async for found_url in katana.stream_discovered_urls(
                    base_url,
                    depth=self.config.katana_depth,
                    js_crawl=True,
                    known_files=True,
                    headless=self.config.katana_headless,
                    max_urls=max_raw,
                    delay=int(self.crawl_delay) if self.crawl_delay >= 1.0 else 0,
                ):
                    clean_url = found_url.split("#")[0]
                    if self.is_book_url(clean_url):
                        await self._enqueue_book(
                            clean_url, queue, queued_set=queued_set
                        )
                        if self._reached_discovery_cap():
                            break
                    elif self.is_category_url(clean_url) and clean_url not in seen_cats:
                        seen_cats.add(clean_url)
                        category_urls.append(clean_url)
            except Exception:
                # Fall back seamlessly to Python Sitemap & Category discovery
                pass

        # Phase A: Sitemap scan (supplements Katana if target cap not yet reached)
        if not self._reached_discovery_cap():
            self.status_message = "Đang quét Sitemap & Danh mục..."
            await self._scan_sitemap(
                client, base_url, queue, queued_set, category_urls, seen_cats
            )

        # Phase B: Homepage + Category & Pagination scan
        if not self._reached_discovery_cap():
            pages_to_visit: List[Tuple[str, str]] = [
                (urljoin(base_url, p), self._category_from_url(p))
                for p in self.seed_category_paths
            ]
            pages_to_visit.append((base_url, "tong-hop"))
            for cat in category_urls:
                pages_to_visit.append((cat, self._category_from_url(cat)))
            await self._crawl_listing_pages(
                client,
                base_url,
                queue,
                queued_set,
                pages_to_visit,
                visited_listing,
                seen_cats,
                max_pages=self.config.max_discovery_pages,
            )

        if not self.status_message.startswith("Bị chặn"):
            self.status_message = "Đang tải sách..."

    def _category_from_url(self, url: str) -> str:
        parts = [p for p in urlparse(url).path.strip("/").split("/") if p]
        return parts[-1] if parts else "tong-hop"

    async def parse_book(
        self, client: httpx.AsyncClient, url: str, category: str
    ) -> BookData:
        """Fetch and extract book metadata, download links, and optional chapters."""
        res = await self.fetch_html(client, url)
        if res.blocked:
            return BookData(url=url, title="", blocked_reason="blocked_by_captcha")
        if res.status_code != 200 or not res.text:
            return BookData(
                url=url, title="", blocked_reason=res.error or f"HTTP {res.status_code}"
            )

        soup = BeautifulSoup(res.text, "lxml")
        h1 = soup.find("h1")
        title = h1.get_text(" ", strip=True) if h1 else ""
        if not title and soup.title and soup.title.string:
            title = soup.title.string.split("|")[0].split("-")[0].strip()

        links = self.extract_ebook_links(soup, res.url, res.text)
        return BookData(
            url=res.url,
            title=title or slugify_filename(urlparse(url).path),
            category=category or "tong-hop",
            epub_url=links.get("epub"),
            pdf_url=links.get("pdf"),
        )

    async def process_book(
        self,
        client: httpx.AsyncClient,
        downloader: FileDownloader,
        download_sem: asyncio.Semaphore,
        url: str,
        category: str,
    ) -> None:
        """Parse a book, download EPUB/PDF (or compile chapters), save metadata, update DB."""
        await self.db.mark_processing(url)
        try:
            book = await self.parse_book(client, url, category)
        except Exception as exc:
            self.failed_count += 1
            self.processed_count += 1
            await self.db.update_book_result(
                url, status="failed", error=f"{type(exc).__name__}: {exc}"
            )
            return

        if book.blocked_reason:
            self.failed_count += 1
            self.processed_count += 1
            status = "blocked_by_captcha" if book.blocked_reason == "blocked_by_captcha" else "failed"
            await self.db.update_book_result(url, status=status, error=book.blocked_reason)
            return

        cat_slug = slugify_filename(book.category or category or "tong-hop", max_len=40)
        book_slug = slugify_filename(book.title or urlparse(url).path, max_len=80)
        book_dir = self.config.output_dir / self.name / cat_slug
        book_dir.mkdir(parents=True, exist_ok=True)

        epub_path_str: Optional[str] = None
        pdf_path_str: Optional[str] = None
        errors: List[str] = []

        # Download EPUB if requested and available
        if "epub" in self.config.formats:
            dest_epub = book_dir / f"{book_slug}.epub"
            if book.epub_url:
                async with download_sem:
                    ok = await downloader.download_book_file(
                        client, book.epub_url, dest_epub, "epub", referer=book.url
                    )
                if ok:
                    epub_path_str = str(dest_epub)
                    self.epub_count += 1
                else:
                    errors.append("epub_download_failed")
            elif book.chapters:
                try:
                    built = build_epub_from_chapters(
                        title=book.title,
                        author=book.author,
                        description=book.description,
                        chapters=book.chapters,
                        output_path=dest_epub,
                    )
                    if validate_file_header(built, "epub"):
                        epub_path_str = str(built)
                        self.epub_count += 1
                except Exception as exc:
                    errors.append(f"epub_build_failed: {exc}")

        # Download PDF only if EPUB was not obtained (prefer EPUB when both exist)
        if "pdf" in self.config.formats and book.pdf_url and not epub_path_str:
            dest_pdf = book_dir / f"{book_slug}.pdf"
            async with download_sem:
                ok = await downloader.download_book_file(
                    client, book.pdf_url, dest_pdf, "pdf", referer=book.url
                )
            if ok:
                pdf_path_str = str(dest_pdf)
                self.pdf_count += 1
            else:
                errors.append("pdf_download_failed")

        # Save metadata JSON beside downloaded books
        if epub_path_str or pdf_path_str:
            meta_path = book_dir / f"{book_slug}.meta.json"
            meta_payload = {
                "url": book.url,
                "source": self.name,
                "title": book.title,
                "author": book.author,
                "category": book.category,
                "description": book.description,
                "epub_url": book.epub_url,
                "pdf_url": book.pdf_url,
                "epub_path": epub_path_str,
                "pdf_path": pdf_path_str,
            }
            meta_path.write_text(
                json.dumps(meta_payload, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )

        got_count = int(bool(epub_path_str)) + int(bool(pdf_path_str))
        if got_count == 0:
            if not book.epub_url and not book.pdf_url and not book.chapters:
                final_status = "no_files"
                self.no_files_count += 1
            else:
                final_status = "failed"
                self.failed_count += 1
        else:
            final_status = "completed"

        self.processed_count += 1
        await self.db.update_book_result(
            url,
            status=final_status,
            title=book.title,
            author=book.author,
            category=book.category,
            epub_url=book.epub_url,
            pdf_url=book.pdf_url,
            epub_path=epub_path_str,
            pdf_path=pdf_path_str,
            error="; ".join(errors) if errors else None,
        )
