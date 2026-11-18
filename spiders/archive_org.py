import asyncio
import json
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote, urlparse

import httpx

from config import SOURCE_MIRRORS
from spiders.base import BaseEbookSpider, BookData, slugify_filename

MAX_DERIVATIVE_EPUB_BYTES = 25 * 1024 * 1024
MAX_IA_PDF_BYTES = 100 * 1024 * 1024


def _first_str(val: Any, default: str = "") -> str:
    if isinstance(val, list):
        for item in val:
            if item:
                return str(item).strip()
        return default
    if val is None:
        return default
    return str(val).strip() or default


def select_best_archive_files(
    files: List[Dict[str, Any]], *, allow_pdf: bool = True
) -> Tuple[Optional[str], Optional[str]]:
    """Select the cleanest open DRM-free EPUB and/or fallback PDF from Archive.org metadata files."""
    epubs: List[Tuple[int, int, str]] = []
    pdfs: List[Tuple[int, int, str]] = []

    for f in files:
        name = str(f.get("name") or "")
        low = name.lower()
        fmt = str(f.get("format") or "").lower()
        if not name or f.get("private") in (True, "true", "True", 1):
            continue
        if "_encrypted." in low or "_lcp." in low or "encrypted" in fmt:
            continue

        try:
            size = int(f.get("size") or 0)
        except (TypeError, ValueError):
            size = 0
        if size <= 1024:
            continue

        is_original = 0 if str(f.get("source") or "").lower() == "original" else 1

        if low.endswith(".epub"):
            epubs.append((is_original, size, name))
        elif low.endswith(".pdf"):
            is_text_pdf = 0 if "text pdf" in fmt else 1
            pdfs.append((is_original, is_text_pdf * 10 + (1 if size > MAX_IA_PDF_BYTES else 0), size, name))

    epubs.sort(key=lambda x: (x[0], x[1]))
    pdfs.sort(key=lambda x: (x[0], x[1], x[2]))

    chosen_epub: Optional[str] = None
    chosen_pdf: Optional[str] = pdfs[0][3] if pdfs else None

    if epubs:
        orig_flag, epub_size, epub_name = epubs[0]
        # Avoid downloading huge 150MB+ auto-OCR derivative EPUBs if a compact original PDF is available
        if (
            allow_pdf
            and orig_flag != 0
            and epub_size > MAX_DERIVATIVE_EPUB_BYTES
            and chosen_pdf is not None
            and pdfs[0][2] <= MAX_IA_PDF_BYTES
        ):
            chosen_epub = None
        else:
            chosen_epub = epub_name

    return chosen_epub, chosen_pdf


class BaseArchiveOrgSpider(BaseEbookSpider):
    """Shared async JSON-API spider for open Internet Archive (archive.org) texts."""

    base_urls = ["https://archive.org"]
    sitemap_paths = []
    search_query: str = ""
    default_category: str = "tong-hop"

    def is_book_url(self, url: str) -> bool:
        return "archive.org/details/" in url

    def _category_from_subject(self, subject: Any) -> str:
        raw = _first_str(subject, "")
        if not raw:
            return self.default_category
        first = raw.split(";")[0].split(",")[0].strip()
        return slugify_filename(first, max_len=40) if first else self.default_category

    async def discover_books(
        self, client: httpx.AsyncClient, queue: asyncio.Queue
    ) -> None:
        self.status_message = "Đang tìm nguồn Archive.org..."
        pending_cap = (
            max(self.config.max_books_per_source * 5, self.config.max_books_per_source + 15)
            if self.config.max_books_per_source > 0
            else 0
        )

        queued_set = set()
        pending = await self.db.get_pending_books(self.name, limit=pending_cap)
        for row in pending:
            url = row["url"]
            if url not in queued_set:
                queued_set.add(url)
                self.discovered_count += 1
                await queue.put((url, row.get("category") or self.default_category))

        if self._reached_discovery_cap():
            self.status_message = "Đang tải sách..."
            return

        # Resume exact API page & offset from discovery_queue
        saved_pages = await self.db.get_discovery_targets(self.name, "ia_api")
        start_page = 1
        start_offset = 0
        if saved_pages:
            last_url = saved_pages[0]["url"]
            start_offset = int(saved_pages[0].get("last_offset") or 0)
            if "page=" in last_url:
                try:
                    start_page = int(last_url.split("page=")[-1].split("&")[0])
                except ValueError:
                    start_page = 1

        page = start_page
        max_pages = max(1, self.config.max_discovery_pages)
        scanned_pages = 0

        while scanned_pages < max_pages and not self._reached_discovery_cap():
            cursor_url = f"https://archive.org/advancedsearch.php?source={self.name}&page={page}"
            await self.db.enqueue_discovery_target(
                cursor_url, self.name, "ia_api", category=self.default_category
            )
            params = {
                "q": self.search_query,
                "fl[]": ["identifier", "title", "creator", "subject", "format"],
                "sort[]": "downloads desc",
                "rows": 50,
                "page": page,
                "output": "json",
            }
            url_with_qs = httpx.URL("https://archive.org/advancedsearch.php", params=params)
            res = await self.fetch_html(client, str(url_with_qs), retries=2)
            if res.status_code != 200 or not res.text:
                break

            try:
                payload = json.loads(res.text)
                docs = payload.get("response", {}).get("docs", [])
            except Exception:
                break

            if not docs:
                await self.db.update_discovery_progress(
                    cursor_url, self.name, "ia_api", last_offset=0, status="done"
                )
                break

            offset = start_offset if page == start_page else 0
            for idx in range(offset, len(docs)):
                doc = docs[idx]
                ident = str(doc.get("identifier") or "").strip()
                if not ident:
                    continue
                book_url = f"https://archive.org/details/{ident}"
                cat = self._category_from_subject(doc.get("subject"))
                await self._enqueue_book(book_url, queue, category=cat, queued_set=queued_set)
                if self._reached_discovery_cap():
                    await self.db.update_discovery_progress(
                        cursor_url,
                        self.name,
                        "ia_api",
                        last_offset=idx + 1,
                        status="pending",
                    )
                    await self.db.update_source_checkpoint(
                        self.name,
                        last_phase="ia_api",
                        last_cursor_url=cursor_url,
                        last_cursor_offset=idx + 1,
                    )
                    self.status_message = "Đang tải sách..."
                    return

            await self.db.update_discovery_progress(
                cursor_url, self.name, "ia_api", last_offset=len(docs), status="done"
            )
            page += 1
            scanned_pages += 1
            next_cursor = f"https://archive.org/advancedsearch.php?source={self.name}&page={page}"
            await self.db.enqueue_discovery_target(
                next_cursor, self.name, "ia_api", category=self.default_category
            )
            await self.db.update_source_checkpoint(
                self.name,
                last_phase="ia_api",
                last_cursor_url=next_cursor,
                last_cursor_offset=0,
            )

        self.status_message = "Đang tải sách..."

    async def parse_book(
        self, client: httpx.AsyncClient, url: str, category: str
    ) -> BookData:
        ident = urlparse(url).path.strip("/").split("/")[-1]
        if not ident:
            return BookData(url=url, title="", blocked_reason="invalid_identifier")

        meta_url = f"https://archive.org/metadata/{ident}"
        res = await self.fetch_html(client, meta_url, retries=2)
        if res.blocked:
            return BookData(url=url, title="", blocked_reason="blocked_by_captcha")
        if res.status_code != 200 or not res.text:
            return BookData(
                url=url, title="", blocked_reason=res.error or f"HTTP {res.status_code}"
            )

        try:
            data = json.loads(res.text)
        except Exception as exc:
            return BookData(url=url, title="", blocked_reason=f"invalid_json: {exc}")

        meta = data.get("metadata") or {}
        files = data.get("files") or []

        title = _first_str(meta.get("title"), ident)
        author = _first_str(meta.get("creator"), "Khuyết danh")
        cat = self._category_from_subject(meta.get("subject")) or category or self.default_category
        desc = _first_str(meta.get("description"), "")

        allow_pdf = "pdf" in self.config.formats
        epub_name, pdf_name = select_best_archive_files(files, allow_pdf=allow_pdf)

        epub_url = (
            f"https://archive.org/download/{ident}/{quote(epub_name, safe='/')}"
            if epub_name
            else None
        )
        pdf_url = (
            f"https://archive.org/download/{ident}/{quote(pdf_name, safe='/')}"
            if pdf_name
            else None
        )

        return BookData(
            url=url,
            title=title,
            author=author,
            category=cat,
            description=desc[:1000],
            epub_url=epub_url,
            pdf_url=pdf_url,
        )


class ArchiveOrgSpider(BaseArchiveOrgSpider):
    """Open Vietnamese EPUB/PDF library on Internet Archive (11,200+ open items)."""

    name = "archive_org"
    base_urls = SOURCE_MIRRORS["archive_org"]
    search_query = (
        'language:(vie OR Vietnamese) AND mediatype:texts AND format:(EPUB OR "Text PDF") '
        "AND -collection:(inlibrary OR printdisabled) AND -subject:8xbet"
    )
    default_category = "sach-tieng-viet"


class ArchiveOrgEnglishSpider(BaseArchiveOrgSpider):
    """Open English EPUB/PDF library on Internet Archive (731,000+ open items)."""

    name = "archive_org_en"
    base_urls = SOURCE_MIRRORS["archive_org_en"]
    search_query = (
        'language:(eng OR English) AND mediatype:texts AND format:(EPUB OR "Text PDF") '
        "AND -collection:(inlibrary OR printdisabled)"
    )
    default_category = "english-books"
