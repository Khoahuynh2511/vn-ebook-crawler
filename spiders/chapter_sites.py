import asyncio
import re
from typing import Dict, List, Set, Tuple
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup
import httpx

from config import SOURCE_MIRRORS
from spiders.base import BaseEbookSpider, BookData, slugify_filename


class SachHayOnlineSpider(BaseEbookSpider):
    """Spider for sachhayonline.com: crawls /tua-sach/<slug> chapters and builds EPUB."""

    name = "sachhayonline"
    base_urls = SOURCE_MIRRORS["sachhayonline"]

    def is_relevant_sitemap(self, sitemap_url: str) -> bool:
        return "sitemap-book-detail" in sitemap_url.lower()

    def is_book_url(self, url: str) -> bool:
        path = urlparse(url).path.strip("/")
        parts = path.split("/")
        # Book page is /tua-sach/<book-slug> (depth 2); chapters have depth >= 3
        return len(parts) == 2 and parts[0] == "tua-sach" and bool(parts[1])

    def is_category_url(self, url: str) -> bool:
        path = urlparse(url).path.strip("/")
        parts = path.split("/")
        return len(parts) == 2 and parts[0] == "danh-muc" and bool(parts[1])

    async def _fetch_chapters(
        self, client: httpx.AsyncClient, chapter_links: List[Tuple[str, str]], referer: str
    ) -> List[Tuple[str, str]]:
        sem = asyncio.Semaphore(5)

        async def fetch_one(idx: int, title: str, chap_url: str) -> Tuple[int, str, str]:
            async with sem:
                res = await self.fetch_html(client, chap_url, retries=2, referer=referer)
                if res.status_code != 200 or not res.text:
                    return (idx, title, "")
                csoup = BeautifulSoup(res.text, "lxml")
                content = csoup.select_one(".chapter-content, #content article, .detail")
                if not content:
                    return (idx, title, "")
                # Remove inline navigation / ad blocks inside chapter-content if any
                for bad in content.select("script, style, .ads, .navigation"):
                    bad.decompose()
                return (idx, title, content.decode_contents())

        results = await asyncio.gather(
            *(fetch_one(i, t, u) for i, (t, u) in enumerate(chapter_links[:80]))
        )
        results.sort(key=lambda x: x[0])
        return [(t, b) for _, t, b in results if b.strip()]

    async def parse_book(
        self, client: httpx.AsyncClient, url: str, category: str
    ) -> BookData:
        res = await self.fetch_html(client, url)
        if res.blocked:
            return BookData(url=url, title="", blocked_reason="blocked_by_captcha")
        if res.status_code != 200 or not res.text:
            return BookData(
                url=url, title="", blocked_reason=res.error or f"HTTP {res.status_code}"
            )

        soup = BeautifulSoup(res.text, "lxml")
        title = ""
        h1 = soup.find("h1")
        if h1:
            title = h1.get_text(" ", strip=True)
        if not title and soup.title and soup.title.string:
            title = soup.title.string.split("|")[0].strip()

        book_path = urlparse(res.url).path.rstrip("/")
        links = self.extract_ebook_links(soup, res.url, res.text)
        epub_url = links.get("epub")
        pdf_url = links.get("pdf")

        # 1. Legacy chapter URLs: /tua-sach/<slug>/<chap>/<id> (sort by trailing numeric ID)
        seen_legacy: Set[str] = set()
        raw_legacy: List[Tuple[int, str, str]] = []
        # 2. Modern chapter URLs: /doc-online/<chap>/<id> (preserve exact DOM TOC order)
        doc_online_toc: Dict[str, str] = {}
        doc_online_fallback: List[str] = []
        seen_fallback: Set[str] = set()

        for idx, a in enumerate(soup.find_all("a", href=True)):
            abs_u = urljoin(res.url, a["href"]).split("#")[0]
            chap_path = urlparse(abs_u).path.rstrip("/")
            link_text = a.get_text(" ", strip=True)

            if chap_path.startswith(book_path + "/") and abs_u not in seen_legacy:
                seen_legacy.add(abs_u)
                chap_title = link_text or f"Chương {len(raw_legacy) + 1}"
                m = re.search(r"/(\d+)$", chap_path)
                order_key = int(m.group(1)) if m else idx
                raw_legacy.append((order_key, chap_title, abs_u))
            elif chap_path.startswith("/doc-online/"):
                is_read_btn = "đọc online" in link_text.lower()
                if not is_read_btn and link_text:
                    if abs_u not in doc_online_toc:
                        doc_online_toc[abs_u] = link_text
                elif abs_u not in seen_fallback:
                    seen_fallback.add(abs_u)
                    doc_online_fallback.append(abs_u)

        chap_links: List[Tuple[str, str]] = []
        if raw_legacy:
            raw_legacy.sort(key=lambda x: x[0])
            chap_links = [(t, u) for _, t, u in raw_legacy]
        elif doc_online_toc or doc_online_fallback:
            chap_links = [(t, u) for u, t in doc_online_toc.items()]
            for u in doc_online_fallback:
                if u not in doc_online_toc:
                    chap_links.append((f"Chương {len(chap_links) + 1}", u))

        chapters = (
            await self._fetch_chapters(client, chap_links, res.url)
            if chap_links and not epub_url and "epub" in self.config.formats
            else []
        )

        # Try extracting category from breadcrumb
        cat = category or "tong-hop"
        crumbs = soup.select('a[href*="/danh-muc/"]')
        if crumbs:
            # First breadcrumb link inside main content area tends to be book's category
            for c in crumbs:
                href_p = urlparse(c["href"]).path.strip("/")
                if href_p.startswith("danh-muc/"):
                    cat = c.get_text(" ", strip=True) or cat
                    break

        return BookData(
            url=res.url,
            title=title or slugify_filename(book_path),
            category=cat,
            epub_url=epub_url,
            pdf_url=pdf_url,
            chapters=chapters,
        )


class IsachSpider(BaseEbookSpider):
    """Spider for isach.info: respects Crawl-delay and compiles chapters into EPUB."""

    name = "isach"
    base_urls = SOURCE_MIRRORS["isach"]
    sitemap_paths = []
    crawl_delay = 3.0

    def is_book_url(self, url: str) -> bool:
        low = url.lower()
        return (
            "isach.info" in low
            and "story.php?story=" in low
            and "chapter=" not in low
            and "isach_tai_lieu_huong_dan" not in low
        )

    def is_category_url(self, url: str) -> bool:
        low = url.lower()
        return "isach.info" in low and "story.php?list=story&category=" in low

    async def _fetch_chapters(
        self, client: httpx.AsyncClient, chap_links: List[Tuple[str, str]], referer: str
    ) -> List[Tuple[str, str]]:
        # Keep concurrency low (1-2) on isach.info due to strict server connection drops
        sem = asyncio.Semaphore(1)

        async def fetch_one(idx: int, title: str, chap_url: str) -> Tuple[int, str, str]:
            async with sem:
                res = await self.fetch_html(client, chap_url, retries=2, referer=referer)
                if res.status_code != 200 or not res.text:
                    return (idx, title, "")
                csoup = BeautifulSoup(res.text, "lxml")
                best_div = max(
                    csoup.find_all("div"),
                    key=lambda d: len(d.find_all("p", recursive=False)),
                    default=None,
                )
                body = best_div.decode_contents() if best_div else ""
                return (idx, title, body)

        results = []
        for i, (t, u) in enumerate(chap_links[:100]):
            results.append(await fetch_one(i, t, u))
        return [(t, b) for _, t, b in results if b.strip()]

    async def parse_book(
        self, client: httpx.AsyncClient, url: str, category: str
    ) -> BookData:
        res = await self.fetch_html(client, url, retries=2)
        if res.blocked:
            return BookData(url=url, title="", blocked_reason="blocked_by_captcha")
        if res.status_code != 200 or not res.text:
            return BookData(
                url=url, title="", blocked_reason=res.error or f"HTTP {res.status_code}"
            )

        soup = BeautifulSoup(res.text, "lxml")
        title = soup.title.string.split("|")[0].strip() if soup.title and soup.title.string else ""
        story_match = re.search(r"story=([^&]+)", res.url)
        story_key = story_match.group(1) if story_match else ""

        seen: Set[str] = set()
        chap_links: List[Tuple[str, str]] = []
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if "chapter=" in href and (not story_key or story_key in href):
                abs_u = urljoin(res.url, href)
                if abs_u not in seen:
                    seen.add(abs_u)
                    chap_links.append((a.get_text(" ", strip=True) or "Chương", abs_u))

        chapters: List[Tuple[str, str]] = []
        if chap_links and "epub" in self.config.formats:
            chapters = await self._fetch_chapters(client, chap_links, res.url)
        elif "epub" in self.config.formats:
            # Single-page story without separate chapters
            best_div = max(
                soup.find_all("div"),
                key=lambda d: len(d.find_all("p", recursive=False)),
                default=None,
            )
            if best_div and len(best_div.find_all("p", recursive=False)) >= 2:
                chapters = [(title or "Nội dung", best_div.decode_contents())]

        return BookData(
            url=res.url,
            title=title or slugify_filename(story_key or url),
            category=category or "van-hoc",
            chapters=chapters,
        )
