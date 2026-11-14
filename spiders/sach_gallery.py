import asyncio
import re
from typing import List, Optional, Set, Tuple
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup
import httpx

from config import SOURCE_MIRRORS
from spiders.base import BaseEbookSpider, BookData, slugify_filename


class SachGallerySpider(BaseEbookSpider):
    """Spider for sach.gallery: supports direct /tai/<id> EPUB downloads and /doc/ chapters."""

    name = "sach_gallery"
    base_urls = SOURCE_MIRRORS["sach_gallery"]

    def is_relevant_sitemap(self, sitemap_url: str) -> bool:
        # Focus directly on book sitemaps rather than author/static page sitemaps
        low = sitemap_url.lower()
        return "/books/" in low or "book" in low

    def is_book_url(self, url: str) -> bool:
        path = urlparse(url).path.strip("/")
        parts = path.split("/")
        return len(parts) == 2 and parts[0] == "sach" and bool(parts[1])

    def is_category_url(self, url: str) -> bool:
        path = urlparse(url).path.strip("/")
        parts = path.split("/")
        return len(parts) == 2 and parts[0] in ("the-loai", "bo-suu-tap") and bool(parts[1])

    async def _fetch_chapters(
        self, client: httpx.AsyncClient, chapter_urls: List[Tuple[str, str]], referer: str
    ) -> List[Tuple[str, str]]:
        sem = asyncio.Semaphore(4)

        async def fetch_one(idx: int, chap_title: str, chap_url: str) -> Tuple[int, str, str]:
            async with sem:
                res = await self.fetch_html(client, chap_url, retries=1, referer=referer)
                if res.status_code != 200 or not res.text:
                    return (idx, chap_title, "")
                csoup = BeautifulSoup(res.text, "lxml")
                container = csoup.select_one("article, main, .prose, .chapter-content")
                body = container.decode_contents() if container else ""
                return (idx, chap_title, body)

        tasks = [
            fetch_one(i, title, u) for i, (title, u) in enumerate(chapter_urls[:150])
        ]
        results = await asyncio.gather(*tasks)
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
        h1 = soup.find("h1")
        title = h1.get_text(" ", strip=True) if h1 else ""
        if not title and soup.title and soup.title.string:
            title = soup.title.string.split("|")[0].strip()

        author_el = soup.select_one('a[href^="/tac-gia/"]')
        author = author_el.get_text(" ", strip=True) if author_el else "Khuyết danh"

        cat_el = soup.select_one('a[href^="/the-loai/"]')
        cat = cat_el.get_text(" ", strip=True) if cat_el else (category or "tong-hop")

        meta_desc = soup.select_one('meta[name="description"]')
        desc = meta_desc.get("content", "") if meta_desc else ""

        links = self.extract_ebook_links(soup, res.url, res.text)
        epub_url = links.get("epub")
        pdf_url = links.get("pdf")

        # Explicitly check sach.gallery /tai/<id> links
        if not epub_url:
            for a in soup.select('a[href*="/tai/"]'):
                href = a.get("href", "")
                if href:
                    abs_dl = urljoin(res.url, href)
                    if "pdf" in a.get_text(" ", strip=True).lower():
                        pdf_url = pdf_url or abs_dl
                    else:
                        epub_url = abs_dl
                        break

        chapters: List[Tuple[str, str]] = []
        if not epub_url and "epub" in self.config.formats:
            seen_ch: Set[str] = set()
            chap_links: List[Tuple[str, str]] = []
            for a in soup.select('a[href*="/doc/"]'):
                href = a.get("href", "")
                if "?ch=" in href:
                    abs_ch = urljoin(res.url, href)
                    if abs_ch not in seen_ch:
                        seen_ch.add(abs_ch)
                        chap_links.append((a.get_text(" ", strip=True) or "Chương", abs_ch))
            if chap_links:
                chapters = await self._fetch_chapters(client, chap_links, res.url)

        return BookData(
            url=res.url,
            title=title or slugify_filename(urlparse(url).path),
            author=author,
            category=cat,
            description=desc,
            epub_url=epub_url,
            pdf_url=pdf_url,
            chapters=chapters,
        )
