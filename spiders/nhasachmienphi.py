import asyncio
from typing import List, Set, Tuple
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup
import httpx

from config import SOURCE_MIRRORS
from spiders.base import BaseEbookSpider, BookData, slugify_filename


class NhaSachMienPhiSpider(BaseEbookSpider):
    """Spider for nhasachmienphi.com: direct PDF/EPUB links plus .item_ch chapter fallback."""

    name = "nhasachmienphi"
    base_urls = SOURCE_MIRRORS["nhasachmienphi"]

    def is_book_url(self, url: str) -> bool:
        parsed = urlparse(url)
        if "nhasachmienphi" not in parsed.netloc:
            return False
        path = parsed.path.strip("/")
        if not path or "/" in path:
            return False
        return path.endswith(".html") and not any(
            x in path for x in ("gioi-thieu", "lien-he", "chinh-sach")
        )

    def is_category_url(self, url: str) -> bool:
        path = urlparse(url).path.strip("/")
        return path.startswith("category/")

    async def _fetch_chapters(
        self, client: httpx.AsyncClient, chap_links: List[Tuple[str, str]], referer: str
    ) -> List[Tuple[str, str]]:
        sem = asyncio.Semaphore(4)

        async def fetch_one(idx: int, title: str, chap_url: str) -> Tuple[int, str, str]:
            async with sem:
                res = await self.fetch_html(client, chap_url, retries=1, referer=referer)
                if res.status_code != 200 or not res.text:
                    return (idx, title, "")
                csoup = BeautifulSoup(res.text, "lxml")
                content = csoup.select_one(".noi_dung_online, .entry-content, article")
                return (idx, title, content.decode_contents() if content else "")

        results = await asyncio.gather(
            *(fetch_one(i, t, u) for i, (t, u) in enumerate(chap_links[:150]))
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
        h1 = soup.select_one("h1.tieu_de_sach, h1")
        title = h1.get_text(" ", strip=True) if h1 else ""
        links = self.extract_ebook_links(soup, res.url, res.text)

        chapters: List[Tuple[str, str]] = []
        if not links.get("epub") and "epub" in self.config.formats:
            seen: Set[str] = set()
            chap_links: List[Tuple[str, str]] = []
            for a in soup.select(".item_ch a[href]"):
                abs_u = urljoin(res.url, a["href"])
                if abs_u not in seen:
                    seen.add(abs_u)
                    chap_links.append((a.get_text(" ", strip=True) or "Chương", abs_u))
            if chap_links:
                chapters = await self._fetch_chapters(client, chap_links, res.url)

        return BookData(
            url=res.url,
            title=title or slugify_filename(urlparse(url).path),
            category=category or "tong-hop",
            epub_url=links.get("epub"),
            pdf_url=links.get("pdf"),
            chapters=chapters,
        )
