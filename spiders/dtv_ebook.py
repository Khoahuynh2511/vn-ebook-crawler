import re
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup
import httpx

from config import SOURCE_MIRRORS
from spiders.base import BaseEbookSpider, BookData, slugify_filename


class DtvEbookSpider(BaseEbookSpider):
    """Spider for dtv-ebook.com.vn (skips gracefully if Cloudflare/CAPTCHA gated)."""

    name = "dtv_ebook"
    base_urls = SOURCE_MIRRORS["dtv_ebook"]

    def is_book_url(self, url: str) -> bool:
        path = urlparse(url).path.lower()
        return "tai-sach-" in path or path.endswith(".html") and "/the-loai/" not in path

    def is_category_url(self, url: str) -> bool:
        return "/the-loai/" in urlparse(url).path.lower()

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
        links = self.extract_ebook_links(soup, res.url, res.text)

        # Follow download frame links if present
        for a in soup.select('a[href*="download"], a[href*="tai-ve"]'):
            href = a.get("href", "")
            if not href:
                continue
            txt = a.get_text(" ", strip=True).lower()
            abs_u = urljoin(res.url, href)
            if "epub" in txt and not links.get("epub"):
                links["epub"] = abs_u
            elif "pdf" in txt and not links.get("pdf"):
                links["pdf"] = abs_u

        return BookData(
            url=res.url,
            title=title or slugify_filename(urlparse(url).path),
            category=category or "tong-hop",
            epub_url=links.get("epub"),
            pdf_url=links.get("pdf"),
        )
