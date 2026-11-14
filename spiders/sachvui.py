import re
from typing import Optional
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup
import httpx

from config import SOURCE_MIRRORS
from spiders.base import BaseEbookSpider, BookData, slugify_filename

NON_BOOK_SLUGS = {
    "get-link",
    "zalo",
    "lien-he",
    "gioi-thieu",
    "chinh-sach-bao-mat",
    "dieu-khoan-su-dung",
}


class SachVuiSpider(BaseEbookSpider):
    """Spider for sachvuii.com / sachvui.com."""

    name = "sachvui"
    base_urls = SOURCE_MIRRORS["sachvui"]

    def is_relevant_sitemap(self, sitemap_url: str) -> bool:
        return "post-sitemap" in sitemap_url.lower()

    def is_book_url(self, url: str) -> bool:
        parsed = urlparse(url)
        if "sachvui" not in parsed.netloc:
            return False
        path = parsed.path.strip("/")
        if not path or "/" in path:
            return False
        if path in NON_BOOK_SLUGS:
            return False
        return True

    def is_category_url(self, url: str) -> bool:
        path = urlparse(url).path.strip("/")
        return path.startswith("the-loai/")

    async def _resolve_get_link(
        self, client: httpx.AsyncClient, link_url: str, referer: str
    ) -> Optional[str]:
        res = await self.fetch_html(client, link_url, retries=1, referer=referer)
        if res.status_code != 200 or not res.text:
            return None
        # Check if /get-link/ is gated by manual Google pass instructions
        if "LÀM THEO HƯỚNG DẪN ĐỂ LẤY PASS" in res.text or "cf-turnstile" in res.text.lower():
            return None
        soup = BeautifulSoup(res.text, "lxml")
        links = self.extract_ebook_links(soup, res.url, res.text)
        return links.get("pdf") or links.get("epub")

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
        h1 = soup.select_one("h1.ebook_title, h1")
        title = h1.get_text(" ", strip=True) if h1 else ""
        title = re.sub(r"\s*(?:PDF|EPUB|MOBI|AZW3|PRC|full)+\s*$", "", title, flags=re.I).strip()

        author_el = soup.select_one('a[href*="/tac-gia/"]')
        author = author_el.get_text(" ", strip=True) if author_el else "Khuyết danh"

        cat_el = soup.select_one('a[rel~="category"]')
        cat = cat_el.get_text(" ", strip=True) if cat_el else (category or "tong-hop")

        links = self.extract_ebook_links(soup, res.url, res.text)
        epub_url = links.get("epub")
        pdf_url = links.get("pdf")

        # Resolve /get-link/?type=... buttons if direct links weren't found
        for a in soup.select('a[href*="/get-link/"]'):
            href = a.get("href", "")
            abs_u = urljoin(res.url, href)
            if "type=epub" in href.lower() and not epub_url:
                epub_url = await self._resolve_get_link(client, abs_u, res.url)
            elif "type=pdf" in href.lower() and not pdf_url:
                pdf_url = await self._resolve_get_link(client, abs_u, res.url)

        return BookData(
            url=res.url,
            title=title or slugify_filename(urlparse(url).path),
            author=author,
            category=cat,
            epub_url=epub_url,
            pdf_url=pdf_url,
        )
