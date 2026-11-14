import re
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup
import httpx

from config import SOURCE_MIRRORS
from spiders.base import BaseEbookSpider, BookData, slugify_filename


class ThuVienSachSpider(BaseEbookSpider):
    """Spider for thuviensach.vn and metaisach.com."""

    name = "thuviensach"
    base_urls = SOURCE_MIRRORS["thuviensach"]
    seed_category_paths = [
        "/thu-vien/khai-tam-mo-tri/",
        "/tu-sach-tinh-hoa/",
        "/thu-vien/ton-giao-tam-linh/",
    ]

    def is_relevant_sitemap(self, sitemap_url: str) -> bool:
        low = sitemap_url.lower()
        return "posts.xml" in low or "book" in low or "post-sitemap" in low

    def is_book_url(self, url: str) -> bool:
        parsed = urlparse(url)
        host = parsed.netloc.lower()
        path = parsed.path.strip("/")
        if "metaisach.com" in host:
            if not path or "/" in path:
                return False
            return not any(
                path.startswith(p)
                for p in ("author", "category", "tag", "set", "reader", "__sitemap__")
            )
        if "thuviensach.vn" in host:
            return bool(re.search(r"-\d+\.html$", path)) and not any(
                x in path for x in ("review-sach", "1066", "1055", "5493")
            )
        return False

    def is_category_url(self, url: str) -> bool:
        path = urlparse(url).path.strip("/")
        return (
            path.startswith("category/")
            or path.startswith("thu-vien/")
            or path.startswith("tu-sach-tinh-hoa")
        )

    async def _resolve_metaisach_reader(
        self, client: httpx.AsyncClient, reader_url: str, referer: str
    ) -> tuple[str | None, str | None]:
        res = await self.fetch_html(client, reader_url, retries=1, referer=referer)
        if res.status_code != 200 or not res.text:
            return None, None
        soup = BeautifulSoup(res.text, "lxml")
        links = self.extract_ebook_links(soup, res.url, res.text)
        # Prefer media.metaisach.com direct URLs over /reader/ URLs
        epub_url = links.get("epub")
        pdf_url = links.get("pdf")
        for media_url in re.findall(
            r'https?://media\.metaisach\.com/[^\s"\'<>\\]+\.(?:pdf|epub)(?:\?[^\s"\'<>\\]*)?',
            res.text,
            re.I,
        ):
            if media_url.lower().endswith(".epub"):
                epub_url = media_url
            elif media_url.lower().endswith(".pdf"):
                pdf_url = media_url
        return epub_url, pdf_url

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

        # Extract title (on metaisach.com, first <h1> can be the site logo 'Mê Tải Sách')
        title = ""
        for h1 in soup.find_all("h1"):
            txt = h1.get_text(" ", strip=True)
            if txt and txt.lower() not in ("mê tải sách", "thư viện sách"):
                title = txt
                break
        if not title and soup.title and soup.title.string:
            title = soup.title.string.split("|")[0].strip()

        author_el = soup.select_one('a[href*="/author/"], a[href*="/tac-gia/"]')
        author = author_el.get_text(" ", strip=True) if author_el else "Khuyết danh"

        cat_el = soup.select_one('a[href*="/category/"], a[href*="/thu-vien/"]')
        cat = cat_el.get_text(" ", strip=True) if cat_el else (category or "tong-hop")

        links = self.extract_ebook_links(soup, res.url, res.text)
        epub_url = links.get("epub")
        pdf_url = links.get("pdf")

        # On metaisach.com, follow /reader/... links to locate media.metaisach.com file URLs
        if "metaisach.com" in res.url:
            reader_hrefs = [
                urljoin(res.url, a["href"])
                for a in soup.select('a[href*="/reader/"]')
                if a.get("href")
            ]
            reader_hrefs.sort(key=lambda u: 0 if ".epub" in u.lower() else 1)
            # Avoid falling back to the HTML /reader/ route itself as a downloadable binary
            if pdf_url and "/reader/" in pdf_url:
                pdf_url = None
            if epub_url and "/reader/" in epub_url:
                epub_url = None

            if not ("epub" in self.config.formats and epub_url):
                for reader_u in reader_hrefs[:3]:
                    r_epub, r_pdf = await self._resolve_metaisach_reader(
                        client, reader_u, res.url
                    )
                    if r_epub and not epub_url:
                        epub_url = r_epub
                    if r_pdf and not pdf_url:
                        pdf_url = r_pdf
                    if ("epub" in self.config.formats and epub_url) or (
                        "epub" not in self.config.formats and pdf_url
                    ):
                        break

        return BookData(
            url=res.url,
            title=title or slugify_filename(urlparse(url).path),
            author=author,
            category=cat,
            epub_url=epub_url,
            pdf_url=pdf_url,
        )


class MetaisachSpider(ThuVienSachSpider):
    """Dedicated parallel spider for metaisach.com (direct PDF/EPUB library)."""

    name = "metaisach"
    base_urls = SOURCE_MIRRORS["metaisach"]
    seed_category_paths = []
