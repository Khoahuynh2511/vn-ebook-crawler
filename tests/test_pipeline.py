import asyncio
import json
import zipfile
from pathlib import Path
from unittest.mock import AsyncMock

from bs4 import BeautifulSoup
import pytest

from config import ACTIVE_SOURCES, CrawlerConfig
from epub_builder import build_epub_from_chapters
from gdrive_uploader import GDriveUploader, gdrive_file_url
from katana_runner import KatanaDiscovery
from main import resolve_sources
from resolvers import FileDownloader, _parse_retry_after, extract_gdrive_id, validate_file_header
from spiders.base import BaseEbookSpider, BookData, is_captcha_or_blocked
from spiders.chapter_sites import SachHayOnlineSpider
from spiders.sach_gallery import SachGallerySpider
from spiders.thuviensach import MetaisachSpider, ThuVienSachSpider
from storage import CatalogDB


def test_build_epub_from_chapters(tmp_path: Path) -> None:
    out_file = tmp_path / "test_book.epub"
    chapters = [
        ("Chương 1: Mở đầu", "<p>Xin chào thế giới.</p><p>Đoạn hai tiếng Việt.</p>"),
        ("Chương 2: Kết thúc", "Văn bản thuần có xuống dòng.\nDòng thứ hai & ký tự đặc biệt < >."),
    ]
    result = build_epub_from_chapters(
        title="Sách Thử Nghiệm",
        author="Tác Giả Việt",
        description="Mô tả ngắn gọn.",
        chapters=chapters,
        output_path=out_file,
    )

    assert result.exists()
    assert result.stat().st_size > 1024
    assert validate_file_header(result, "epub") is True

    with zipfile.ZipFile(result, "r") as zf:
        names = zf.namelist()
        assert names[0] == "mimetype"
        assert zf.read("mimetype") == b"application/epub+zip"
        assert "META-INF/container.xml" in names
        assert "OEBPS/content.opf" in names
        assert "OEBPS/nav.xhtml" in names
        assert "OEBPS/toc.ncx" in names
        assert "OEBPS/chapter_1.xhtml" in names
        assert "OEBPS/chapter_2.xhtml" in names
        opf = zf.read("OEBPS/content.opf").decode("utf-8")
        assert "Sách Thử Nghiệm" in opf
        assert "<dc:language>vi</dc:language>" in opf


def test_extract_gdrive_id() -> None:
    assert (
        extract_gdrive_id("https://drive.google.com/file/d/1AbCdEfGhIjKlMnOpQrStUvWxYz012345/view?usp=sharing")
        == "1AbCdEfGhIjKlMnOpQrStUvWxYz012345"
    )
    assert (
        extract_gdrive_id("https://drive.google.com/open?id=1AbCdEfGhIjKlMnOpQrStUvWxYz012345")
        == "1AbCdEfGhIjKlMnOpQrStUvWxYz012345"
    )
    assert (
        extract_gdrive_id("https://drive.google.com/uc?export=download&id=1AbCdEfGhIjKlMnOpQrStUvWxYz012345")
        == "1AbCdEfGhIjKlMnOpQrStUvWxYz012345"
    )
    assert extract_gdrive_id("https://example.com/file.pdf") is None


def test_validate_file_header_pdf_and_reject_html(tmp_path: Path) -> None:
    valid_pdf = tmp_path / "good.pdf"
    valid_pdf.write_bytes(b"%PDF-1.7\n" + b"0" * 2048)
    assert validate_file_header(valid_pdf, "pdf") is True
    assert valid_pdf.exists()

    bad_pdf = tmp_path / "bad.pdf"
    bad_pdf.write_bytes(b"<!DOCTYPE html><html>Login required</html>" + b" " * 2048)
    assert validate_file_header(bad_pdf, "pdf") is False
    assert not bad_pdf.exists()  # Must delete invalid file

    bad_epub = tmp_path / "bad.epub"
    bad_epub.write_bytes(b"<html>Not a zip</html>" + b" " * 2048)
    assert validate_file_header(bad_epub, "epub") is False
    assert not bad_epub.exists()


@pytest.mark.asyncio
async def test_catalog_db_crud_and_dedup(tmp_path: Path) -> None:
    db = CatalogDB(tmp_path / "catalog.db")
    try:
        inserted1 = await db.upsert_discovered_book(
            "https://example.com/sach/1", "sach_gallery", category="Văn học"
        )
        inserted2 = await db.upsert_discovered_book(
            "https://example.com/sach/1", "sach_gallery", category="Văn học"
        )
        assert inserted1 is True
        assert inserted2 is False

        pending = await db.get_pending_books("sach_gallery")
        assert len(pending) == 1
        assert pending[0]["url"] == "https://example.com/sach/1"

        await db.mark_processing("https://example.com/sach/1")
        await db.update_book_result(
            "https://example.com/sach/1",
            status="completed",
            title="Sách 1",
            author="Tác giả A",
            epub_url="https://example.com/tai/1",
            epub_path=str(tmp_path / "1.epub"),
        )

        pending_after = await db.get_pending_books("sach_gallery")
        assert len(pending_after) == 0

        stats = await db.get_summary_stats()
        assert stats["sach_gallery"]["total"] == 1
        assert stats["sach_gallery"]["completed"] == 1
        assert stats["sach_gallery"]["epubs"] == 1
    finally:
        db.close()


def test_spider_url_filters_and_link_extraction(tmp_path: Path) -> None:
    cfg = CrawlerConfig(output_dir=tmp_path / "dl", db_path=tmp_path / "cat.db")
    db = CatalogDB(cfg.db_path)
    try:
        sg = SachGallerySpider(cfg, db)
        assert sg.is_book_url("https://www.sach.gallery/sach/tan-bien") is True
        assert sg.is_book_url("https://www.sach.gallery/the-loai/tieu-thuyet") is False
        assert sg.is_category_url("https://www.sach.gallery/the-loai/tieu-thuyet") is True

        tvs = ThuVienSachSpider(cfg, db)
        assert tvs.is_book_url("https://thuviensach.vn/dat-rung-phuong-nam-1234.html") is True

        ms = MetaisachSpider(cfg, db)
        assert ms.name == "metaisach"
        assert ms.is_book_url("https://metaisach.com/3-sai-lam-lanh-dao") is True
        assert ms.is_book_url("https://metaisach.com/author/abc") is False

        sho = SachHayOnlineSpider(cfg, db)
        assert sho.is_book_url("https://www.sachhayonline.com/tua-sach/viet-nam-su-luoc") is True
        assert sho.is_book_url("https://www.sachhayonline.com/tua-sach/viet-nam-su-luoc/chuong-1/278") is False

        html_sample = """
        <html><body>
          <a href="/tai/10325" class="btn">Tải EPUB · 2.0 MB</a>
          <a href="https://media.metaisach.com/2025/04/sample.pdf">Đọc PDF</a>
        </body></html>
        """
        soup = BeautifulSoup(html_sample, "lxml")
        extracted = BaseEbookSpider(cfg, db).extract_ebook_links(
            soup, "https://www.sach.gallery/sach/test", html_sample
        )
        assert extracted["epub"] == "https://www.sach.gallery/tai/10325"
        assert extracted["pdf"] == "https://media.metaisach.com/2025/04/sample.pdf"

        assert is_captcha_or_blocked(403, "Just a moment... cf-challenge") is True
        assert is_captcha_or_blocked(200, "<html>cf-turnstile</html>") is True
        assert is_captcha_or_blocked(200, "<html>" + "a" * 60000 + "turnstile</html>") is False
        assert is_captcha_or_blocked(200, "<html>Normal book page</html>") is False
    finally:
        db.close()


def test_katana_build_command(tmp_path: Path) -> None:
    fake_bin = tmp_path / "katana.exe"
    fake_bin.write_bytes(b"MZ")
    kd = KatanaDiscovery(fake_bin)
    assert kd.is_available is True

    cmd = kd.build_command(
        "https://www.sach.gallery",
        depth=2,
        js_crawl=True,
        known_files=True,
        headless=True,
        delay=3,
    )
    assert cmd[0] == str(fake_bin)
    assert "-u" in cmd and "https://www.sach.gallery" in cmd
    assert "-d" in cmd and "2" in cmd
    assert "-jc" in cmd
    assert "-kf" in cmd and "all" in cmd
    assert "-hl" in cmd and "-nos" in cmd
    assert "-delay" in cmd and "3" in cmd


@pytest.mark.asyncio
async def test_katana_stream_discovered_urls(tmp_path: Path, monkeypatch) -> None:
    fake_bin = tmp_path / "katana.exe"
    fake_bin.write_bytes(b"MZ")
    kd = KatanaDiscovery(fake_bin)

    class _FakeStdout:
        def __init__(self, lines):
            self._lines = list(lines)

        async def readline(self) -> bytes:
            return self._lines.pop(0) if self._lines else b""

    class _FakeProc:
        def __init__(self):
            self.stdout = _FakeStdout(
                [
                    b"[INF] banner\n",
                    b"https://www.sach.gallery/sach/book-1\n",
                    b"https://www.sach.gallery/sach/book-2\n",
                    b"https://www.sach.gallery/sach/book-3\n",
                ]
            )
            self.returncode = None
            self.terminated = False

        def terminate(self):
            self.terminated = True
            self.returncode = 0

        async def wait(self):
            self.returncode = 0
            return 0

    fake_proc = _FakeProc()

    async def _fake_exec(*args, **kwargs):
        return fake_proc

    monkeypatch.setattr(asyncio, "create_subprocess_exec", _fake_exec)

    urls = [
        u
        async for u in kd.stream_discovered_urls(
            "https://www.sach.gallery", max_urls=2
        )
    ]
    assert urls == [
        "https://www.sach.gallery/sach/book-1",
        "https://www.sach.gallery/sach/book-2",
    ]
    assert fake_proc.terminated is True


@pytest.mark.asyncio
async def test_stop_and_resume_skips_completed_and_redownloads_deleted(tmp_path: Path) -> None:
    db_path = tmp_path / "catalog.db"
    book_file = tmp_path / "Books" / "sach_gallery" / "cat" / "done.epub"
    book_file.parent.mkdir(parents=True, exist_ok=True)
    book_file.write_bytes(b"PK\x03\x04" + b"0" * 1200)

    db = CatalogDB(db_path)
    try:
        done_url = "https://www.sach.gallery/sach/done-book"
        interrupted_url = "https://www.sach.gallery/sach/interrupted-book"
        failed_url = "https://www.sach.gallery/sach/failed-book"

        await db.upsert_discovered_book(done_url, "sach_gallery", category="van-hoc")
        await db.update_book_result(
            done_url,
            status="completed",
            title="Sách Đã Tải",
            epub_path=str(book_file),
        )

        await db.upsert_discovered_book(interrupted_url, "sach_gallery", category="van-hoc")
        await db.mark_processing(interrupted_url)

        await db.upsert_discovered_book(failed_url, "sach_gallery", category="van-hoc")
        await db.update_book_result(failed_url, status="failed", error="timeout")

        # Export checkpoint JSON
        ckpt_path = tmp_path / "Books" / "crawl_progress.json"
        ckpt = await db.export_progress_checkpoint(ckpt_path)
        assert ckpt_path.exists()
        parsed = json.loads(ckpt_path.read_text(encoding="utf-8"))
        assert parsed["pending_total"] == 1
        assert len(parsed["completed_books"]) == 1
        assert parsed["completed_books"][0]["url"] == done_url
        assert ckpt["summary_by_source"]["sach_gallery"]["completed"] == 1
    finally:
        db.close()

    # Re-open DB (simulating resume after stop/Ctrl+C): 'processing' resets to 'discovered'
    cfg = CrawlerConfig(output_dir=tmp_path / "Books", db_path=db_path, max_books_per_source=2)
    db2 = CatalogDB(db_path)
    try:
        pending = await db2.get_pending_books("sach_gallery")
        assert [r["url"] for r in pending] == [interrupted_url]

        spider = SachGallerySpider(cfg, db2)
        queue: asyncio.Queue = asyncio.Queue()
        queued_set = set()

        # 1. Completed book whose file exists on disk must be skipped
        assert await db2.is_book_already_done(done_url) is True
        added_done = await spider._enqueue_book(done_url, queue, queued_set=queued_set)
        assert added_done is False
        assert spider.skipped_count == 1
        assert queue.empty()

        # 2. Failed book is skipped until reset_failed_books() is called
        assert await db2.is_book_already_done(failed_url) is True
        assert await db2.reset_failed_books("sach_gallery") == 1
        assert await db2.is_book_already_done(failed_url) is False

        # 3. Deleting the file on disk makes completed book eligible for re-download
        book_file.unlink()
        assert await db2.is_book_already_done(done_url) is False
        queued_set.clear()
        added_again = await spider._enqueue_book(done_url, queue, queued_set=queued_set)
        assert added_again is True
        assert queue.qsize() == 1
    finally:
        db2.close()


@pytest.mark.asyncio
async def test_process_book_prefers_epub_over_pdf(tmp_path: Path) -> None:
    cfg = CrawlerConfig(
        output_dir=tmp_path / "Books",
        db_path=tmp_path / "catalog.db",
        formats={"epub", "pdf"},
    )
    db = CatalogDB(cfg.db_path)
    try:
        spider = BaseEbookSpider(cfg, db)
        sem = asyncio.Semaphore(2)

        class _FakeDownloader:
            def __init__(self, fail_epub: bool = False) -> None:
                self.calls = []
                self.fail_epub = fail_epub

            async def download_book_file(
                self, client, url: str, dest: Path, expected_type: str, referer=None
            ) -> bool:
                self.calls.append((expected_type, url, dest))
                if expected_type == "epub" and self.fail_epub:
                    return False
                dest.parent.mkdir(parents=True, exist_ok=True)
                header = b"PK\x03\x04" if expected_type == "epub" else b"%PDF-1.7\n"
                dest.write_bytes(header + b"0" * 1200)
                return True

        # Case 1: Both EPUB & PDF exist -> download only EPUB, skip PDF
        u1 = "https://example.com/sach/both-formats"
        await db.upsert_discovered_book(u1, "base", category="van-hoc")
        spider.parse_book = AsyncMock(
            return_value=BookData(
                url=u1,
                title="Sách Có Cả Hai",
                category="van-hoc",
                epub_url="https://example.com/both.epub",
                pdf_url="https://example.com/both.pdf",
            )
        )
        dl_ok = _FakeDownloader(fail_epub=False)
        await spider.process_book(None, dl_ok, sem, u1, "van-hoc")

        assert [c[0] for c in dl_ok.calls] == ["epub"]
        assert spider.epub_count == 1
        assert spider.pdf_count == 0
        row1 = db._conn.execute("SELECT status, epub_path, pdf_path FROM books WHERE url = ?", (u1,)).fetchone()
        assert row1["status"] == "completed"
        assert row1["epub_path"] is not None and Path(row1["epub_path"]).exists()
        assert row1["pdf_path"] is None

        # Case 2: EPUB fails -> automatically fall back to PDF
        u2 = "https://example.com/sach/epub-broken"
        await db.upsert_discovered_book(u2, "base", category="van-hoc")
        spider.parse_book = AsyncMock(
            return_value=BookData(
                url=u2,
                title="Sách Hỏng EPUB",
                category="van-hoc",
                epub_url="https://example.com/broken.epub",
                pdf_url="https://example.com/fallback.pdf",
            )
        )
        dl_fb = _FakeDownloader(fail_epub=True)
        await spider.process_book(None, dl_fb, sem, u2, "van-hoc")

        assert [c[0] for c in dl_fb.calls] == ["epub", "pdf"]
        assert spider.pdf_count == 1
        row2 = db._conn.execute("SELECT status, epub_path, pdf_path FROM books WHERE url = ?", (u2,)).fetchone()
        assert row2["status"] == "completed"
        assert row2["epub_path"] is None
        assert row2["pdf_path"] is not None and Path(row2["pdf_path"]).exists()
    finally:
        db.close()


@pytest.mark.asyncio
async def test_active_sources_and_retry_backoff(tmp_path: Path, monkeypatch) -> None:
    import httpx

    assert resolve_sources("all") == ACTIVE_SOURCES
    assert set(resolve_sources("all")) == {
        "sach_gallery",
        "metaisach",
        "sachhayonline",
    }
    assert resolve_sources("thuviensach") == ["thuviensach"]
    assert resolve_sources("isach") == ["isach"]

    resp_429 = httpx.Response(429, headers={"Retry-After": "1"}, text="rate limit")
    wait = _parse_retry_after(resp_429, default_wait=2.0)
    assert 1.0 <= wait <= 2.0

    resp_capped = httpx.Response(429, headers={"Retry-After": "999"})
    assert 30.0 <= _parse_retry_after(resp_capped, default_wait=2.0) <= 31.0

    sleep_calls = []

    async def _fast_sleep(sec: float) -> None:
        sleep_calls.append(sec)

    monkeypatch.setattr(asyncio, "sleep", _fast_sleep)

    cfg = CrawlerConfig(output_dir=tmp_path / "dl", db_path=tmp_path / "cat.db", request_delay=0.0)
    db = CatalogDB(cfg.db_path)
    try:
        spider = BaseEbookSpider(cfg, db)
        calls = 0

        async def _mock_get(url, **kwargs):
            nonlocal calls
            calls += 1
            if calls == 1:
                return httpx.Response(
                    429,
                    headers={"Retry-After": "1"},
                    text="Too many requests",
                    request=httpx.Request("GET", url),
                )
            return httpx.Response(
                200,
                text="<html><body>OK</body></html>",
                request=httpx.Request("GET", url),
            )

        class _FakeClient:
            get = staticmethod(_mock_get)

        res = await spider.fetch_html(_FakeClient(), "https://example.com/book", retries=2)
        assert res.status_code == 200
        assert calls == 2
        assert any(s >= 1.0 for s in sleep_calls)
        assert spider.adaptive_delay >= 0.4 - 1e-6  # +0.5 on 429, -0.1 on 200 OK
    finally:
        db.close()


@pytest.mark.asyncio
async def test_gdrive_uploader_and_book_sync(tmp_path: Path, monkeypatch) -> None:
    fake_bin = tmp_path / "gdrive.cmd"
    fake_bin.write_text("@echo off\n", encoding="utf-8")

    cfg = CrawlerConfig(
        output_dir=tmp_path / "Books",
        db_path=tmp_path / "catalog.db",
        formats={"epub", "pdf"},
        use_gdrive=True,
        gdrive_root_folder="Books",
        delete_local_after_upload=True,
    )
    db = CatalogDB(cfg.db_path)
    try:
        uploader = GDriveUploader(db, bin_path=fake_bin)
        assert uploader.is_available is True

        cli_calls = []

        async def _mock_cli(*args, **kwargs):
            cli_calls.append(args)
            if args[:2] == ("readonly", "search"):
                return []
            if args[:2] == ("mutate", "mkdir"):
                folder_name = args[2]
                return {"id": f"fld_{folder_name}", "name": folder_name}
            if args[:2] == ("mutate", "upload"):
                return {"id": "file_12345", "name": Path(args[2]).name}
            if args[:2] == ("mutate", "update"):
                return {"id": args[2], "name": Path(args[3]).name}
            return None

        monkeypatch.setattr(uploader, "_run_cli", _mock_cli)

        # Ensure pre-existing unsynced query works
        unsynced_url = "https://example.com/sach/local-only"
        local_pdf = tmp_path / "Books" / "metaisach" / "cat" / "local.pdf"
        local_pdf.parent.mkdir(parents=True, exist_ok=True)
        local_pdf.write_bytes(b"%PDF-1.7\n" + b"0" * 1200)
        await db.upsert_discovered_book(unsynced_url, "metaisach", category="cat")
        await db.update_book_result(
            unsynced_url,
            status="completed",
            title="Sách Cục Bộ",
            pdf_path=str(local_pdf),
        )
        unsynced = await db.get_unsynced_completed_books()
        assert [r["url"] for r in unsynced] == [unsynced_url]

        # Test process_book auto-uploading to Google Drive & cleaning up local file
        spider = BaseEbookSpider(cfg, db)
        book_url = "https://example.com/sach/cloud-book"
        await db.upsert_discovered_book(book_url, "base", category="van-hoc")
        spider.parse_book = AsyncMock(
            return_value=BookData(
                url=book_url,
                title="Sách Lên Drive",
                category="van-hoc",
                epub_url="https://example.com/cloud-book.epub",
            )
        )

        class _FakeDownloader:
            async def download_book_file(
                self, client, url: str, dest: Path, expected_type: str, referer=None
            ) -> bool:
                dest.parent.mkdir(parents=True, exist_ok=True)
                dest.write_bytes(b"PK\x03\x04" + b"0" * 1200)
                return True

        sem = asyncio.Semaphore(1)
        await spider.process_book(
            None, _FakeDownloader(), sem, book_url, "van-hoc", gdrive=uploader
        )

        assert spider.epub_count == 1
        assert spider.gdrive_count == 1

        # Verify folder hierarchy cache in DB
        assert await db.get_gdrive_folder_id("root/Books") == "fld_Books"
        assert await db.get_gdrive_folder_id("fld_Books/base") == "fld_base"
        assert await db.get_gdrive_folder_id("fld_base/van-hoc") == "fld_van-hoc"

        # Verify DB row stores gdrive_id/url, clears local paths, and keeps epub count via file_format
        row = db._conn.execute(
            "SELECT epub_path, pdf_path, file_format, gdrive_id, gdrive_url FROM books WHERE url = ?",
            (book_url,),
        ).fetchone()
        assert row["gdrive_id"] == "file_12345"
        assert row["gdrive_url"] == gdrive_file_url("file_12345")
        assert row["file_format"] == "epub"
        assert row["epub_path"] is None
        assert row["pdf_path"] is None
        assert not (tmp_path / "Books" / "base").exists()

        # Verify is_book_already_done remains True when gdrive_id exists despite no local file
        assert await db.is_book_already_done(book_url) is True

        stats = await db.get_summary_stats()
        assert stats["base"]["epubs"] == 1
        assert stats["base"]["gdrive_count"] == 1
        assert stats["metaisach"]["gdrive_count"] == 0
    finally:
        db.close()


@pytest.mark.asyncio
async def test_resume_cursor_and_rate_limit_pause(tmp_path: Path) -> None:
    import httpx
    from spiders.base import FetchResult

    cfg = CrawlerConfig(
        output_dir=tmp_path / "staging",
        db_path=tmp_path / "catalog.db",
        max_books_per_source=1,
        request_delay=0.0,
        use_gdrive=False,
    )
    db = CatalogDB(cfg.db_path)
    try:
        # 1. Verify FIFO order is preserved even if an earlier book had mark_processing called
        for i in range(1, 4):
            await db.upsert_discovered_book(f"https://www.sach.gallery/sach/fifo-{i}", "sach_gallery")
        await db.mark_processing("https://www.sach.gallery/sach/fifo-1")
        await db.reset_interrupted_books()
        fifo = await db.get_pending_books("sach_gallery")
        assert [r["url"] for r in fifo[:3]] == [
            "https://www.sach.gallery/sach/fifo-1",
            "https://www.sach.gallery/sach/fifo-2",
            "https://www.sach.gallery/sach/fifo-3",
        ]

        # 2. Simulate sitemap resume cursor stopping mid-sitemap and continuing from exact offset
        locs = "\n".join(
            f"<url><loc>https://www.sach.gallery/sach/sm-{i:02d}</loc></url>"
            for i in range(1, 26)
        )
        sm_xml = f"<urlset>{locs}</urlset>"

        sp1 = SachGallerySpider(cfg, db)
        sp1.sitemap_paths = ["/sitemap.xml"]
        sp1.fetch_html = AsyncMock(
            return_value=FetchResult(
                url="https://www.sach.gallery/sitemap.xml",
                status_code=200,
                text=sm_xml,
            )
        )
        q1: asyncio.Queue = asyncio.Queue()
        queued1 = set()
        # With 3 FIFO items already counted, discovered_count starts at 3, cap is 16 -> processes 13 items
        sp1.discovered_count = 3
        await sp1._scan_sitemap(
            None, "https://www.sach.gallery", q1, queued1, [], set()
        )
        entry_after_1 = await db.get_discovery_entry("https://www.sach.gallery/sitemap.xml")
        assert entry_after_1 is not None
        assert entry_after_1["status"] == "pending"
        assert entry_after_1["last_offset"] == 13
        assert q1.qsize() == 13

        # Second spider resumes directly at offset 13 and reaches end (25)
        cfg2 = CrawlerConfig(
            output_dir=tmp_path / "staging",
            db_path=cfg.db_path,
            max_books_per_source=10,
            request_delay=0.0,
            use_gdrive=False,
        )
        sp2 = SachGallerySpider(cfg2, db)
        sp2.sitemap_paths = ["/sitemap.xml"]
        sp2.fetch_html = AsyncMock(
            return_value=FetchResult(
                url="https://www.sach.gallery/sitemap.xml",
                status_code=200,
                text=sm_xml,
            )
        )
        q2: asyncio.Queue = asyncio.Queue()
        await sp2._scan_sitemap(
            None, "https://www.sach.gallery", q2, set(), [], set()
        )
        entry_after_2 = await db.get_discovery_entry("https://www.sach.gallery/sitemap.xml")
        assert entry_after_2["status"] == "done"
        assert entry_after_2["last_offset"] == 25
        # Only remaining 12 items (14..25) were queued, NOT re-scanning 1..13!
        assert q2.qsize() == 12
        assert sp2.skipped_count == 0

        # 3. Verify HTTP 429 daily quota keeps book pending (not failed) and sets rate_limited_stop
        rl_url = "https://www.sach.gallery/sach/fifo-1"
        sp2.parse_book = AsyncMock(
            return_value=BookData(
                url=rl_url,
                title="Sách Chạm Hạn Mức",
                category="van-hoc",
                epub_url="https://www.sach.gallery/tai/99",
            )
        )

        class _RateLimitDownloader(FileDownloader):
            async def resolve_download_url(self, client, url: str):
                return url

        class _Mock429Stream:
            def __init__(self, url: str) -> None:
                self.status_code = 429
                self.headers = {"content-type": "text/html; charset=utf-8"}
                self.url = url

            async def __aenter__(self):
                return self

            async def __aexit__(self, *args):
                return False

        class _Mock429Client:
            def stream(self, method: str, url: str, **kwargs):
                return _Mock429Stream(url)

        await sp2.process_book(
            _Mock429Client(), _RateLimitDownloader(), asyncio.Semaphore(1), rl_url, "van-hoc"
        )
        assert sp2.rate_limited_stop is True
        assert sp2.failed_count == 0
        row_rl = db._conn.execute(
            "SELECT status, error FROM books WHERE url = ?", (rl_url,)
        ).fetchone()
        assert row_rl["status"] == "discovered"
        assert row_rl["error"] == "rate_limited_daily"

        ckpt = await db.export_progress_checkpoint(tmp_path / "crawl_progress.json")
        assert "sach_gallery" in ckpt["resume_cursors"]
        assert ckpt["resume_cursors"]["sach_gallery"]["next_pending_urls"][0] == rl_url
    finally:
        db.close()


def test_sachhayonline_doc_online_and_direct_pdf(tmp_path):
    import asyncio
    from config import CrawlerConfig
    from storage import CatalogDB
    from spiders.base import FetchResult
    from spiders.chapter_sites import SachHayOnlineSpider

    book_doc_url = "https://sachhayonline.com/tua-sach/sach-moi"
    chap_0_url = "https://sachhayonline.com/doc-online/dan-nhap/100"
    chap_1_url = "https://sachhayonline.com/doc-online/chuong-1/133330"
    chap_6_url = "https://sachhayonline.com/doc-online/chuong-6/132562"
    book_pdf_url = "https://sachhayonline.com/tua-sach/sach-pdf-moi"

    pages = {
        book_doc_url: """
        <html><body>
          <h1>Sách Mới Doc Online</h1>
          <a href="/doc-online/chuong-1/133330">⭐ Đọc Online</a>
          <ul class="toc">
            <li><a href="/doc-online/dan-nhap/100">Dẫn Nhập</a></li>
            <li><a href="/doc-online/chuong-1/133330">Chương 1</a></li>
            <li><a href="/doc-online/chuong-6/132562">Chương 6</a></li>
          </ul>
        </body></html>
        """,
        chap_0_url: '<html><body><div class="chapter-content"><p>Nội dung dẫn nhập đầy đủ để kiểm tra.</p></div></body></html>',
        chap_1_url: '<html><body><div class="chapter-content"><p>Nội dung chương một đầy đủ để kiểm tra.</p></div></body></html>',
        chap_6_url: '<html><body><div class="chapter-content"><p>Nội dung chương sáu đầy đủ để kiểm tra.</p></div></body></html>',
        book_pdf_url: """
        <html><body>
          <h1>Sách PDF Mới</h1>
          <a href="/uploads/books/pdf/test-123.pdf">PDF</a>
        </body></html>
        """,
    }

    class FakeClient:
        async def get(self, url, **kwargs):
            return FetchResult(url=url, status_code=200, text=pages[url])

        async def fetch(self, url, **kwargs):
            return FetchResult(url=url, status_code=200, text=pages[url])

    db = CatalogDB(tmp_path / "catalog.db")
    try:
        cfg = CrawlerConfig(output_dir=tmp_path)
        spider = SachHayOnlineSpider(cfg, db)
        client = FakeClient()

        bd1 = asyncio.run(spider.parse_book(client, book_doc_url, "Văn học"))
        assert len(bd1.chapters) == 3
        assert [title for title, _ in bd1.chapters] == ["Dẫn Nhập", "Chương 1", "Chương 6"]
        assert "Nội dung dẫn nhập" in bd1.chapters[0][1]
        assert "Nội dung chương một" in bd1.chapters[1][1]
        assert "Nội dung chương sáu" in bd1.chapters[2][1]

        bd2 = asyncio.run(spider.parse_book(client, book_pdf_url, "Triết học"))
        assert bd2.pdf_url == "https://sachhayonline.com/uploads/books/pdf/test-123.pdf"
    finally:
        db.close()
