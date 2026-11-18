import asyncio
import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import httpx
from rich.console import Console
from rich.live import Live
from rich.table import Table

from config import DEFAULT_HEADERS, CrawlerConfig
from gdrive_uploader import GDriveUploader, gdrive_file_url
from resolvers import FileDownloader
from spiders import SPIDER_REGISTRY, BaseEbookSpider
from spiders.base import slugify_filename
from storage import CatalogDB


class ParallelCrawlEngine:
    """Runs multiple ebook spiders concurrently with per-source producer/consumer pools."""

    def __init__(self, config: CrawlerConfig, source_names: List[str]) -> None:
        self.config = config
        self.source_names = source_names
        self.db = CatalogDB(config.db_path)
        self.downloader = FileDownloader()
        self.gdrive = GDriveUploader(
            self.db, root_folder_id=config.gdrive_root_id
        )
        self.download_sem = asyncio.Semaphore(config.download_concurrency)
        self.console = Console()
        self.spiders: List[BaseEbookSpider] = [
            SPIDER_REGISTRY[name](config, self.db)
            for name in source_names
            if name in SPIDER_REGISTRY
        ]

    def build_status_table(self) -> Table:
        table = Table(title="VN Ebook Crawler - Trạng thái luồng song song")
        table.add_column("Nguồn", style="cyan", no_wrap=True)
        table.add_column("Đã tìm", justify="right")
        table.add_column("Đã có (Skip)", justify="right", style="dim cyan")
        table.add_column("Đã xử lý", justify="right")
        table.add_column("EPUB", justify="right", style="green")
        table.add_column("PDF", justify="right", style="blue")
        table.add_column("GDrive", justify="right", style="bright_green")
        table.add_column("Lỗi/Chặn", justify="right", style="red")
        table.add_column("Trạng thái", style="yellow")

        for sp in self.spiders:
            table.add_row(
                sp.name,
                str(sp.discovered_count),
                str(sp.skipped_count),
                str(sp.processed_count),
                str(sp.epub_count),
                str(sp.pdf_count),
                str(sp.gdrive_count),
                str(sp.failed_count),
                sp.status_message,
            )
        return table

    async def _run_spider_pipeline(
        self, spider: BaseEbookSpider, client: httpx.AsyncClient
    ) -> None:
        queue: asyncio.Queue[Optional[Tuple[str, str]]] = asyncio.Queue()
        worker_count = max(1, self.config.workers_per_source)
        # Polite single-worker limit for crawl-delay heavy sites like isach
        if spider.crawl_delay >= 2.0:
            worker_count = 1

        completed_lock = asyncio.Lock()
        stop_event = asyncio.Event()

        async def producer() -> None:
            try:
                await spider.discover_books(client, queue)
            except Exception as exc:
                spider.status_message = f"Lỗi quét: {type(exc).__name__}"
            finally:
                for _ in range(worker_count):
                    await queue.put(None)

        async def consumer() -> None:
            while True:
                item = await queue.get()
                try:
                    if item is None:
                        return
                    if stop_event.is_set():
                        continue

                    book_url, category = item
                    if self.config.max_books_per_source > 0:
                        async with completed_lock:
                            saved_so_far = spider.epub_count + spider.pdf_count
                            if saved_so_far >= self.config.max_books_per_source:
                                stop_event.set()
                                continue

                    await spider.process_book(
                        client,
                        self.downloader,
                        self.download_sem,
                        book_url,
                        category,
                        gdrive=self.gdrive if self.config.use_gdrive else None,
                    )

                    if self.config.max_books_per_source > 0:
                        async with completed_lock:
                            saved_so_far = spider.epub_count + spider.pdf_count
                            if saved_so_far >= self.config.max_books_per_source:
                                stop_event.set()
                finally:
                    queue.task_done()

        await asyncio.gather(
            producer(),
            *(consumer() for _ in range(worker_count)),
        )

        if not spider.status_message.startswith("Bị chặn") and not spider.status_message.startswith("Không thể"):
            spider.status_message = "Hoàn tất"

    async def _upload_checkpoint_to_gdrive(self, ckpt_path: Path) -> None:
        if not (self.config.use_gdrive and self.gdrive.is_available and ckpt_path.exists()):
            return
        try:
            root_id = await self.gdrive.ensure_root_folder(self.config.gdrive_root_folder)
            if root_id:
                fid = await self.gdrive.upload_file(ckpt_path, root_id)
                if fid and self.config.delete_local_after_upload:
                    ckpt_path.unlink(missing_ok=True)
                    try:
                        self.config.output_dir.rmdir()
                    except OSError:
                        pass
        except Exception:
            pass

    async def sync_existing_books_to_gdrive(self) -> int:
        """Upload all previously downloaded local books in catalog.db that lack gdrive_id."""
        self.config.output_dir.mkdir(parents=True, exist_ok=True)
        uploaded_count = 0
        try:
            if not self.config.use_gdrive:
                self.console.print("[yellow]Google Drive đang tắt (--no-gdrive).[/yellow]")
                return 0
            if not self.gdrive.is_available:
                self.console.print(
                    "[red]Không tìm thấy công cụ gdrive CLI. Hãy kiểm tra plugin Google Drive.[/red]"
                )
                return 0

            root_id = await self.gdrive.ensure_root_folder(self.config.gdrive_root_folder)
            if not root_id:
                self.console.print("[red]Không thể tạo/mở thư mục gốc trên Google Drive.[/red]")
                return 0

            unsynced = await self.db.get_unsynced_completed_books(
                sources=self.source_names or None
            )
            if not unsynced:
                self.console.print(
                    "[green]Tất cả sách đã tải trong thư viện đều đã được đồng bộ lên Google Drive.[/green]"
                )
                ckpt_path = self.config.output_dir / "crawl_progress.json"
                await self.db.export_progress_checkpoint(ckpt_path)
                await self._upload_checkpoint_to_gdrive(ckpt_path)
                return 0

            self.console.print(
                f"[cyan]Đang đồng bộ {len(unsynced)} đầu sách hiện có lên Google Drive "
                f"(thư mục '{self.config.gdrive_root_folder}')...[/cyan]"
            )

            count_lock = asyncio.Lock()

            async def _sync_one(row: Dict[str, Any]) -> None:
                nonlocal uploaded_count
                primary_str = row.get("epub_path") or row.get("pdf_path")
                if not primary_str:
                    return
                primary_path = Path(primary_str)
                if not primary_path.exists():
                    return

                source = row["source"]
                file_fmt = row.get("file_format") or (
                    "epub" if row.get("epub_path") else "pdf"
                )
                cat_slug = (
                    primary_path.parent.name
                    if primary_path.parent.parent.name == source
                    else slugify_filename(row.get("category") or "tong-hop", max_len=40)
                )
                try:
                    cat_folder_id = await self.gdrive.ensure_category_folder(
                        self.config.gdrive_root_folder, source, cat_slug
                    )
                    if not cat_folder_id:
                        return
                    gdrive_id = await self.gdrive.upload_file(primary_path, cat_folder_id)
                    if not gdrive_id:
                        return
                    gdrive_url = gdrive_file_url(gdrive_id)
                    meta_path = primary_path.with_suffix(".meta.json")
                    cleared_local = False
                    if self.config.delete_local_after_upload:
                        primary_path.unlink(missing_ok=True)
                        meta_path.unlink(missing_ok=True)
                        for empty_dir in (primary_path.parent, primary_path.parent.parent):
                            try:
                                empty_dir.rmdir()
                            except OSError:
                                pass
                        cleared_local = True
                    elif meta_path.exists():
                        try:
                            meta = json.loads(meta_path.read_text(encoding="utf-8"))
                            meta["gdrive_id"] = gdrive_id
                            meta["gdrive_url"] = gdrive_url
                            meta_path.write_text(
                                json.dumps(meta, ensure_ascii=False, indent=2),
                                encoding="utf-8",
                            )
                        except Exception:
                            pass

                    await self.db.update_book_result(
                        row["url"],
                        status=row["status"],
                        file_format=file_fmt,
                        gdrive_id=gdrive_id,
                        gdrive_url=gdrive_url,
                        clear_local_paths=cleared_local,
                    )
                    async with count_lock:
                        uploaded_count += 1
                        if uploaded_count % 10 == 0 or uploaded_count == len(unsynced):
                            self.console.print(
                                f"  [green]Đã đẩy {uploaded_count}/{len(unsynced)} sách lên Google Drive...[/green]"
                            )
                except Exception as exc:
                    self.console.print(
                        f"[yellow]Lỗi upload {primary_path.name}: {exc}[/yellow]"
                    )

            await asyncio.gather(*(_sync_one(r) for r in unsynced))

            ckpt_path = self.config.output_dir / "crawl_progress.json"
            await self.db.export_progress_checkpoint(ckpt_path)
            await self._upload_checkpoint_to_gdrive(ckpt_path)
            self.console.print(
                f"[green]Đã đồng bộ thành công {uploaded_count}/{len(unsynced)} sách lên Google Drive![/green]"
            )
            return uploaded_count
        finally:
            self.db.close()

    async def run(
        self,
        *,
        retry_failed: bool = False,
        reset_discovery: bool = False,
    ) -> None:
        self.config.output_dir.mkdir(parents=True, exist_ok=True)
        limits = httpx.Limits(max_connections=80, max_keepalive_connections=30)

        try:
            await self.db.reset_interrupted_books()
            if retry_failed:
                for src in self.source_names:
                    await self.db.reset_failed_books(src)
            if reset_discovery:
                for src in self.source_names:
                    await self.db.clear_visited_pages(src)

            if self.config.use_gdrive:
                if self.gdrive.is_available:
                    await self.gdrive.ensure_root_folder(self.config.gdrive_root_folder)
                else:
                    self.console.print(
                        "[yellow]Cảnh báo: Không tìm thấy gdrive CLI, sách tạm thời chỉ lưu ở thư mục cục bộ.[/yellow]"
                    )

            async with httpx.AsyncClient(
                headers=DEFAULT_HEADERS,
                follow_redirects=True,
                timeout=self.config.request_timeout,
                limits=limits,
            ) as client:
                stop_refresh = asyncio.Event()

                async def dashboard_loop(live: Optional[Live]) -> None:
                    while not stop_refresh.is_set():
                        if live is not None:
                            live.update(self.build_status_table())
                        await asyncio.sleep(0.5)

                is_tty = sys.stdout.isatty()
                if is_tty:
                    with Live(
                        self.build_status_table(),
                        console=self.console,
                        refresh_per_second=2,
                    ) as live:
                        dash_task = asyncio.create_task(dashboard_loop(live))
                        try:
                            await asyncio.gather(
                                *(
                                    self._run_spider_pipeline(sp, client)
                                    for sp in self.spiders
                                )
                            )
                        finally:
                            stop_refresh.set()
                            await dash_task
                        live.update(self.build_status_table())
                else:
                    await asyncio.gather(
                        *(self._run_spider_pipeline(sp, client) for sp in self.spiders)
                    )
                    self.console.print(self.build_status_table())
        except (asyncio.CancelledError, KeyboardInterrupt):
            for sp in self.spiders:
                if sp.status_message not in ("Hoàn tất", "Không thể kết nối") and not sp.status_message.startswith("Bị chặn"):
                    sp.status_message = "Đã tạm dừng (Lưu tiến trình)"
            raise
        finally:
            try:
                await self.db.reset_interrupted_books()
                ckpt_path = self.config.output_dir / "crawl_progress.json"
                await self.db.export_progress_checkpoint(ckpt_path)
                await self._upload_checkpoint_to_gdrive(ckpt_path)
            finally:
                self.db.close()
