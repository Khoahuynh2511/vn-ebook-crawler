import asyncio
import sys
from typing import List, Optional, Tuple

import httpx
from rich.console import Console
from rich.live import Live
from rich.table import Table

from config import DEFAULT_HEADERS, CrawlerConfig
from resolvers import FileDownloader
from spiders import SPIDER_REGISTRY, BaseEbookSpider
from storage import CatalogDB


class ParallelCrawlEngine:
    """Runs multiple ebook spiders concurrently with per-source producer/consumer pools."""

    def __init__(self, config: CrawlerConfig, source_names: List[str]) -> None:
        self.config = config
        self.source_names = source_names
        self.db = CatalogDB(config.db_path)
        self.downloader = FileDownloader()
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
                        client, self.downloader, self.download_sem, book_url, category
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
                await self.db.export_progress_checkpoint(
                    self.config.output_dir / "crawl_progress.json"
                )
            finally:
                self.db.close()
