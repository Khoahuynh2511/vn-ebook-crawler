import argparse
import asyncio
import sys
import tempfile
from pathlib import Path
from typing import List, Set

from rich.console import Console
from rich.table import Table

from config import ACTIVE_SOURCES, ALL_SOURCES, CrawlerConfig
from engine import ParallelCrawlEngine
from storage import CatalogDB


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Multi-Source Parallel Vietnamese Ebook Crawler (EPUB & PDF)"
    )
    parser.add_argument(
        "--sources",
        default="all",
        help=(
            "Danh sách nguồn cách nhau bởi dấu phẩy hoặc 'all'. "
            f"Mặc định 'all' chạy 4 nguồn hiệu quả: {','.join(ACTIVE_SOURCES)}"
        ),
    )
    parser.add_argument(
        "--formats",
        default="epub,pdf",
        help="Định dạng cần tải/tạo: epub,pdf (mặc định: epub,pdf)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Số lượng file sách tối đa mỗi nguồn (0 = quét & tải tất cả)",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=5,
        help="Số luồng worker song song cho mỗi nguồn (mặc định: 5)",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=10,
        help="Số lượng kết nối tải file đồng thời tối đa (mặc định: 10)",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.5,
        help="Độ trễ giữa các request (giây, mặc định: 0.5)",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path(tempfile.gettempdir()) / "vn-ebook-staging",
        help="Thư mục đệm tạm thời trước khi đẩy lên Google Drive (mặc định: thư mục TEMP hệ thống)",
    )
    parser.add_argument(
        "--db",
        type=Path,
        default=Path(r"D:\vn-ebook-crawler\catalog.db"),
        help=r"Đường dẫn file SQLite lưu trạng thái crawl (mặc định: D:\vn-ebook-crawler\catalog.db)",
    )
    parser.add_argument(
        "--stats",
        action="store_true",
        help="Hiển thị thống kê trong catalog.db rồi thoát",
    )
    parser.add_argument(
        "--no-katana",
        action="store_true",
        help="Tắt Katana và chỉ dùng bộ quét Python thuần",
    )
    parser.add_argument(
        "--katana-depth",
        type=int,
        default=2,
        help="Độ sâu crawl cho Katana (mặc định: 2)",
    )
    parser.add_argument(
        "--katana-headless",
        action="store_true",
        help="Bật trình duyệt headless của Katana (-hl) cho trang SPA",
    )
    parser.add_argument(
        "--retry-failed",
        action="store_true",
        help="Đặt lại các sách từng bị lỗi (failed) về trạng thái chờ để thử tải lại",
    )
    parser.add_argument(
        "--reset-discovery",
        action="store_true",
        help="Xóa dấu trang danh mục đã quét để duyệt lại từ đầu (vẫn giữ và bỏ qua các sách đã tải)",
    )
    parser.add_argument(
        "--no-gdrive",
        action="store_true",
        help="Tắt tự động lưu lên Google Drive, chỉ lưu ở thư mục cục bộ",
    )
    parser.add_argument(
        "--gdrive-folder",
        default="Books",
        help="Tên thư mục gốc trên Google Drive để lưu sách (mặc định: Books)",
    )
    parser.add_argument(
        "--gdrive-folder-id",
        default=None,
        help="ID thư mục gốc trên Google Drive nếu muốn chỉ định trực tiếp",
    )
    parser.add_argument(
        "--keep-local",
        action="store_true",
        help="Giữ lại bản sao cục bộ trên máy sau khi đã upload lên Google Drive (mặc định sẽ xóa file tạm)",
    )
    parser.add_argument(
        "--sync-gdrive",
        action="store_true",
        help="Đồng bộ toàn bộ sách đã tải sẵn trong thư mục cục bộ lên Google Drive rồi thoát",
    )
    return parser.parse_args()


def resolve_sources(raw: str) -> List[str]:
    if raw.strip().lower() == "all":
        return list(ACTIVE_SOURCES)
    selected = [s.strip() for s in raw.split(",") if s.strip()]
    invalid = [s for s in selected if s not in ALL_SOURCES]
    if invalid:
        raise ValueError(
            f"Nguồn không hợp lệ: {', '.join(invalid)}. "
            f"Các nguồn hỗ trợ: {', '.join(ALL_SOURCES)}"
        )
    return selected


def resolve_formats(raw: str) -> Set[str]:
    fmts = {f.strip().lower() for f in raw.split(",") if f.strip()}
    valid = fmts & {"epub", "pdf"}
    if not valid:
        raise ValueError("Định dạng phải bao gồm ít nhất 'epub' hoặc 'pdf'.")
    return valid


async def print_stats(db_path: Path) -> None:
    console = Console()
    db = CatalogDB(db_path)
    try:
        stats = await db.get_summary_stats()
        checkpoints = await db.get_source_checkpoints()
    finally:
        db.close()

    table = Table(title=f"Thống kê thư viện ({db_path})")
    table.add_column("Nguồn", style="cyan")
    table.add_column("Tổng tìm thấy", justify="right")
    table.add_column("Hoàn thành", justify="right", style="green")
    table.add_column("Một phần", justify="right", style="yellow")
    table.add_column("EPUB", justify="right", style="green")
    table.add_column("PDF", justify="right", style="blue")
    table.add_column("Trên GDrive", justify="right", style="bright_green")
    table.add_column("Chờ xử lý", justify="right")
    table.add_column("Không file/Lỗi", justify="right", style="red")

    if not stats:
        console.print("[yellow]Chưa có dữ liệu nào trong catalog.db.[/yellow]")
        return

    for source, row in stats.items():
        table.add_row(
            source,
            str(row["total"]),
            str(row["completed"]),
            str(row["partial"]),
            str(row["epubs"]),
            str(row["pdfs"]),
            str(row["gdrive_count"]),
            str(row["pending"]),
            str(row["no_files"] + row["failed"]),
        )
    console.print(table)

    if checkpoints:
        cp_table = Table(title="Điểm dừng & Vị trí tiếp tục (Resume Cursors)")
        cp_table.add_column("Nguồn", style="cyan", no_wrap=True)
        cp_table.add_column("Vị trí quét (Cursor)", overflow="fold")
        cp_table.add_column("Cuốn vừa tải", overflow="fold", style="green")
        cp_table.add_column("Cuốn tiếp theo sẽ tải", overflow="fold", style="blue")
        cp_table.add_column("Ghi chú", style="yellow")

        for source, cp in checkpoints.items():
            cursor_txt = cp.get("last_cursor_url") or "-"
            offset = cp.get("last_cursor_offset") or 0
            if cursor_txt != "-" and offset:
                cursor_txt = f"{cursor_txt} (#{offset})"
            next_urls = cp.get("next_pending_urls") or []
            next_txt = next_urls[0] if next_urls else "(Sẽ quét tiếp trang/sitemap kế tiếp)"
            cp_table.add_row(
                source,
                cursor_txt,
                cp.get("last_book_title") or cp.get("last_book_url") or "-",
                next_txt,
                cp.get("status_note") or "-",
            )
        console.print(cp_table)


def main() -> int:
    # Ensure UTF-8 console output on Windows
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")

    args = parse_args()
    if args.stats:
        asyncio.run(print_stats(args.db))
        return 0

    try:
        sources = resolve_sources(args.sources)
        formats = resolve_formats(args.formats)
    except ValueError as exc:
        print(f"Lỗi tham số: {exc}", file=sys.stderr)
        return 2

    cfg = CrawlerConfig(
        output_dir=args.output_dir,
        db_path=args.db,
        formats=formats,
        workers_per_source=args.workers,
        download_concurrency=args.concurrency,
        request_delay=args.delay,
        max_books_per_source=args.limit,
        use_katana=not args.no_katana,
        katana_depth=args.katana_depth,
        katana_headless=args.katana_headless,
        use_gdrive=not args.no_gdrive,
        gdrive_root_folder=args.gdrive_folder,
        gdrive_root_id=args.gdrive_folder_id,
        delete_local_after_upload=not args.keep_local and not args.no_gdrive,
    )
    engine = ParallelCrawlEngine(cfg, sources)
    if args.sync_gdrive:
        asyncio.run(engine.sync_existing_books_to_gdrive())
        asyncio.run(print_stats(args.db))
        return 0

    interrupted = False
    try:
        asyncio.run(
            engine.run(
                retry_failed=args.retry_failed,
                reset_discovery=args.reset_discovery,
            )
        )
    except KeyboardInterrupt:
        interrupted = True
        Console().print(
            f"\n[yellow]Đã dừng theo yêu cầu (Ctrl+C). Tiến trình crawl đã được lưu an toàn vào "
            f"{args.db} và {args.output_dir / 'crawl_progress.json'} để tiếp tục ở lần chạy sau.[/yellow]"
        )

    asyncio.run(print_stats(args.db))
    return 130 if interrupted else 0


if __name__ == "__main__":
    raise SystemExit(main())
