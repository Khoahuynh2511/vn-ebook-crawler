from dataclasses import dataclass, field
from pathlib import Path
import tempfile
from typing import Dict, List, Optional, Set

DEFAULT_HEADERS: Dict[str, str] = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/131.0.0.0 Safari/537.36"
    ),
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;q=0.9,"
        "image/avif,image/webp,*/*;q=0.8"
    ),
    "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7",
    "Accept-Encoding": "gzip, deflate",
}

SOURCE_MIRRORS: Dict[str, List[str]] = {
    "sach_gallery": ["https://www.sach.gallery", "https://sach.gallery"],
    "nhasachmienphi": ["https://nhasachmienphi.com", "https://www.nhasachmienphi.com"],
    "sachvui": ["https://sachvuii.com", "https://sachvui.com"],
    "dtv_ebook": ["https://dtv-ebook.com.vn"],
    "metaisach": ["https://metaisach.com"],
    "thuviensach": ["https://thuviensach.vn"],
    "isach": ["https://isach.info"],
    "sachhayonline": ["https://www.sachhayonline.com", "https://sachhayonline.com"],
    "archive_org": ["https://archive.org"],
    "archive_org_en": ["https://archive.org"],
}

ALL_SOURCES: List[str] = list(SOURCE_MIRRORS.keys())
ACTIVE_SOURCES: List[str] = [
    "sach_gallery",
    "metaisach",
    "thuviensach",
    "sachhayonline",
    "archive_org",
    "archive_org_en",
]


@dataclass
class CrawlerConfig:
    output_dir: Path = field(
        default_factory=lambda: Path(tempfile.gettempdir()) / "vn-ebook-staging"
    )
    db_path: Path = Path(r"D:\vn-ebook-crawler\catalog.db")
    formats: Set[str] = field(default_factory=lambda: {"epub", "pdf"})
    workers_per_source: int = 5
    download_concurrency: int = 10
    request_delay: float = 0.5
    max_books_per_source: int = 0  # 0 = unlimited / scan all
    request_timeout: float = 30.0
    download_timeout: float = 90.0
    max_discovery_pages: int = 500
    use_katana: bool = True
    katana_depth: int = 2
    katana_headless: bool = False
    katana_bin: Path = Path(r"D:\vn-ebook-crawler\bin\katana.exe")
    use_gdrive: bool = True
    gdrive_root_folder: str = "Books"
    gdrive_root_id: Optional[str] = None
    delete_local_after_upload: bool = True
