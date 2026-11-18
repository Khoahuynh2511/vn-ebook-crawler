import asyncio
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class CatalogDB:
    """Thread/async-safe SQLite state tracker for discovered books and pagination resume."""

    def __init__(self, db_path: Path) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = asyncio.Lock()
        self._conn = sqlite3.connect(str(self.db_path), check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._init_schema()
        self._reset_interrupted_sync()

    def _init_schema(self) -> None:
        with self._conn:
            self._conn.execute("PRAGMA journal_mode=WAL;")
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS books (
                    url TEXT PRIMARY KEY,
                    source TEXT NOT NULL,
                    title TEXT,
                    author TEXT,
                    category TEXT,
                    epub_url TEXT,
                    pdf_url TEXT,
                    epub_path TEXT,
                    pdf_path TEXT,
                    file_format TEXT,
                    gdrive_id TEXT,
                    gdrive_url TEXT,
                    status TEXT NOT NULL DEFAULT 'discovered',
                    error TEXT,
                    updated_at TIMESTAMP NOT NULL
                )
                """
            )
            for col_def in ("file_format TEXT", "gdrive_id TEXT", "gdrive_url TEXT"):
                try:
                    self._conn.execute(f"ALTER TABLE books ADD COLUMN {col_def};")
                except sqlite3.OperationalError:
                    pass
            self._conn.execute(
                "UPDATE books SET file_format = 'epub' "
                "WHERE (file_format IS NULL OR file_format = '') AND epub_path IS NOT NULL AND epub_path != ''"
            )
            self._conn.execute(
                "UPDATE books SET file_format = 'pdf' "
                "WHERE (file_format IS NULL OR file_format = '') AND pdf_path IS NOT NULL AND pdf_path != ''"
            )
            self._conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_books_source_status ON books(source, status);"
            )
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS visited_pages (
                    url TEXT PRIMARY KEY,
                    source TEXT NOT NULL,
                    visited_at TIMESTAMP NOT NULL
                )
                """
            )
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS gdrive_folders (
                    cache_key TEXT PRIMARY KEY,
                    folder_id TEXT NOT NULL,
                    updated_at TIMESTAMP NOT NULL
                )
                """
            )
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS discovery_queue (
                    url TEXT PRIMARY KEY,
                    source TEXT NOT NULL,
                    kind TEXT NOT NULL,
                    category TEXT,
                    status TEXT NOT NULL DEFAULT 'pending',
                    last_offset INTEGER NOT NULL DEFAULT 0,
                    updated_at TIMESTAMP NOT NULL
                )
                """
            )
            self._conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_discovery_queue_source ON discovery_queue(source, kind, status);"
            )
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS source_checkpoints (
                    source TEXT PRIMARY KEY,
                    last_phase TEXT,
                    last_cursor_url TEXT,
                    last_cursor_offset INTEGER NOT NULL DEFAULT 0,
                    last_book_url TEXT,
                    last_book_title TEXT,
                    status_note TEXT,
                    updated_at TIMESTAMP NOT NULL
                )
                """
            )

    def _reset_interrupted_sync(self) -> int:
        with self._conn:
            cur = self._conn.execute(
                "UPDATE books SET status = 'discovered' WHERE status = 'processing'"
            )
            return int(cur.rowcount or 0)

    def close(self) -> None:
        self._conn.close()

    async def reset_interrupted_books(self) -> int:
        """Reset any in-flight 'processing' books back to 'discovered' for clean resume."""
        async with self._lock:
            return self._reset_interrupted_sync()

    async def reset_failed_books(self, source: Optional[str] = None) -> int:
        """Re-queue books marked 'failed' back to 'discovered'."""
        now = _utc_now()
        async with self._lock:
            with self._conn:
                if source:
                    cur = self._conn.execute(
                        "UPDATE books SET status = 'discovered', error = NULL, updated_at = ? "
                        "WHERE source = ? AND status = 'failed'",
                        (now, source),
                    )
                else:
                    cur = self._conn.execute(
                        "UPDATE books SET status = 'discovered', error = NULL, updated_at = ? "
                        "WHERE status = 'failed'",
                        (now,),
                    )
                return int(cur.rowcount or 0)

    async def clear_visited_pages(self, source: Optional[str] = None) -> None:
        """Clear visited pages, discovery cursors, and checkpoints to force discovery re-scan."""
        async with self._lock:
            with self._conn:
                if source:
                    self._conn.execute(
                        "DELETE FROM visited_pages WHERE source = ?", (source,)
                    )
                    self._conn.execute(
                        "DELETE FROM discovery_queue WHERE source = ?", (source,)
                    )
                    self._conn.execute(
                        "UPDATE source_checkpoints SET last_phase = NULL, last_cursor_url = NULL, last_cursor_offset = 0 "
                        "WHERE source = ?",
                        (source,),
                    )
                else:
                    self._conn.execute("DELETE FROM visited_pages")
                    self._conn.execute("DELETE FROM discovery_queue")
                    self._conn.execute(
                        "UPDATE source_checkpoints SET last_phase = NULL, last_cursor_url = NULL, last_cursor_offset = 0"
                    )

    async def reset_rate_limited_to_pending(self, url: str) -> None:
        """Keep a rate-limited book as 'discovered' (preserves FIFO rowid) so next run retries it."""
        now = _utc_now()
        async with self._lock:
            with self._conn:
                self._conn.execute(
                    "UPDATE books SET status = 'discovered', error = 'rate_limited_daily', updated_at = ? "
                    "WHERE url = ?",
                    (now, url),
                )

    async def get_gdrive_folder_id(self, cache_key: str) -> Optional[str]:
        async with self._lock:
            row = self._conn.execute(
                "SELECT folder_id FROM gdrive_folders WHERE cache_key = ?",
                (cache_key,),
            ).fetchone()
            return str(row["folder_id"]) if row else None

    async def save_gdrive_folder_id(self, cache_key: str, folder_id: str) -> None:
        now = _utc_now()
        async with self._lock:
            with self._conn:
                self._conn.execute(
                    """
                    INSERT OR REPLACE INTO gdrive_folders (cache_key, folder_id, updated_at)
                    VALUES (?, ?, ?)
                    """,
                    (cache_key, folder_id, now),
                )

    async def is_book_already_done(self, url: str) -> bool:
        """Return True if URL was already processed (on disk or uploaded to Google Drive)."""
        async with self._lock:
            row = self._conn.execute(
                "SELECT status, epub_path, pdf_path, gdrive_id FROM books WHERE url = ?",
                (url,),
            ).fetchone()
            if row is None:
                return False

            status = row["status"]
            if status in ("completed", "partial"):
                if row["gdrive_id"]:
                    return True
                paths = [p for p in (row["epub_path"], row["pdf_path"]) if p]
                if any(Path(p).exists() for p in paths):
                    return True
                # Downloaded file was deleted from disk and not on Google Drive -> allow re-download
                now = _utc_now()
                with self._conn:
                    self._conn.execute(
                        "UPDATE books SET status = 'discovered', epub_path = NULL, pdf_path = NULL, updated_at = ? "
                        "WHERE url = ?",
                        (now, url),
                    )
                return False

            if status in ("no_files", "blocked_by_captcha", "failed"):
                return True

            return False

    async def upsert_discovered_book(
        self,
        url: str,
        source: str,
        category: Optional[str] = None,
        title: Optional[str] = None,
    ) -> bool:
        """Insert a newly discovered book URL. Returns True if newly inserted."""
        now = _utc_now()
        async with self._lock:
            cur = self._conn.execute("SELECT 1 FROM books WHERE url = ?", (url,))
            exists = cur.fetchone() is not None
            if exists:
                return False
            with self._conn:
                self._conn.execute(
                    """
                    INSERT INTO books (url, source, title, category, status, updated_at)
                    VALUES (?, ?, ?, ?, 'discovered', ?)
                    """,
                    (url, source, title, category, now),
                )
            return True

    async def mark_page_visited(self, url: str, source: str) -> None:
        now = _utc_now()
        async with self._lock:
            with self._conn:
                self._conn.execute(
                    """
                    INSERT OR REPLACE INTO visited_pages (url, source, visited_at)
                    VALUES (?, ?, ?)
                    """,
                    (url, source, now),
                )

    async def is_page_visited(self, url: str) -> bool:
        async with self._lock:
            cur = self._conn.execute(
                "SELECT 1 FROM visited_pages WHERE url = ?", (url,)
            )
            return cur.fetchone() is not None

    async def get_pending_books(
        self, source: str, limit: int = 0
    ) -> List[Dict[str, Any]]:
        """Return books whose status is discovered or processing in exact FIFO discovery order."""
        async with self._lock:
            sql = """
                SELECT url, source, title, author, category, status
                FROM books
                WHERE source = ? AND status IN ('discovered', 'processing')
                ORDER BY rowid ASC
            """
            params: List[Any] = [source]
            if limit and limit > 0:
                sql += " LIMIT ?"
                params.append(limit)
            rows = self._conn.execute(sql, params).fetchall()
            return [dict(r) for r in rows]

    async def enqueue_discovery_target(
        self,
        url: str,
        source: str,
        kind: str,
        category: Optional[str] = None,
    ) -> None:
        now = _utc_now()
        async with self._lock:
            with self._conn:
                self._conn.execute(
                    """
                    INSERT OR IGNORE INTO discovery_queue (url, source, kind, category, status, last_offset, updated_at)
                    VALUES (?, ?, ?, ?, 'pending', 0, ?)
                    """,
                    (url, source, kind, category, now),
                )

    async def get_discovery_targets(
        self, source: str, kind: str
    ) -> List[Dict[str, Any]]:
        async with self._lock:
            rows = self._conn.execute(
                """
                SELECT url, source, kind, category, status, last_offset
                FROM discovery_queue
                WHERE source = ? AND kind = ? AND status = 'pending'
                ORDER BY rowid ASC
                """,
                (source, kind),
            ).fetchall()
            return [dict(r) for r in rows]

    async def get_discovery_entry(self, url: str) -> Optional[Dict[str, Any]]:
        async with self._lock:
            row = self._conn.execute(
                "SELECT url, source, kind, category, status, last_offset FROM discovery_queue WHERE url = ?",
                (url,),
            ).fetchone()
            return dict(row) if row else None

    async def update_discovery_progress(
        self,
        url: str,
        source: str,
        kind: str,
        *,
        last_offset: int = 0,
        status: str = "pending",
        category: Optional[str] = None,
    ) -> None:
        now = _utc_now()
        async with self._lock:
            with self._conn:
                self._conn.execute(
                    """
                    INSERT INTO discovery_queue (url, source, kind, category, status, last_offset, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(url) DO UPDATE SET
                        status = excluded.status,
                        last_offset = excluded.last_offset,
                        category = COALESCE(excluded.category, discovery_queue.category),
                        updated_at = excluded.updated_at
                    """,
                    (url, source, kind, category, status, last_offset, now),
                )

    async def has_discovery_history(self, source: str) -> bool:
        async with self._lock:
            row = self._conn.execute(
                "SELECT 1 FROM discovery_queue WHERE source = ? LIMIT 1",
                (source,),
            ).fetchone()
            return row is not None

    async def update_source_checkpoint(
        self,
        source: str,
        *,
        last_phase: Optional[str] = None,
        last_cursor_url: Optional[str] = None,
        last_cursor_offset: Optional[int] = None,
        last_book_url: Optional[str] = None,
        last_book_title: Optional[str] = None,
        status_note: Optional[str] = None,
    ) -> None:
        now = _utc_now()
        async with self._lock:
            existing = self._conn.execute(
                "SELECT * FROM source_checkpoints WHERE source = ?",
                (source,),
            ).fetchone()
            with self._conn:
                if existing is None:
                    self._conn.execute(
                        """
                        INSERT INTO source_checkpoints (
                            source, last_phase, last_cursor_url, last_cursor_offset,
                            last_book_url, last_book_title, status_note, updated_at
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            source,
                            last_phase,
                            last_cursor_url,
                            last_cursor_offset if last_cursor_offset is not None else 0,
                            last_book_url,
                            last_book_title,
                            status_note,
                            now,
                        ),
                    )
                else:
                    self._conn.execute(
                        """
                        UPDATE source_checkpoints
                        SET
                            last_phase = COALESCE(?, last_phase),
                            last_cursor_url = COALESCE(?, last_cursor_url),
                            last_cursor_offset = COALESCE(?, last_cursor_offset),
                            last_book_url = COALESCE(?, last_book_url),
                            last_book_title = COALESCE(?, last_book_title),
                            status_note = COALESCE(?, status_note),
                            updated_at = ?
                        WHERE source = ?
                        """,
                        (
                            last_phase,
                            last_cursor_url,
                            last_cursor_offset,
                            last_book_url,
                            last_book_title,
                            status_note,
                            now,
                            source,
                        ),
                    )

    async def get_source_checkpoints(self) -> Dict[str, Dict[str, Any]]:
        async with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM source_checkpoints ORDER BY source"
            ).fetchall()
            result: Dict[str, Dict[str, Any]] = {dict(r)["source"]: dict(r) for r in rows}

            # Ensure every source present in books table also exposes its resume queue/last book
            sources_in_db = [
                r["source"]
                for r in self._conn.execute("SELECT DISTINCT source FROM books ORDER BY source").fetchall()
            ]
            for src in sources_in_db:
                if src not in result:
                    last_done = self._conn.execute(
                        "SELECT url, title, updated_at FROM books WHERE source = ? AND status IN ('completed', 'partial') "
                        "ORDER BY updated_at DESC LIMIT 1",
                        (src,),
                    ).fetchone()
                    result[src] = {
                        "source": src,
                        "last_phase": None,
                        "last_cursor_url": None,
                        "last_cursor_offset": 0,
                        "last_book_url": last_done["url"] if last_done else None,
                        "last_book_title": last_done["title"] if last_done else None,
                        "status_note": "Sẵn sàng tiếp tục",
                        "updated_at": last_done["updated_at"] if last_done else None,
                    }

                item = result[src]
                next_rows = self._conn.execute(
                    "SELECT url FROM books WHERE source = ? AND status IN ('discovered', 'processing') "
                    "ORDER BY rowid ASC LIMIT 3",
                    (src,),
                ).fetchall()
                item["next_pending_urls"] = [nr["url"] for nr in next_rows]
                pending_disc = self._conn.execute(
                    "SELECT COUNT(*) AS cnt FROM discovery_queue WHERE source = ? AND status = 'pending'",
                    (src,),
                ).fetchone()
                item["pending_discovery_pages"] = int(pending_disc["cnt"]) if pending_disc else 0
            return result

    async def count_books_by_source(self, source: str) -> int:
        async with self._lock:
            row = self._conn.execute(
                "SELECT COUNT(*) AS cnt FROM books WHERE source = ?", (source,)
            ).fetchone()
            return int(row["cnt"]) if row else 0

    async def count_completed_books(self, source: str) -> int:
        async with self._lock:
            row = self._conn.execute(
                "SELECT COUNT(*) AS cnt FROM books WHERE source = ? AND status IN ('completed', 'partial')",
                (source,),
            ).fetchone()
            return int(row["cnt"]) if row else 0

    async def mark_processing(self, url: str) -> None:
        now = _utc_now()
        async with self._lock:
            with self._conn:
                self._conn.execute(
                    "UPDATE books SET status = 'processing', updated_at = ? WHERE url = ?",
                    (now, url),
                )

    async def get_unsynced_completed_books(
        self, sources: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        """Return completed/partial books with local files that have not been uploaded to Google Drive yet."""
        async with self._lock:
            sql = """
                SELECT url, source, title, author, category, epub_path, pdf_path, file_format, status
                FROM books
                WHERE status IN ('completed', 'partial')
                  AND (gdrive_id IS NULL OR gdrive_id = '')
                  AND ((epub_path IS NOT NULL AND epub_path != '') OR (pdf_path IS NOT NULL AND pdf_path != ''))
            """
            params: List[Any] = []
            if sources:
                placeholders = ",".join("?" for _ in sources)
                sql += f" AND source IN ({placeholders})"
                params.extend(sources)
            sql += " ORDER BY updated_at ASC"
            rows = self._conn.execute(sql, params).fetchall()
            return [dict(r) for r in rows]

    async def update_book_result(
        self,
        url: str,
        *,
        status: str,
        title: Optional[str] = None,
        author: Optional[str] = None,
        category: Optional[str] = None,
        epub_url: Optional[str] = None,
        pdf_url: Optional[str] = None,
        epub_path: Optional[str] = None,
        pdf_path: Optional[str] = None,
        file_format: Optional[str] = None,
        gdrive_id: Optional[str] = None,
        gdrive_url: Optional[str] = None,
        clear_local_paths: bool = False,
        error: Optional[str] = None,
    ) -> None:
        now = _utc_now()
        clear_flag = 1 if clear_local_paths else 0
        async with self._lock:
            with self._conn:
                self._conn.execute(
                    """
                    UPDATE books
                    SET
                        title = COALESCE(?, title),
                        author = COALESCE(?, author),
                        category = COALESCE(?, category),
                        epub_url = COALESCE(?, epub_url),
                        pdf_url = COALESCE(?, pdf_url),
                        epub_path = CASE WHEN ? THEN NULL ELSE COALESCE(?, epub_path) END,
                        pdf_path = CASE WHEN ? THEN NULL ELSE COALESCE(?, pdf_path) END,
                        file_format = COALESCE(?, file_format),
                        gdrive_id = COALESCE(?, gdrive_id),
                        gdrive_url = COALESCE(?, gdrive_url),
                        status = ?,
                        error = ?,
                        updated_at = ?
                    WHERE url = ?
                    """,
                    (
                        title,
                        author,
                        category,
                        epub_url,
                        pdf_url,
                        clear_flag,
                        epub_path,
                        clear_flag,
                        pdf_path,
                        file_format,
                        gdrive_id,
                        gdrive_url,
                        status,
                        error,
                        now,
                        url,
                    ),
                )

    async def get_summary_stats(self) -> Dict[str, Dict[str, int]]:
        async with self._lock:
            rows = self._conn.execute(
                """
                SELECT
                    source,
                    COUNT(*) AS total,
                    SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) AS completed,
                    SUM(CASE WHEN status = 'partial' THEN 1 ELSE 0 END) AS partial,
                    SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS failed,
                    SUM(CASE WHEN status = 'no_files' THEN 1 ELSE 0 END) AS no_files,
                    SUM(CASE WHEN status IN ('discovered', 'processing') THEN 1 ELSE 0 END) AS pending,
                    SUM(CASE WHEN file_format = 'epub' OR (epub_path IS NOT NULL AND epub_path != '') THEN 1 ELSE 0 END) AS epubs,
                    SUM(CASE WHEN file_format = 'pdf' OR (pdf_path IS NOT NULL AND pdf_path != '') THEN 1 ELSE 0 END) AS pdfs,
                    SUM(CASE WHEN gdrive_id IS NOT NULL AND gdrive_id != '' THEN 1 ELSE 0 END) AS gdrive_count
                FROM books
                GROUP BY source
                ORDER BY source
                """
            ).fetchall()
            return {
                r["source"]: {
                    "total": int(r["total"] or 0),
                    "completed": int(r["completed"] or 0),
                    "partial": int(r["partial"] or 0),
                    "failed": int(r["failed"] or 0),
                    "no_files": int(r["no_files"] or 0),
                    "pending": int(r["pending"] or 0),
                    "epubs": int(r["epubs"] or 0),
                    "pdfs": int(r["pdfs"] or 0),
                    "gdrive_count": int(r["gdrive_count"] or 0),
                }
                for r in rows
            }

    async def export_progress_checkpoint(self, checkpoint_path: Path) -> Dict[str, Any]:
        """Export human-readable JSON progress checkpoint alongside SQLite DB."""
        stats = await self.get_summary_stats()
        checkpoints = await self.get_source_checkpoints()
        async with self._lock:
            done_rows = self._conn.execute(
                """
                SELECT url, source, title, author, category, status, file_format, epub_path, pdf_path, gdrive_id, gdrive_url, updated_at
                FROM books
                WHERE status IN ('completed', 'partial')
                ORDER BY updated_at DESC
                """
            ).fetchall()
            pending_row = self._conn.execute(
                "SELECT COUNT(*) AS cnt FROM books WHERE status IN ('discovered', 'processing')"
            ).fetchone()

        payload: Dict[str, Any] = {
            "updated_at": _utc_now(),
            "db_path": str(self.db_path),
            "pending_total": int(pending_row["cnt"]) if pending_row else 0,
            "summary_by_source": stats,
            "resume_cursors": checkpoints,
            "completed_books": [dict(r) for r in done_rows],
        }
        checkpoint_path = Path(checkpoint_path)
        checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
        checkpoint_path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        return payload
