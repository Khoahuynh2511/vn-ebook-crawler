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
                    status TEXT NOT NULL DEFAULT 'discovered',
                    error TEXT,
                    updated_at TIMESTAMP NOT NULL
                )
                """
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
        """Clear visited listing/category pages to force full discovery re-scan."""
        async with self._lock:
            with self._conn:
                if source:
                    self._conn.execute(
                        "DELETE FROM visited_pages WHERE source = ?", (source,)
                    )
                else:
                    self._conn.execute("DELETE FROM visited_pages")

    async def is_book_already_done(self, url: str) -> bool:
        """Return True if URL was already processed; verify downloaded files still exist on disk."""
        async with self._lock:
            row = self._conn.execute(
                "SELECT status, epub_path, pdf_path FROM books WHERE url = ?",
                (url,),
            ).fetchone()
            if row is None:
                return False

            status = row["status"]
            if status in ("completed", "partial"):
                paths = [p for p in (row["epub_path"], row["pdf_path"]) if p]
                if any(Path(p).exists() for p in paths):
                    return True
                # Downloaded file was deleted from disk -> allow re-downloading
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
        """Return books whose status is discovered or processing or failed (for resume)."""
        async with self._lock:
            sql = """
                SELECT url, source, title, author, category, status
                FROM books
                WHERE source = ? AND status IN ('discovered', 'processing')
                ORDER BY updated_at ASC
            """
            params: List[Any] = [source]
            if limit and limit > 0:
                sql += " LIMIT ?"
                params.append(limit)
            rows = self._conn.execute(sql, params).fetchall()
            return [dict(r) for r in rows]

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
        error: Optional[str] = None,
    ) -> None:
        now = _utc_now()
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
                        epub_path = COALESCE(?, epub_path),
                        pdf_path = COALESCE(?, pdf_path),
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
                        epub_path,
                        pdf_path,
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
                    SUM(CASE WHEN epub_path IS NOT NULL AND epub_path != '' THEN 1 ELSE 0 END) AS epubs,
                    SUM(CASE WHEN pdf_path IS NOT NULL AND pdf_path != '' THEN 1 ELSE 0 END) AS pdfs
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
                }
                for r in rows
            }

    async def export_progress_checkpoint(self, checkpoint_path: Path) -> Dict[str, Any]:
        """Export human-readable JSON progress checkpoint alongside SQLite DB."""
        stats = await self.get_summary_stats()
        async with self._lock:
            done_rows = self._conn.execute(
                """
                SELECT url, source, title, author, category, status, epub_path, pdf_path, updated_at
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
            "completed_books": [dict(r) for r in done_rows],
        }
        checkpoint_path = Path(checkpoint_path)
        checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
        checkpoint_path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        return payload
