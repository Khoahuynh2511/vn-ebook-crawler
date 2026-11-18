import asyncio
import json
import re
import shutil
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, List, Optional

if TYPE_CHECKING:
    from storage import CatalogDB


def gdrive_file_url(file_id: str) -> str:
    return f"https://drive.google.com/file/d/{file_id}/view"


def gdrive_folder_url(folder_id: str) -> str:
    return f"https://drive.google.com/drive/folders/{folder_id}"


def _infer_mime_type(path: Path) -> Optional[str]:
    suffix = path.suffix.lower()
    return {
        ".epub": "application/epub+zip",
        ".pdf": "application/pdf",
        ".json": "application/json",
    }.get(suffix)


def _extract_id(payload: Any) -> Optional[str]:
    if isinstance(payload, dict):
        return payload.get("id")
    if isinstance(payload, list):
        for item in payload:
            if isinstance(item, dict) and item.get("id"):
                return item["id"]
    if isinstance(payload, str):
        m = re.search(r"\(ID:\s*([A-Za-z0-9_-]{10,})\)", payload)
        if m:
            return m.group(1)
        m2 = re.search(r'"id"\s*:\s*"([A-Za-z0-9_-]{10,})"', payload)
        if m2:
            return m2.group(1)
    return None


class GDriveUploader:
    """Async Google Drive folder manager and uploader backed by the `gdrive` plugin CLI."""

    def __init__(
        self,
        db: "CatalogDB",
        root_folder_id: Optional[str] = None,
        bin_path: Optional[Path] = None,
        upload_concurrency: int = 4,
    ) -> None:
        self.db = db
        self.root_folder_id = root_folder_id
        self.bin_path = self._resolve_bin(bin_path)
        self._folder_cache: Dict[str, str] = {}
        self._folder_lock = asyncio.Lock()
        self._upload_sem = asyncio.Semaphore(max(1, upload_concurrency))

    @staticmethod
    def _resolve_bin(bin_path: Optional[Path]) -> Optional[str]:
        if bin_path is not None:
            p = Path(bin_path)
            if p.exists() and p.is_file():
                return str(p)
        return shutil.which("gdrive")

    @property
    def is_available(self) -> bool:
        return bool(self.bin_path)

    async def _run_cli(
        self, *args: str, timeout: float = 120.0, retries: int = 1
    ) -> Any:
        if not self.bin_path:
            raise FileNotFoundError("gdrive CLI not available on PATH")

        # Execute gdrive.CMD from OS temp dir (on C:) so credential lookup succeeds even when invoked from D:\
        cli_cwd = str(Path(self.bin_path).parent) if Path(self.bin_path).parent.exists() else tempfile.gettempdir()
        last_err = ""
        for attempt in range(retries + 1):
            proc = await asyncio.create_subprocess_exec(
                self.bin_path,
                *args,
                cwd=cli_cwd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            try:
                stdout_b, stderr_b = await asyncio.wait_for(
                    proc.communicate(), timeout=timeout
                )
            except asyncio.TimeoutError:
                proc.kill()
                await proc.wait()
                last_err = f"gdrive command timed out after {timeout}s"
                continue

            out_str = stdout_b.decode("utf-8", errors="replace").strip()
            err_str = stderr_b.decode("utf-8", errors="replace").strip()

            if proc.returncode == 0:
                if "--json" in args:
                    if not out_str:
                        return None
                    try:
                        return json.loads(out_str)
                    except json.JSONDecodeError:
                        return out_str
                return out_str

            last_err = err_str or out_str or f"gdrive exited with code {proc.returncode}"
            if attempt < retries:
                await asyncio.sleep(1.0 * (attempt + 1))

        raise RuntimeError(last_err)

    async def ensure_folder(
        self, name: str, parent_id: Optional[str] = None
    ) -> Optional[str]:
        """Find or create a folder by exact name under parent_id (or root)."""
        if not self.is_available:
            return None

        cache_key = f"{parent_id or 'root'}/{name}"
        async with self._folder_lock:
            if cache_key in self._folder_cache:
                return self._folder_cache[cache_key]

            cached_db = await self.db.get_gdrive_folder_id(cache_key)
            if cached_db:
                self._folder_cache[cache_key] = cached_db
                return cached_db

            search_args: List[str] = [
                "readonly",
                "search",
                "--name-exact",
                name,
                "--type",
                "folder",
                "--parent-id",
                parent_id or "root",
                "--json",
            ]
            found = await self._run_cli(*search_args, timeout=30.0)
            fid = _extract_id(found)

            if not fid:
                mkdir_args: List[str] = ["mutate", "mkdir", name]
                if parent_id:
                    mkdir_args.extend(["--parent", parent_id])
                mkdir_args.append("--json")
                created = await self._run_cli(*mkdir_args, timeout=30.0)
                fid = _extract_id(created)
                if not fid:
                    found_after = await self._run_cli(*search_args, timeout=30.0)
                    fid = _extract_id(found_after)

            if fid:
                self._folder_cache[cache_key] = fid
                await self.db.save_gdrive_folder_id(cache_key, fid)
            return fid

    async def ensure_root_folder(self, root_name: str = "Books") -> Optional[str]:
        if self.root_folder_id:
            return self.root_folder_id
        fid = await self.ensure_folder(root_name, parent_id=None)
        if fid:
            self.root_folder_id = fid
        return fid

    async def ensure_category_folder(
        self, root_name: str, source: str, category: str
    ) -> Optional[str]:
        """Ensure Books/<source>/<category> hierarchy on Google Drive."""
        root_id = await self.ensure_root_folder(root_name)
        if not root_id:
            return None
        source_id = await self.ensure_folder(source, parent_id=root_id)
        if not source_id:
            return None
        return await self.ensure_folder(category or "tong-hop", parent_id=source_id)

    async def upload_file(
        self,
        local_path: Path,
        parent_id: str,
        existing_file_id: Optional[str] = None,
    ) -> Optional[str]:
        """Upload a file to parent_id (or update in-place if existing file matches)."""
        if not self.is_available:
            return None

        local_path = Path(local_path)
        if not local_path.exists() or not local_path.is_file():
            return None

        mime = _infer_mime_type(local_path)

        async with self._upload_sem:
            target_id = existing_file_id
            if not target_id:
                existing = await self._run_cli(
                    "readonly",
                    "search",
                    "--name-exact",
                    local_path.name,
                    "--parent-id",
                    parent_id,
                    "--json",
                    timeout=30.0,
                )
                if isinstance(existing, list):
                    for item in existing:
                        if (
                            isinstance(item, dict)
                            and item.get("mimeType") != "application/vnd.google-apps.folder"
                            and item.get("id")
                        ):
                            target_id = item["id"]
                            break

            abs_path = str(local_path.resolve())
            if target_id:
                cmd: List[str] = ["mutate", "update", target_id, abs_path]
                if mime:
                    cmd.extend(["--mime-type", mime])
                cmd.append("--json")
                res = await self._run_cli(*cmd, timeout=180.0)
                return _extract_id(res) or target_id

            cmd = ["mutate", "upload", abs_path, "--parent", parent_id]
            if mime:
                cmd.extend(["--mime-type", mime])
            cmd.append("--json")
            res = await self._run_cli(*cmd, timeout=180.0)
            fid = _extract_id(res)
            if not fid:
                found_after = await self._run_cli(
                    "readonly",
                    "search",
                    "--name-exact",
                    local_path.name,
                    "--parent-id",
                    parent_id,
                    "--json",
                    timeout=30.0,
                )
                fid = _extract_id(found_after)
            return fid
