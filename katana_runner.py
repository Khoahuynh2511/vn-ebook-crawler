import asyncio
import shutil
import time
from pathlib import Path
from typing import AsyncIterator, List, Optional


class KatanaDiscovery:
    """Async wrapper around ProjectDiscovery Katana for high-speed URL & JS/sitemap discovery."""

    def __init__(self, bin_path: Optional[Path] = None) -> None:
        self.bin_path = self._resolve_bin(bin_path)

    @staticmethod
    def _resolve_bin(bin_path: Optional[Path]) -> Optional[Path]:
        if bin_path is not None:
            candidate = Path(bin_path)
            if candidate.exists() and candidate.is_file():
                return candidate
        which_path = shutil.which("katana")
        return Path(which_path) if which_path else None

    @property
    def is_available(self) -> bool:
        return self.bin_path is not None and self.bin_path.exists()

    def build_command(
        self,
        base_url: str,
        *,
        depth: int = 3,
        js_crawl: bool = True,
        known_files: bool = True,
        headless: bool = False,
        concurrency: int = 15,
        parallelism: int = 10,
        timeout: int = 15,
        delay: int = 0,
    ) -> List[str]:
        if not self.bin_path:
            raise FileNotFoundError("katana binary not found")

        cmd: List[str] = [
            str(self.bin_path),
            "-u",
            base_url,
            "-d",
            str(depth),
            "-c",
            str(concurrency),
            "-p",
            str(parallelism),
            "-timeout",
            str(timeout),
            "-silent",
            "-nc",
        ]
        if js_crawl:
            cmd.append("-jc")
        if known_files:
            cmd.extend(["-kf", "all"])
        if headless:
            cmd.extend(["-hl", "-nos"])
        if delay > 0:
            cmd.extend(["-delay", str(delay)])
        return cmd

    async def stream_discovered_urls(
        self,
        base_url: str,
        depth: int = 3,
        js_crawl: bool = True,
        known_files: bool = True,
        headless: bool = False,
        max_urls: int = 0,
        delay: int = 0,
        max_duration: float = 15.0,
        line_timeout: float = 8.0,
    ) -> AsyncIterator[str]:
        """Run katana.exe asynchronously and stream discovered URLs with hard timeouts."""
        if not self.is_available:
            return

        cmd = self.build_command(
            base_url,
            depth=depth,
            js_crawl=js_crawl,
            known_files=known_files,
            headless=headless,
            delay=delay,
        )

        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
        )

        yielded = 0
        start_time = time.monotonic()
        try:
            assert proc.stdout is not None
            while True:
                elapsed = time.monotonic() - start_time
                if max_duration > 0 and elapsed >= max_duration:
                    break
                step_timeout = (
                    min(line_timeout, max(0.1, max_duration - elapsed))
                    if max_duration > 0
                    else line_timeout
                )
                try:
                    line = await asyncio.wait_for(
                        proc.stdout.readline(), timeout=step_timeout
                    )
                except asyncio.TimeoutError:
                    break
                if not line:
                    break
                url = line.decode("utf-8", errors="ignore").strip()
                if not url.startswith(("http://", "https://")):
                    continue
                yield url
                yielded += 1
                if max_urls > 0 and yielded >= max_urls:
                    break
        finally:
            if proc.returncode is None:
                try:
                    proc.terminate()
                    await asyncio.wait_for(proc.wait(), timeout=2.0)
                except Exception:
                    try:
                        proc.kill()
                        await proc.wait()
                    except Exception:
                        pass
            else:
                await proc.wait()
