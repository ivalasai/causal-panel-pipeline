#!/usr/bin/env python3
"""
Generic archive downloader and tabular consolidation utility.

Downloads zipped archives from a remote endpoint, decompresses members in
streaming chunks, normalizes records to a single master table, and writes
a consolidated CSV. Designed for resumable, rate-limited ingestion pipelines.
"""

from __future__ import annotations

import argparse
import io
import json
import logging
import sys
import time
import zipfile
from pathlib import Path
from typing import Any, Iterator

import pandas as pd
import requests
from dotenv import load_dotenv
from tqdm import tqdm

# ---------------------------------------------------------------------------
# Configuration defaults (override via CLI or environment)
# ---------------------------------------------------------------------------
DEFAULT_ARCHIVE_URL = "https://example.com/api/v1/archives/sample_batch.zip"
DEFAULT_OUTPUT_PATH = Path("data/processed/master_table.csv")
DEFAULT_THROTTLE_SECONDS = 2.0
DEFAULT_TIMEOUT_SECONDS = 120
DEFAULT_CHUNK_SIZE = 64 * 1024  # 64 KiB streaming buffer
REQUEST_HEADERS = {"User-Agent": "data-pipeline/1.0 (+https://example.com)"}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def load_config() -> dict[str, str | None]:
    """Load optional credentials and base URL from environment."""
    load_dotenv()
    import os

    return {
        "api_base_url": os.getenv("API_BASE_URL"),
        "api_token": os.getenv("API_TOKEN"),
    }


def build_session(token: str | None) -> requests.Session:
    """Return a configured HTTP session with optional bearer authentication."""
    session = requests.Session()
    session.headers.update(REQUEST_HEADERS)
    if token:
        session.headers["Authorization"] = f"Bearer {token}"
    return session


def download_archive(
    session: requests.Session,
    url: str,
    destination: Path,
    timeout: int = DEFAULT_TIMEOUT_SECONDS,
) -> Path:
    """
    Stream-download a remote archive to disk with progress reporting.

    Args:
        session: Authenticated requests session.
        url: Fully qualified archive URL.
        destination: Local path for the downloaded bytes.
        timeout: Per-request timeout in seconds.

    Returns:
        Path to the written archive file.

    Raises:
        requests.RequestException: On network or HTTP failure.
    """
    destination.parent.mkdir(parents=True, exist_ok=True)

    try:
        with session.get(url, stream=True, timeout=timeout) as response:
            response.raise_for_status()
            total = int(response.headers.get("content-length", 0))
            with destination.open("wb") as handle, tqdm(
                total=total or None,
                unit="B",
                unit_scale=True,
                desc=destination.name,
            ) as progress:
                for chunk in response.iter_content(chunk_size=DEFAULT_CHUNK_SIZE):
                    if not chunk:
                        continue
                    handle.write(chunk)
                    progress.update(len(chunk))
    except requests.RequestException as exc:
        logger.error("Download failed for %s: %s", url, exc)
        raise

    logger.info("Archive saved to %s", destination)
    return destination


def iter_zip_members(archive_path: Path) -> Iterator[tuple[str, bytes]]:
    """
    Yield (member_name, raw_bytes) for each file inside a ZIP archive.

    Reads member payloads into memory; for very large members, replace with
    incremental extraction to a staging directory.
    """
    try:
        with zipfile.ZipFile(archive_path, "r") as archive:
            for info in archive.infolist():
                if info.is_dir():
                    continue
                yield info.filename, archive.read(info.filename)
    except zipfile.BadZipFile as exc:
        logger.error("Invalid archive %s: %s", archive_path, exc)
        raise


def parse_member_payload(name: str, payload: bytes) -> list[dict[str, Any]]:
    """
    Parse a single archive member into a list of row dictionaries.

  Supports newline-delimited JSON and UTF-8 CSV payloads. Extend this
  function when your source format requires custom parsers.
    """
    suffix = Path(name).suffix.lower()

    if suffix == ".json":
        text = payload.decode("utf-8", errors="replace").strip()
        if text.startswith("["):
            data = json.loads(text)
            return data if isinstance(data, list) else [data]
        return [json.loads(line) for line in text.splitlines() if line.strip()]

    if suffix == ".csv":
        frame = pd.read_csv(io.BytesIO(payload))
        return frame.to_dict(orient="records")

    logger.warning("Skipping unsupported member type: %s", name)
    return []


def consolidate_records(
    records: list[dict[str, Any]],
    source_label: str,
) -> pd.DataFrame:
    """Attach provenance metadata and return a normalized DataFrame."""
    if not records:
        return pd.DataFrame()

    frame = pd.json_normalize(records)
    frame["source_batch"] = source_label
    return frame


def write_master_table(frame: pd.DataFrame, output_path: Path) -> None:
    """Persist the consolidated table to CSV."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(output_path, index=False)
    logger.info("Wrote %s rows to %s", len(frame), output_path)


def run_pipeline(
    archive_url: str,
    output_path: Path,
    throttle_seconds: float,
    skip_download: bool = False,
    local_archive: Path | None = None,
) -> int:
    """
    Execute the full download → decompress → consolidate workflow.

    Returns:
        Process exit code (0 = success, 1 = failure).
    """
    config = load_config()
    token = config.get("api_token")
    if config.get("api_base_url") and archive_url.startswith("https://example.com"):
        archive_url = f"{config['api_base_url'].rstrip('/')}/archives/sample_batch.zip"

    session = build_session(token if isinstance(token, str) else None)
    staging_dir = Path("data/raw")
    staging_dir.mkdir(parents=True, exist_ok=True)
    archive_path = local_archive or staging_dir / "latest_archive.zip"

    if not skip_download:
        try:
            download_archive(session, archive_url, archive_path)
        except requests.RequestException:
            return 1
        time.sleep(throttle_seconds)

    if not archive_path.is_file():
        logger.error("Archive not found at %s", archive_path)
        return 1

    all_frames: list[pd.DataFrame] = []
    try:
        for member_name, payload in iter_zip_members(archive_path):
            logger.info("Processing member: %s", member_name)
            records = parse_member_payload(member_name, payload)
            batch_frame = consolidate_records(records, source_label=member_name)
            if not batch_frame.empty:
                all_frames.append(batch_frame)
            time.sleep(throttle_seconds)
    except zipfile.BadZipFile:
        return 1

    if not all_frames:
        logger.error("No tabular records extracted from archive.")
        return 1

    master = pd.concat(all_frames, ignore_index=True)

    # Example deduplication: retain the latest row per entity identifier.
    if "entity_id" in master.columns:
        master = master.sort_values("source_batch").drop_duplicates(
            subset=["entity_id"],
            keep="last",
        )

    write_master_table(master, output_path)
    return 0


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Download, decompress, and consolidate remote archive batches.",
    )
    parser.add_argument(
        "--url",
        default=DEFAULT_ARCHIVE_URL,
        help="Remote archive URL (default: mock example endpoint).",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT_PATH,
        help="Output path for the master CSV.",
    )
    parser.add_argument(
        "--throttle",
        type=float,
        default=DEFAULT_THROTTLE_SECONDS,
        help="Seconds to sleep between network or batch operations.",
    )
    parser.add_argument(
        "--skip-download",
        action="store_true",
        help="Skip download and process an existing local archive.",
    )
    parser.add_argument(
        "--local-archive",
        type=Path,
        default=None,
        help="Path to a local ZIP when using --skip-download.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """CLI entry point."""
    args = parse_args(argv)
    return run_pipeline(
        archive_url=args.url,
        output_path=args.output,
        throttle_seconds=args.throttle,
        skip_download=args.skip_download,
        local_archive=args.local_archive,
    )


if __name__ == "__main__":
    sys.exit(main())
