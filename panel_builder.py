#!/usr/bin/env python3
"""
Generic panel construction utility.

Reads a source table, computes group-level baseline statistics, derives a
deviation metric relative to that baseline, and exports a panel-ready dataset
suitable for downstream econometric estimation.
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
# Default column names (override via CLI for schema portability)
# ---------------------------------------------------------------------------
DEFAULT_INPUT_PATH = Path("data/processed/master_table.csv")
DEFAULT_OUTPUT_PATH = Path("data/processed/panel_table.csv")
DEFAULT_ENTITY_COL = "entity_id"
DEFAULT_GROUP_COL = "group_id"
DEFAULT_TIME_COL = "period_id"
DEFAULT_VALUE_COL = "metric_value"
DEFAULT_DEVIATION_COL = "metric_deviation"
DEFAULT_BASELINE_COL = "group_baseline"
CHUNK_SIZE = 50_000

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def validate_columns(frame: pd.DataFrame, required: list[str]) -> None:
    """Raise a clear error when expected columns are absent."""
    missing = [col for col in required if col not in frame.columns]
    if missing:
        raise ValueError(f"Input table is missing required columns: {missing}")


def compute_group_baselines(
    frame: pd.DataFrame,
    group_col: str,
    time_col: str,
    value_col: str,
    baseline_col: str,
) -> pd.DataFrame:
    """
    Compute the median baseline metric for each group-time cell.

    The baseline represents the typical level within a reference group for a
    given period. Median is used for robustness to outliers; swap with mean
  if your application requires it.
    """
    working = frame[[group_col, time_col, value_col]].copy()
    working[value_col] = pd.to_numeric(working[value_col], errors="coerce")
    working = working.dropna(subset=[group_col, time_col, value_col])

    baselines = (
        working.groupby([group_col, time_col], as_index=False)[value_col]
        .median()
        .rename(columns={value_col: baseline_col})
    )
    return baselines


def attach_deviation(
    frame: pd.DataFrame,
    baselines: pd.DataFrame,
    group_col: str,
    time_col: str,
    value_col: str,
    baseline_col: str,
    deviation_col: str,
) -> pd.DataFrame:
    """
    Merge baselines and compute deviation = individual value − group baseline.

    Positive deviations indicate values above the group-period norm; negative
    deviations indicate values below the norm.
    """
    merged = frame.merge(baselines, on=[group_col, time_col], how="left")
    merged[value_col] = pd.to_numeric(merged[value_col], errors="coerce")
    merged[deviation_col] = merged[value_col] - merged[baseline_col]
    return merged


def build_panel(
    input_path: Path,
    output_path: Path,
    entity_col: str,
    group_col: str,
    time_col: str,
    value_col: str,
    deviation_col: str,
    baseline_col: str,
    use_chunks: bool = False,
) -> pd.DataFrame:
    """
    End-to-end panel build: load → baseline → deviation → persist.

    Args:
        input_path: Source CSV path.
        output_path: Destination CSV path.
        entity_col: Unit identifier column.
        group_col: Grouping dimension for baseline calculation.
        time_col: Time index column.
        value_col: Raw metric column.
        deviation_col: Output deviation column name.
        baseline_col: Output baseline column name.
        use_chunks: Stream large files in chunks (two-pass algorithm).

    Returns:
        The constructed panel DataFrame.
    """
    required = [entity_col, group_col, time_col, value_col]

    if use_chunks:
        return _build_panel_chunked(
            input_path=input_path,
            output_path=output_path,
            required=required,
            entity_col=entity_col,
            group_col=group_col,
            time_col=time_col,
            value_col=value_col,
            deviation_col=deviation_col,
            baseline_col=baseline_col,
        )

    frame = pd.read_csv(input_path, low_memory=False)
    validate_columns(frame, required)

    logger.info("Loaded %s rows from %s", f"{len(frame):,}", input_path)
    baselines = compute_group_baselines(frame, group_col, time_col, value_col, baseline_col)
    panel = attach_deviation(
        frame,
        baselines,
        group_col,
        time_col,
        value_col,
        baseline_col,
        deviation_col,
    )

    _write_panel(panel, output_path, entity_col, deviation_col)
    return panel


def _build_panel_chunked(
    input_path: Path,
    output_path: Path,
    required: list[str],
    entity_col: str,
    group_col: str,
    time_col: str,
    value_col: str,
    deviation_col: str,
    baseline_col: str,
) -> pd.DataFrame:
    """Two-pass chunked implementation for memory-constrained environments."""
    sums: dict[tuple, float] = {}
    counts: dict[tuple, int] = {}

    total_rows = 0
    for chunk in pd.read_csv(input_path, chunksize=CHUNK_SIZE, low_memory=False):
        validate_columns(chunk, required)
        total_rows += len(chunk)
        chunk[value_col] = pd.to_numeric(chunk[value_col], errors="coerce")
        valid = chunk.dropna(subset=[group_col, time_col, value_col])
        grouped = valid.groupby([group_col, time_col])[value_col]
        for key, series in grouped:
            cell = (key[0], key[1]) if isinstance(key, tuple) else (key,)
            sums[cell] = sums.get(cell, 0.0) + float(series.sum())
            counts[cell] = counts.get(cell, 0) + int(series.count())

    logger.info("Pass 1 complete: scanned %s rows", f"{total_rows:,}")

    baselines = pd.DataFrame(
        [
            {group_col: k[0], time_col: k[1], baseline_col: sums[k] / counts[k]}
            for k in sums
        ]
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    if output_path.exists():
        output_path.unlink()

    header_written = False
    panel_parts: list[pd.DataFrame] = []

    for chunk in pd.read_csv(input_path, chunksize=CHUNK_SIZE, low_memory=False):
        panel_chunk = attach_deviation(
            chunk,
            baselines,
            group_col,
            time_col,
            value_col,
            baseline_col,
            deviation_col,
        )
        panel_chunk.to_csv(
            output_path,
            mode="a",
            header=not header_written,
            index=False,
        )
        header_written = True
        panel_parts.append(panel_chunk)

    panel = pd.concat(panel_parts, ignore_index=True)
    _log_summary(panel, entity_col, deviation_col)
    logger.info("Wrote panel to %s", output_path)
    return panel


def _write_panel(panel: pd.DataFrame, output_path: Path, entity_col: str, deviation_col: str) -> None:
    """Persist panel and emit summary statistics."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    panel.to_csv(output_path, index=False)
    _log_summary(panel, entity_col, deviation_col)
    logger.info("Wrote panel to %s", output_path)


def _log_summary(panel: pd.DataFrame, entity_col: str, deviation_col: str) -> None:
    """Print basic validation metrics for the constructed panel."""
    n_units = panel[entity_col].nunique() if entity_col in panel.columns else np.nan
    dev = pd.to_numeric(panel.get(deviation_col), errors="coerce").dropna()
    logger.info("Panel rows: %s | Unique units: %s", f"{len(panel):,}", f"{n_units:,}")
    if len(dev):
        logger.info(
            "Deviation summary — mean: %.4f | std: %.4f | median: %.4f",
            dev.mean(),
            dev.std(),
            dev.median(),
        )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description="Build a panel with baseline deviation metrics.")
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT_PATH)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT_PATH)
    parser.add_argument("--entity-col", default=DEFAULT_ENTITY_COL)
    parser.add_argument("--group-col", default=DEFAULT_GROUP_COL)
    parser.add_argument("--time-col", default=DEFAULT_TIME_COL)
    parser.add_argument("--value-col", default=DEFAULT_VALUE_COL)
    parser.add_argument("--deviation-col", default=DEFAULT_DEVIATION_COL)
    parser.add_argument("--baseline-col", default=DEFAULT_BASELINE_COL)
    parser.add_argument(
        "--chunked",
        action="store_true",
        help="Use chunked reads for large input files.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """CLI entry point."""
    args = parse_args(argv)
    try:
        build_panel(
            input_path=args.input,
            output_path=args.output,
            entity_col=args.entity_col,
            group_col=args.group_col,
            time_col=args.time_col,
            value_col=args.value_col,
            deviation_col=args.deviation_col,
            baseline_col=args.baseline_col,
            use_chunks=args.chunked,
        )
    except (FileNotFoundError, ValueError) as exc:
        logger.error("%s", exc)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
