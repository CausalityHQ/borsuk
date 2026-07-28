#!/usr/bin/env python3
"""Render dependency-free SVG recall@10 versus p95-latency curves."""

from __future__ import annotations

import argparse
import csv
from collections import defaultdict
from pathlib import Path
from typing import Any

WIDTH = 920
HEIGHT = 520
LEFT = 90
RIGHT = 35
TOP = 65
BOTTOM = 75


def escape(value: str) -> str:
    return (
        value.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


def load_series(
    path: Path, dataset: str | None = None
) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    with path.open(newline="") as handle:
        for source in csv.DictReader(handle):
            candidates = source.get("max_candidates") or source.get("candidates")
            p95_ms = source.get("p95_ms") or source.get("uncached_p95_ms")
            if candidates is None or p95_ms is None:
                raise ValueError(
                    "recall input requires max_candidates/candidates and p95_ms/uncached_p95_ms"
                )
            nprobe = source.get("nprobe") or "0"
            label = source.get("label")
            if label is None and not source.get("nprobe"):
                label = f"cand={int(float(candidates))}"
            method = source.get("method") or source.get("mode") or "selected"
            if phase := source.get("phase"):
                method = f"{method} · {phase}"
            row_dataset = source.get("dataset") or dataset
            if not row_dataset:
                raise ValueError("recall input requires a dataset column or --dataset")
            grouped[row_dataset].append(
                {
                    "nprobe": float(nprobe),
                    "max_candidates": float(candidates),
                    "recall_at_10": float(source["recall_at_10"]),
                    "p95_ms": float(p95_ms),
                    "mean_ms": float(source.get("mean_ms") or p95_ms),
                    "stddev_ms": float(source.get("stddev_ms") or 0.0),
                    "label": label,
                    "method": method,
                }
            )
    return {
        dataset: sorted(rows, key=lambda row: (row["recall_at_10"], row["p95_ms"]))
        for dataset, rows in sorted(grouped.items())
    }


def render(
    dataset: str,
    rows: list[dict[str, Any]],
    subtitle: str = "measured cache phase · selected search engine · exact rerank",
    effectiveness_label: str = "recall@10",
    title_metric: str = "recall",
) -> str:
    if not rows:
        raise ValueError(f"{dataset} has no recall/latency rows")
    plot_width = WIDTH - LEFT - RIGHT
    plot_height = HEIGHT - TOP - BOTTOM
    x_low = max(0.0, min(row["recall_at_10"] for row in rows) - 0.02)
    x_high = min(1.0, max(row["recall_at_10"] for row in rows) + 0.01)
    if x_high <= x_low:
        x_high = min(1.0, x_low + 0.01)
    y_high = max(
        1.0,
        max(
            max(
                row["p95_ms"],
                row.get("mean_ms", row["p95_ms"]) + row.get("stddev_ms", 0.0),
            )
            for row in rows
        )
        * 1.12,
    )

    def x(value: float) -> float:
        return LEFT + (value - x_low) / (x_high - x_low) * plot_width

    def y(value: float) -> float:
        return TOP + plot_height - value / y_high * plot_height

    pieces = [
        f'<line x1="{LEFT}" y1="{TOP}" x2="{LEFT}" y2="{TOP + plot_height}" class="axis"/>',
        f'<line x1="{LEFT}" y1="{TOP + plot_height}" x2="{WIDTH - RIGHT}" y2="{TOP + plot_height}" class="axis"/>',
    ]
    for tick in range(6):
        fraction = tick / 5
        xv = x_low + fraction * (x_high - x_low)
        px = LEFT + fraction * plot_width
        pieces.append(
            f'<text x="{px:.1f}" y="{TOP + plot_height + 24}" text-anchor="middle" class="tick">{xv:.3f}</text>'
        )
        yv = y_high * fraction
        py = TOP + plot_height - fraction * plot_height
        pieces.append(
            f'<text x="{LEFT - 12}" y="{py + 4:.1f}" text-anchor="end" class="tick">{yv:.1f}</text>'
        )
        pieces.append(
            f'<line x1="{LEFT}" y1="{py:.1f}" x2="{WIDTH - RIGHT}" y2="{py:.1f}" class="grid"/>'
        )
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[str(row.get("method") or "selected")].append(row)
    palette = ("#2563eb", "#dd6b20", "#2f855a", "#805ad5", "#d53f8c")
    for series_index, (method, series_rows) in enumerate(sorted(grouped.items())):
        color = palette[series_index % len(palette)]
        points = " ".join(
            f"{x(row['recall_at_10']):.1f},{y(row['p95_ms']):.1f}"
            for row in series_rows
        )
        pieces.append(
            f'<polyline points="{points}" fill="none" stroke="{color}" stroke-width="3"/>'
        )
        for row in series_rows:
            px = x(row["recall_at_10"])
            py = y(row["p95_ms"])
            mean_ms = row.get("mean_ms", row["p95_ms"])
            stddev_ms = row.get("stddev_ms", 0.0)
            mean_y = y(mean_ms)
            std_low_y = y(max(0.0, mean_ms - stddev_ms))
            std_high_y = y(mean_ms + stddev_ms)
            if stddev_ms > 0.0:
                pieces.append(
                    f'<line x1="{px:.1f}" y1="{std_high_y:.1f}" x2="{px:.1f}" y2="{std_low_y:.1f}" '
                    f'stroke="{color}" stroke-width="1.5" class="std-whisker"/>'
                )
                pieces.append(
                    f'<line x1="{px - 4:.1f}" y1="{std_high_y:.1f}" x2="{px + 4:.1f}" y2="{std_high_y:.1f}" '
                    f'stroke="{color}" stroke-width="1.5" class="std-whisker"/>'
                )
                pieces.append(
                    f'<line x1="{px - 4:.1f}" y1="{std_low_y:.1f}" x2="{px + 4:.1f}" y2="{std_low_y:.1f}" '
                    f'stroke="{color}" stroke-width="1.5" class="std-whisker"/>'
                )
                pieces.append(
                    f'<circle cx="{px:.1f}" cy="{mean_y:.1f}" r="3" fill="#fff" stroke="{color}" '
                    f'stroke-width="1.5" class="mean-marker"/>'
                )
            pieces.append(f'<circle cx="{px:.1f}" cy="{py:.1f}" r="5" fill="{color}"/>')
            label = row.get("label")
            if label is None:
                label = (
                    f"nprobe={int(row['nprobe'])}, cand={int(row['max_candidates'])}"
                )
            if not label:
                continue
            if px > WIDTH - RIGHT - 150:
                label_x = px - 8
                anchor = "end"
            else:
                label_x = px + 8
                anchor = "start"
            pieces.append(
                f'<text x="{label_x:.1f}" y="{py - 8:.1f}" text-anchor="{anchor}" class="label">{escape(str(label))}</text>'
            )
        if len(grouped) > 1:
            legend_x = WIDTH - RIGHT - 115
            legend_y = 22 + series_index * 17
            pieces.append(
                f'<line x1="{legend_x}" y1="{legend_y}" x2="{legend_x + 20}" '
                f'y2="{legend_y}" stroke="{color}" stroke-width="3"/>'
            )
            pieces.append(
                f'<text x="{legend_x + 26}" y="{legend_y + 4}" class="legend">{escape(method)}</text>'
            )
    return f'''<svg xmlns="http://www.w3.org/2000/svg" width="{WIDTH}" height="{HEIGHT}" viewBox="0 0 {WIDTH} {HEIGHT}">
<style>
  text {{ font-family: ui-sans-serif, system-ui, sans-serif; fill: #172033; }}
  .title {{ font-size: 22px; font-weight: 700; }}
  .subtitle, .tick {{ font-size: 12px; fill: #526075; }}
  .label {{ font-size: 11px; font-weight: 600; }}
  .legend {{ font-size: 11px; font-weight: 650; }}
  .axis {{ stroke: #7b8798; stroke-width: 1.2; }}
  .grid {{ stroke: #e5e9f0; stroke-width: 1; }}
</style>
<rect width="100%" height="100%" fill="#fff"/>
<text x="{LEFT}" y="30" class="title">{escape(dataset)} {escape(title_metric)}/latency curve</text>
<text x="{LEFT}" y="50" class="subtitle">{escape(subtitle)} · whiskers: per-query mean ±1 sample SD</text>
{"".join(pieces)}
<text x="{LEFT + plot_width / 2:.1f}" y="{HEIGHT - 20}" text-anchor="middle">{escape(effectiveness_label)}</text>
<text x="22" y="{TOP + plot_height / 2:.1f}" text-anchor="middle" transform="rotate(-90 22 {TOP + plot_height / 2:.1f})">latency (ms)</text>
</svg>'''


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument(
        "--dataset",
        help="dataset name for production_bench CSVs that do not repeat it per row",
    )
    parser.add_argument(
        "--subtitle",
        default="measured cache phase · selected search engine · exact rerank",
    )
    args = parser.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    for dataset, rows in load_series(args.input, dataset=args.dataset).items():
        (args.output_dir / f"recall-latency-{dataset}.svg").write_text(
            render(dataset, rows, args.subtitle)
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
