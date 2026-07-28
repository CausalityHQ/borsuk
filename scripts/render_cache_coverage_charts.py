#!/usr/bin/env python3
"""Render observed RAM/disk/backing coverage and latency from mixed-cache runs."""

from __future__ import annotations

import argparse
import csv
import math
import re
import statistics
from collections import defaultdict
from pathlib import Path
from typing import Any

WIDTH = 980
HEIGHT = 590
LEFT = 88
RIGHT = 90
TOP = 82
BOTTOM = 92


def escape(value: str) -> str:
    return (
        value.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


def load_rows(path: Path) -> list[dict[str, Any]]:
    required = {
        "target_hot_query_fraction",
        "query_class",
        "latency_ms",
        "decoded_access_fraction",
        "disk_access_fraction",
        "backing_access_fraction",
    }
    rows: list[dict[str, Any]] = []
    with path.open(newline="") as handle:
        reader = csv.DictReader(handle)
        missing = required.difference(reader.fieldnames or [])
        if missing:
            raise ValueError(f"cache coverage input missing columns: {sorted(missing)}")
        for source in reader:
            rows.append(
                {
                    "target_hot_query_fraction": float(
                        source["target_hot_query_fraction"]
                    ),
                    "query_class": source["query_class"],
                    "latency_ms": float(source["latency_ms"]),
                    "decoded_access_fraction": float(source["decoded_access_fraction"]),
                    "disk_access_fraction": float(source["disk_access_fraction"]),
                    "backing_access_fraction": float(source["backing_access_fraction"]),
                }
            )
    return rows


def percentile(values: list[float], fraction: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    return ordered[max(0, math.ceil(len(ordered) * fraction) - 1)]


def sample_mean(values: list[float]) -> float | None:
    return statistics.fmean(values) if values else None


def sample_stddev(values: list[float]) -> float | None:
    if not values:
        return None
    return statistics.stdev(values) if len(values) > 1 else 0.0


def aggregate_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[float, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[row["target_hot_query_fraction"]].append(row)
    result = []
    for target, group in sorted(grouped.items()):
        count = len(group)
        if count == 0:
            continue
        hot = [row["latency_ms"] for row in group if row["query_class"] == "hot"]
        outside = [
            row["latency_ms"]
            for row in group
            if row["query_class"] == "outside_hot_set"
        ]
        all_latencies = [row["latency_ms"] for row in group]
        result.append(
            {
                "target_hot_fraction": target,
                "decoded_fraction": sum(row["decoded_access_fraction"] for row in group)
                / count,
                "disk_fraction": sum(row["disk_access_fraction"] for row in group)
                / count,
                "backing_fraction": sum(row["backing_access_fraction"] for row in group)
                / count,
                "all_p95_ms": percentile(all_latencies, 0.95),
                "hot_p95_ms": percentile(hot, 0.95),
                "outside_p95_ms": percentile(outside, 0.95),
                "all_mean_ms": sample_mean(all_latencies),
                "all_stddev_ms": sample_stddev(all_latencies),
                "hot_mean_ms": sample_mean(hot),
                "hot_stddev_ms": sample_stddev(hot),
                "outside_mean_ms": sample_mean(outside),
                "outside_stddev_ms": sample_stddev(outside),
            }
        )
    return result


def render(rows: list[dict[str, Any]], title: str) -> str:
    grouped = aggregate_rows(rows)
    if not grouped:
        raise ValueError("cache coverage input contains no rows")
    plot_width = WIDTH - LEFT - RIGHT
    plot_height = HEIGHT - TOP - BOTTOM
    step = plot_width / len(grouped)
    bar_width = min(92.0, step * 0.58)
    latency_values = [
        value
        for row in grouped
        for key in ("all_p95_ms", "hot_p95_ms", "outside_p95_ms")
        if (value := row[key]) is not None
    ]
    latency_values.extend(
        mean + stddev
        for row in grouped
        for prefix in ("all", "hot", "outside")
        if (mean := row[f"{prefix}_mean_ms"]) is not None
        and (stddev := row[f"{prefix}_stddev_ms"]) is not None
    )
    latency_high = max(1.0, max(latency_values) * 1.12)

    def y_fraction(value: float) -> float:
        return TOP + plot_height * (1.0 - value)

    def y_latency(value: float) -> float:
        return TOP + plot_height * (1.0 - value / latency_high)

    pieces = [
        f'<line x1="{LEFT}" y1="{TOP}" x2="{LEFT}" y2="{TOP + plot_height}" class="axis"/>',
        f'<line x1="{LEFT}" y1="{TOP + plot_height}" x2="{WIDTH - RIGHT}" y2="{TOP + plot_height}" class="axis"/>',
        f'<line x1="{WIDTH - RIGHT}" y1="{TOP}" x2="{WIDTH - RIGHT}" y2="{TOP + plot_height}" class="axis"/>',
    ]
    for tick in range(5):
        fraction = tick / 4
        py = y_fraction(fraction)
        pieces.append(
            f'<line x1="{LEFT}" y1="{py:.1f}" x2="{WIDTH - RIGHT}" y2="{py:.1f}" class="grid"/>'
        )
        pieces.append(
            f'<text x="{LEFT - 11}" y="{py + 4:.1f}" text-anchor="end" class="tick">{fraction:.0%}</text>'
        )
        pieces.append(
            f'<text x="{WIDTH - RIGHT + 11}" y="{py + 4:.1f}" class="tick">{latency_high * fraction:.0f}</text>'
        )

    tiers = (
        ("decoded_fraction", "decoded RAM", "#2b6cb0"),
        ("disk_fraction", "disk cache", "#38a169"),
        ("backing_fraction", "backing storage", "#dd6b20"),
    )
    centers = []
    for index, row in enumerate(grouped):
        center = LEFT + step * (index + 0.5)
        centers.append(center)
        cumulative = 0.0
        for key, _, color in tiers:
            value = row[key]
            top = y_fraction(cumulative + value)
            height = plot_height * value
            pieces.append(
                f'<rect x="{center - bar_width / 2:.1f}" y="{top:.1f}" width="{bar_width:.1f}" height="{height:.1f}" fill="{color}"/>'
            )
            cumulative += value
        pieces.append(
            f'<text x="{center:.1f}" y="{TOP + plot_height + 25}" text-anchor="middle" class="tick">{row["target_hot_fraction"]:.0%}</text>'
        )

    lines = (
        (
            "all_p95_ms",
            "all_mean_ms",
            "all_stddev_ms",
            "all-query p95 + μ±σ",
            "#172033",
            "",
        ),
        (
            "hot_p95_ms",
            "hot_mean_ms",
            "hot_stddev_ms",
            "hot-query p95 + μ±σ",
            "#805ad5",
            "6 4",
        ),
        (
            "outside_p95_ms",
            "outside_mean_ms",
            "outside_stddev_ms",
            "outside-hot-set p95 + μ±σ",
            "#d53f8c",
            "2 4",
        ),
    )
    for key, mean_key, stddev_key, _, color, dash in lines:
        points = [
            (center, y_latency(row[key]))
            for center, row in zip(centers, grouped, strict=True)
            if row[key] is not None
        ]
        if len(points) > 1:
            dash_attr = f' stroke-dasharray="{dash}"' if dash else ""
            pieces.append(
                f'<polyline points="{" ".join(f"{x:.1f},{y:.1f}" for x, y in points)}" fill="none" stroke="{color}" stroke-width="3"{dash_attr}/>'
            )
        for center, row in zip(centers, grouped, strict=True):
            if row[key] is None:
                continue
            pieces.append(
                f'<circle cx="{center:.1f}" cy="{y_latency(row[key]):.1f}" r="4" fill="{color}"/>'
            )
            mean = row[mean_key]
            stddev = row[stddev_key]
            if mean is None or stddev is None or stddev <= 0.0:
                continue
            top = y_latency(mean + stddev)
            bottom = y_latency(max(0.0, mean - stddev))
            pieces.append(
                f'<line x1="{center:.1f}" y1="{top:.1f}" x2="{center:.1f}" y2="{bottom:.1f}" '
                f'stroke="{color}" stroke-width="1.4" class="std-whisker"/>'
            )
            pieces.append(
                f'<circle cx="{center:.1f}" cy="{y_latency(mean):.1f}" r="2.8" fill="#fff" '
                f'stroke="{color}" stroke-width="1.4" class="mean-marker"/>'
            )

    legend = []
    legend_x = LEFT
    legend_y = HEIGHT - 34
    for _, label, color in tiers:
        legend.append(
            f'<rect x="{legend_x}" y="{legend_y - 10}" width="14" height="14" fill="{color}"/><text x="{legend_x + 20}" y="{legend_y + 1}" class="legend">{label}</text>'
        )
        legend_x += 145
    for _, _, _, label, color, dash in lines:
        dash_attr = f' stroke-dasharray="{dash}"' if dash else ""
        legend.append(
            f'<line x1="{legend_x}" y1="{legend_y - 3}" x2="{legend_x + 18}" y2="{legend_y - 3}" stroke="{color}" stroke-width="3"{dash_attr}/><text x="{legend_x + 24}" y="{legend_y + 1}" class="legend">{label}</text>'
        )
        legend_x += 145

    return f'''<svg xmlns="http://www.w3.org/2000/svg" width="{WIDTH}" height="{HEIGHT}" viewBox="0 0 {WIDTH} {HEIGHT}">
<style>
  text {{ font-family: ui-sans-serif, system-ui, sans-serif; fill: #172033; }}
  .title {{ font-size: 21px; font-weight: 700; }}
  .subtitle, .tick, .legend {{ font-size: 11px; fill: #526075; }}
  .axis {{ stroke: #7b8798; stroke-width: 1.2; }}
  .grid {{ stroke: #e5e9f0; stroke-width: 1; }}
</style>
<rect width="100%" height="100%" fill="#fff"/>
<text x="{LEFT}" y="29" class="title">{escape(title)} cache residency and latency</text>
<text x="{LEFT}" y="49" class="subtitle">bars: observed data-access fraction · lines: p95 · whiskers: mean ±1 sample SD</text>
{"".join(pieces)}
<text x="{LEFT + plot_width / 2:.1f}" y="{HEIGHT - 62}" text-anchor="middle">requested hot-query fraction</text>
<text x="22" y="{TOP + plot_height / 2:.1f}" text-anchor="middle" transform="rotate(-90 22 {TOP + plot_height / 2:.1f})">observed data-access fraction</text>
<text x="{WIDTH - 18}" y="{TOP + plot_height / 2:.1f}" text-anchor="middle" transform="rotate(90 {WIDTH - 18} {TOP + plot_height / 2:.1f})">latency (ms)</text>
{"".join(legend)}
</svg>'''


def slug(value: str) -> str:
    return re.sub(r"[^a-zA-Z0-9.-]+", "-", value.replace("_", "-")).strip("-")


def main() -> int:
    parser = argparse.ArgumentParser()
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--input", type=Path)
    source.add_argument("--experiment-root", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--title")
    args = parser.parse_args()

    if args.input:
        if args.output is None:
            parser.error("--input requires --output")
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(
            render(load_rows(args.input), args.title or args.input.parent.name)
        )
        return 0

    if args.output_dir is None:
        parser.error("--experiment-root requires --output-dir")
    paths = sorted(args.experiment_root.rglob("bench_cache_coverage.csv"))
    if not paths:
        parser.error(f"no bench_cache_coverage.csv below {args.experiment_root}")
    args.output_dir.mkdir(parents=True, exist_ok=True)
    for path in paths:
        relative = path.parent.relative_to(args.experiment_root)
        title = " / ".join(relative.parts) or "experiment"
        (args.output_dir / f"cache-coverage-{slug(title)}.svg").write_text(
            render(load_rows(path), title)
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
