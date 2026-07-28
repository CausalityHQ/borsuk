#!/usr/bin/env python3
"""Render dependency-free SVG CPU/RAM/disk timelines from benchmark telemetry."""

from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path

WIDTH = 1100
PANEL_HEIGHT = 180
LEFT = 78
RIGHT = 24
TOP = 55
FINAL_ONLY_FIELDS = {"child_cpu_seconds", "child_max_rss_bytes"}


def load_rows(path: Path) -> list[dict[str, float]]:
    with path.open(newline="") as handle:
        return [
            {
                key: 0.0 if key in FINAL_ONLY_FIELDS and value == "" else float(value)
                for key, value in row.items()
            }
            for row in csv.DictReader(handle)
        ]


def downsample(
    rows: list[dict[str, float]], limit: int = 1200
) -> list[dict[str, float]]:
    if len(rows) <= limit:
        return rows
    step = (len(rows) - 1) / (limit - 1)
    return [rows[round(index * step)] for index in range(limit)]


def escape(value: str) -> str:
    return (
        value.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


def polyline(
    rows: list[dict[str, float]],
    field: str,
    top: float,
    height: float,
    time_max: float,
    value_max: float,
    color: str,
) -> str:
    plot_width = WIDTH - LEFT - RIGHT
    points = " ".join(
        f"{LEFT + row['elapsed_ms'] / time_max * plot_width:.1f},"
        f"{top + height - row[field] / value_max * height:.1f}"
        for row in rows
    )
    return (
        f'<polyline fill="none" stroke="{color}" stroke-width="2" points="{points}"/>'
    )


def panel(
    rows: list[dict[str, float]],
    top: float,
    title: str,
    series: list[tuple[str, str, str, float]],
    time_max: float,
) -> str:
    height = PANEL_HEIGHT - 42
    value_max = max(
        1.0, *(row[field] * scale for field, _, _, scale in series for row in rows)
    )
    pieces = [
        f'<text x="{LEFT}" y="{top - 14}" class="panel-title">{escape(title)}</text>',
        f'<line x1="{LEFT}" y1="{top}" x2="{LEFT}" y2="{top + height}" class="axis"/>',
        f'<line x1="{LEFT}" y1="{top + height}" x2="{WIDTH - RIGHT}" y2="{top + height}" class="axis"/>',
        f'<text x="{LEFT - 10}" y="{top + 5}" text-anchor="end" class="tick">{value_max:.1f}</text>',
        f'<text x="{LEFT - 10}" y="{top + height + 5}" text-anchor="end" class="tick">0</text>',
    ]
    scaled_rows = [dict(row) for row in rows]
    for field, _label, color, scale in series:
        scaled = f"__{field}"
        for row, source in zip(scaled_rows, rows, strict=True):
            row[scaled] = source[field] * scale
        pieces.append(
            polyline(scaled_rows, scaled, top, height, time_max, value_max, color)
        )
    legend_x = LEFT
    for _, label, color, _ in series:
        pieces.append(
            f'<line x1="{legend_x}" y1="{top + height + 25}" x2="{legend_x + 20}" '
            f'y2="{top + height + 25}" stroke="{color}" stroke-width="3"/>'
        )
        pieces.append(
            f'<text x="{legend_x + 26}" y="{top + height + 29}" class="legend">{escape(label)}</text>'
        )
        legend_x += 150
    return "\n".join(pieces)


def render(path: Path, title: str) -> str:
    rows = downsample(load_rows(path))
    if not rows:
        raise ValueError(f"{path} contains no telemetry rows")
    time_max = max(1.0, rows[-1]["elapsed_ms"])
    mib = 1 / (1024 * 1024)
    disk_series = [
        ("cache_disk_bytes", "cache size", "#38a169", mib),
        ("process_read_bytes", "disk read", "#805ad5", mib),
        ("process_write_bytes", "disk write", "#d53f8c", mib),
    ]
    if "scratch_disk_bytes" in rows[0]:
        disk_series.insert(1, ("scratch_disk_bytes", "build scratch", "#319795", mib))
    panels = [
        panel(
            rows,
            TOP + 35,
            "CPU utilization (%)",
            [("cpu_percent", "CPU", "#dd6b20", 1)],
            time_max,
        ),
        panel(
            rows,
            TOP + PANEL_HEIGHT + 35,
            "Process memory (MiB)",
            [("rss_bytes", "RSS", "#2b6cb0", mib)],
            time_max,
        ),
        panel(
            rows,
            TOP + PANEL_HEIGHT * 2 + 35,
            "Disk and cache footprint (MiB, cumulative)",
            disk_series,
            time_max,
        ),
    ]
    if {
        "network_receive_bytes",
        "network_transmit_bytes",
    }.issubset(rows[0]):
        panels.append(
            panel(
                rows,
                TOP + PANEL_HEIGHT * 3 + 35,
                "Network I/O (MiB, host-interface cumulative)",
                [
                    ("network_receive_bytes", "received", "#2f855a", mib),
                    ("network_transmit_bytes", "transmitted", "#c05621", mib),
                ],
                time_max,
            )
        )
    total_height = TOP + PANEL_HEIGHT * len(panels) + 55
    return f'''<svg xmlns="http://www.w3.org/2000/svg" width="{WIDTH}" height="{total_height}" viewBox="0 0 {WIDTH} {total_height}">
<style>
  text {{ font-family: ui-sans-serif, system-ui, sans-serif; fill: #1a202c; }}
  .title {{ font-size: 22px; font-weight: 700; }}
  .subtitle {{ font-size: 12px; fill: #4a5568; }}
  .panel-title {{ font-size: 14px; font-weight: 650; }}
  .tick, .legend {{ font-size: 11px; fill: #4a5568; }}
  .axis {{ stroke: #a0aec0; stroke-width: 1; }}
</style>
<rect width="100%" height="100%" fill="#fff"/>
<text x="{LEFT}" y="30" class="title">{escape(title)}</text>
<text x="{LEFT}" y="49" class="subtitle">elapsed {time_max / 1000:.1f}s · sampled resource envelope</text>
{"".join(panels)}
</svg>'''


def render_experiment_tree(
    experiment_root: Path, output_dir: Path, prefix: str
) -> tuple[int, int]:
    paths = sorted(experiment_root.rglob("resources.csv"))
    output_dir.mkdir(parents=True, exist_ok=True)
    rendered = 0
    skipped = 0
    for path in paths:
        relative = path.parent.relative_to(experiment_root)
        slug = "-".join(relative.parts).replace("_", "-") or "experiment"
        output = output_dir / f"{prefix}-{slug}.svg"
        try:
            svg = render(path, f"{prefix}: {' / '.join(relative.parts)}")
        except ValueError as error:
            print(f"skipping incomplete resource artifact: {error}", file=sys.stderr)
            skipped += 1
            continue
        output.write_text(svg)
        rendered += 1
    return rendered, skipped


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--experiment-root", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--prefix", default="resources")
    args = parser.parse_args()
    paths = sorted(args.experiment_root.rglob("resources.csv"))
    if not paths:
        parser.error(f"no resources.csv below {args.experiment_root}")
    rendered, _ = render_experiment_tree(
        args.experiment_root, args.output_dir, args.prefix
    )
    if rendered == 0:
        parser.error(f"all resources.csv below {args.experiment_root} are empty")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
