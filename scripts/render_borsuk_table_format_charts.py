#!/usr/bin/env python3
"""Render real BORSUK table-format replay summaries as dependency-free SVG."""

from __future__ import annotations

import argparse
import csv
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping

FORMAT_ORDER = ("parquet/source", "vortex/default", "vortex/compact")
WORKLOAD_ORDER = (
    "projection",
    "point_lookup",
    "range_lookup",
    "filtered_scan",
    "full_scan",
)
COLORS = {
    "parquet/source": "#2563eb",
    "vortex/default": "#0f9f75",
    "vortex/compact": "#d97706",
}
DISPLAY_NAMES = {
    "parquet/source": "Parquet",
    "vortex/default": "Vortex default",
    "vortex/compact": "Vortex compact",
}
SHORT_NAMES = {
    "parquet/source": "P",
    "vortex/default": "Vd",
    "vortex/compact": "Vc",
}


@dataclass(frozen=True)
class LatencyDistribution:
    object: str
    samples: int
    mean_ms: float
    stddev_ms: float
    p50_ms: float
    p95_ms: float
    p99_ms: float


@dataclass(frozen=True)
class SummaryData:
    storage_bytes: Mapping[str, int]
    workloads: tuple[str, ...]
    formats: tuple[str, ...]
    latencies: Mapping[str, Mapping[str, tuple[LatencyDistribution, ...]]]


def escape(value: object) -> str:
    return (
        str(value)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


def _ordered(values: set[str], preferred: tuple[str, ...]) -> tuple[str, ...]:
    return tuple(item for item in preferred if item in values) + tuple(
        sorted(values.difference(preferred))
    )


def _finite_nonnegative(row: dict[str, str], field: str) -> float:
    try:
        value = float(row[field])
    except (KeyError, TypeError, ValueError) as error:
        raise ValueError(f"invalid or missing {field}") from error
    if not math.isfinite(value) or value < 0:
        raise ValueError(f"{field} must be finite and nonnegative")
    return value


def load_summary(path: Path) -> SummaryData:
    """Load only complete materialized-Arrow cells without aggregating percentiles."""
    required = {
        "object",
        "format",
        "layout",
        "execution_mode",
        "workload",
        "status",
        "samples",
        "mean_ms",
        "stddev_ms",
        "p50_ms",
        "p95_ms",
        "p99_ms",
        "bytes",
    }
    with path.open(newline="") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames is None or not required.issubset(reader.fieldnames):
            missing = sorted(required.difference(reader.fieldnames or ()))
            raise ValueError(f"summary CSV is missing required fields: {missing}")
        rows = list(reader)
    if not rows:
        raise ValueError("summary CSV is empty")

    storage_objects: dict[tuple[str, str], int] = {}
    latency_lists: dict[str, dict[str, list[LatencyDistribution]]] = {}
    formats: set[str] = set()
    workloads: set[str] = set()
    for row in rows:
        if row["execution_mode"] != "materialized_arrow":
            raise ValueError("table-format charts accept materialized_arrow rows only")
        if row["status"] != "complete":
            raise ValueError("table-format charts accept complete rows only")
        label = f"{row['format']}/{row['layout']}"
        object_name = row["object"]
        workload = row["workload"]
        if not object_name or not workload:
            raise ValueError("object and workload must be non-empty")
        try:
            samples = int(row["samples"])
            file_bytes = int(row["bytes"])
        except (TypeError, ValueError) as error:
            raise ValueError("samples and bytes must be integers") from error
        if samples <= 0 or file_bytes < 0:
            raise ValueError("samples must be positive and bytes nonnegative")

        storage_key = (object_name, label)
        prior_bytes = storage_objects.setdefault(storage_key, file_bytes)
        if prior_bytes != file_bytes:
            raise ValueError(f"inconsistent bytes for {object_name} in {label}")
        point = LatencyDistribution(
            object=object_name,
            samples=samples,
            mean_ms=_finite_nonnegative(row, "mean_ms"),
            stddev_ms=_finite_nonnegative(row, "stddev_ms"),
            p50_ms=_finite_nonnegative(row, "p50_ms"),
            p95_ms=_finite_nonnegative(row, "p95_ms"),
            p99_ms=_finite_nonnegative(row, "p99_ms"),
        )
        latency_lists.setdefault(workload, {}).setdefault(label, []).append(point)
        formats.add(label)
        workloads.add(workload)

    ordered_formats = _ordered(formats, FORMAT_ORDER)
    ordered_workloads = _ordered(workloads, WORKLOAD_ORDER)
    storage_bytes = {
        label: sum(
            size
            for (_, object_format), size in storage_objects.items()
            if object_format == label
        )
        for label in ordered_formats
    }
    latencies = {
        workload: {
            label: tuple(
                sorted(
                    latency_lists.get(workload, {}).get(label, []),
                    key=lambda point: point.object,
                )
            )
            for label in ordered_formats
        }
        for workload in ordered_workloads
    }
    return SummaryData(
        storage_bytes,
        ordered_workloads,
        ordered_formats,
        latencies,
    )


def _human_bytes(value: int) -> str:
    units = ("B", "KiB", "MiB", "GiB", "TiB")
    amount = float(value)
    unit = units[0]
    for unit in units:
        if amount < 1024.0 or unit == units[-1]:
            break
        amount /= 1024.0
    precision = 0 if unit == "B" else 2
    return f"{amount:.{precision}f} {unit}"


def render(data: SummaryData, *, title: str = "BORSUK table-format replay") -> str:
    if not data.formats or not data.workloads:
        raise ValueError("chart requires at least one format and workload")

    left = 92
    right = 38
    category_width = 76
    category_count = len(data.formats) * len(data.workloads)
    width = max(1080, left + right + category_count * category_width)
    storage_top = 82
    storage_height = 178
    latency_top = 390
    latency_height = 455
    height = 950
    plot_right = width - right
    plot_width = plot_right - left
    pieces: list[str] = []

    max_storage = max(data.storage_bytes.values(), default=1) or 1
    storage_plot_width = min(plot_width, 820)
    bar_gap = 20
    bar_width = min(
        150,
        (storage_plot_width - bar_gap * (len(data.formats) - 1)) / len(data.formats),
    )
    for index, label in enumerate(data.formats):
        value = data.storage_bytes[label]
        x = left + index * (bar_width + bar_gap)
        height_px = storage_height * value / max_storage
        y = storage_top + storage_height - height_px
        color = COLORS.get(label, "#667085")
        pieces.extend(
            [
                f'<rect x="{x:.1f}" y="{y:.1f}" width="{bar_width:.1f}" '
                f'height="{height_px:.1f}" rx="4" fill="{color}" class="storage-bar"/>',
                f'<text x="{x + bar_width / 2:.1f}" y="{y - 9:.1f}" '
                f'text-anchor="middle" class="value">{escape(_human_bytes(value))}</text>',
                f'<text x="{x + bar_width / 2:.1f}" y="{storage_top + storage_height + 22}" '
                f'text-anchor="middle" class="category">{escape(DISPLAY_NAMES.get(label, label))}</text>',
            ]
        )

    all_values = [
        value
        for workload in data.workloads
        for label in data.formats
        for point in data.latencies[workload][label]
        for value in (
            max(0.0, point.mean_ms - point.stddev_ms),
            point.mean_ms + point.stddev_ms,
            point.p50_ms,
            point.p95_ms,
            point.p99_ms,
        )
        if value > 0
    ]
    if not all_values:
        raise ValueError("chart requires at least one positive latency")
    log_low = math.floor(math.log10(min(all_values)))
    log_high = math.ceil(math.log10(max(all_values)))
    if log_high <= log_low:
        log_high = log_low + 1

    def latency_y(value: float) -> float:
        bounded = max(value, 10**log_low)
        fraction = (math.log10(bounded) - log_low) / (log_high - log_low)
        return latency_top + latency_height * (1.0 - fraction)

    pieces.extend(
        [
            f'<line x1="{left}" y1="{latency_top}" x2="{left}" '
            f'y2="{latency_top + latency_height}" class="axis"/>',
            f'<line x1="{left}" y1="{latency_top + latency_height}" '
            f'x2="{plot_right}" y2="{latency_top + latency_height}" class="axis"/>',
        ]
    )
    for exponent in range(log_low, log_high + 1):
        value = 10**exponent
        y = latency_y(value)
        pieces.extend(
            [
                f'<line x1="{left}" y1="{y:.1f}" x2="{plot_right}" '
                f'y2="{y:.1f}" class="grid"/>',
                f'<text x="{left - 12}" y="{y + 4:.1f}" text-anchor="end" '
                f'class="tick">{escape(f"{value:g}")}</text>',
            ]
        )

    group_width = plot_width / len(data.workloads)
    format_width = group_width / len(data.formats)
    for workload_index, workload in enumerate(data.workloads):
        group_left = left + workload_index * group_width
        if workload_index:
            pieces.append(
                f'<line x1="{group_left:.1f}" y1="{latency_top}" '
                f'x2="{group_left:.1f}" y2="{latency_top + latency_height}" '
                f'class="group-divider"/>'
            )
        pieces.append(
            f'<text x="{group_left + group_width / 2:.1f}" '
            f'y="{latency_top + latency_height + 49}" text-anchor="middle" '
            f'class="workload">{escape(workload)}</text>'
        )
        for format_index, label in enumerate(data.formats):
            center = group_left + (format_index + 0.5) * format_width
            points = data.latencies[workload][label]
            pieces.append(
                f'<text x="{center:.1f}" y="{latency_top + latency_height + 22}" '
                f'text-anchor="middle" class="format-short">'
                f"{escape(SHORT_NAMES.get(label, label))}</text>"
            )
            for point_index, point in enumerate(points):
                if len(points) == 1:
                    jitter = 0.0
                else:
                    jitter = ((point_index / (len(points) - 1)) - 0.5) * min(
                        30.0, format_width * 0.42
                    )
                x = center + jitter
                color = COLORS.get(label, "#667085")
                mean_low = max(0.0, point.mean_ms - point.stddev_ms)
                title_text = (
                    f"{point.object} · {workload} · "
                    f"{DISPLAY_NAMES.get(label, label)} · "
                    f"mean {point.mean_ms:.3f} ± {point.stddev_ms:.3f} ms · "
                    f"p50 {point.p50_ms:.3f} · p95 {point.p95_ms:.3f} · "
                    f"p99 {point.p99_ms:.3f} ms · n={point.samples}"
                )
                p50_y = latency_y(point.p50_ms)
                p95_y = latency_y(point.p95_ms)
                p99_y = latency_y(point.p99_ms)
                mean_y = latency_y(point.mean_ms)
                low_y = latency_y(mean_low)
                high_y = latency_y(point.mean_ms + point.stddev_ms)
                pieces.append(f"<g><title>{escape(title_text)}</title>")
                pieces.append(
                    f'<line x1="{x:.1f}" y1="{p50_y:.1f}" x2="{x:.1f}" '
                    f'y2="{p99_y:.1f}" stroke="{color}" class="percentile-range"/>'
                )
                pieces.append(
                    f'<line x1="{x - 3:.1f}" y1="{low_y:.1f}" '
                    f'x2="{x + 3:.1f}" y2="{low_y:.1f}" '
                    f'stroke="{color}" class="mean-std"/>'
                )
                pieces.append(
                    f'<line x1="{x:.1f}" y1="{high_y:.1f}" x2="{x:.1f}" '
                    f'y2="{low_y:.1f}" stroke="{color}" class="mean-std"/>'
                )
                pieces.append(
                    f'<line x1="{x - 3:.1f}" y1="{high_y:.1f}" '
                    f'x2="{x + 3:.1f}" y2="{high_y:.1f}" '
                    f'stroke="{color}" class="mean-std"/>'
                )
                pieces.append(
                    f'<rect x="{x - 2.5:.1f}" y="{mean_y - 2.5:.1f}" '
                    f'width="5" height="5" fill="#fff" stroke="{color}" '
                    f'class="mean-marker"/>'
                )
                pieces.append(
                    f'<circle cx="{x:.1f}" cy="{p50_y:.1f}" r="2.8" '
                    f'fill="{color}" class="p50-marker"/>'
                )
                pieces.append(
                    f'<path d="M {x:.1f} {p95_y - 3.5:.1f} '
                    f"L {x + 3.5:.1f} {p95_y:.1f} "
                    f"L {x:.1f} {p95_y + 3.5:.1f} "
                    f'L {x - 3.5:.1f} {p95_y:.1f} Z" fill="{color}" '
                    f'class="p95-marker"/>'
                )
                pieces.append(
                    f'<path d="M {x:.1f} {p99_y - 4:.1f} '
                    f"L {x + 4:.1f} {p99_y + 3:.1f} "
                    f'L {x - 4:.1f} {p99_y + 3:.1f} Z" fill="{color}" '
                    f'class="p99-marker"/></g>'
                )

    legend_x = width - 350
    for index, label in enumerate(data.formats):
        y = 92 + index * 22
        color = COLORS.get(label, "#667085")
        pieces.append(
            f'<rect x="{legend_x}" y="{y - 9}" width="12" height="12" '
            f'rx="2" fill="{color}"/>'
        )
        pieces.append(
            f'<text x="{legend_x + 19}" y="{y + 1}" class="legend">'
            f"{escape(DISPLAY_NAMES.get(label, label))}</text>"
        )

    return f'''<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">
<style>
  text {{ font-family: ui-sans-serif, system-ui, sans-serif; fill: #172033; }}
  .title {{ font-size: 23px; font-weight: 750; }}
  .subtitle, .tick {{ font-size: 12px; fill: #5d687a; }}
  .panel-title {{ font-size: 17px; font-weight: 700; }}
  .category, .workload {{ font-size: 12px; font-weight: 650; }}
  .format-short, .legend {{ font-size: 11px; font-weight: 650; }}
  .value {{ font-size: 11px; font-weight: 650; }}
  .axis {{ stroke: #7b8798; stroke-width: 1.2; }}
  .grid {{ stroke: #e5e9f0; stroke-width: 1; }}
  .group-divider {{ stroke: #cbd2dc; stroke-width: 1; stroke-dasharray: 4 4; }}
  .percentile-range {{ stroke-width: 1.2; opacity: .55; }}
  .mean-std {{ stroke-width: 1.4; opacity: .8; }}
</style>
<rect width="100%" height="100%" fill="#fff"/>
<text x="{left}" y="31" class="title">{escape(title)}</text>
<text x="{left}" y="53" class="subtitle">corrected real-artifact replay · materialized_arrow only</text>
<text x="{left}" y="76" class="panel-title">Storage footprint</text>
<text x="{legend_x}" y="69" class="subtitle">formats</text>
{"".join(pieces)}
<text x="{left}" y="{latency_top - 42}" class="panel-title">Latency distributions by workload</text>
<text x="{left}" y="{latency_top - 21}" class="subtitle">one glyph per object summary · no percentile aggregation · log scale</text>
<text x="25" y="{latency_top + latency_height / 2:.1f}" text-anchor="middle"
 transform="rotate(-90 25 {latency_top + latency_height / 2:.1f})">latency (ms, log scale)</text>
<text x="{left}" y="{height - 35}" class="subtitle">glyph: square mean; whisker mean ±1 sample SD; circle p50; diamond p95; triangle p99</text>
</svg>'''


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--title", default="BORSUK table-format replay")
    args = parser.parse_args()
    data = load_summary(args.input)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(render(data, title=args.title))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
