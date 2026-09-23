"""Create-only Spot worker for the frozen 1M paired two-bit score gate."""

from __future__ import annotations

import base64
import gzip
import hashlib
import textwrap

from scripts.native_one_million_page_oracle_worker import _download
from scripts.native_one_million_progressive_code_projection_worker import (
    _once,
    progressive_code_wave_worker_script,
)
from scripts.native_one_million_progressive_paired_cell import (
    PRIOR_PLAN_SEAL_SHA256,
    PRIOR_PLAN_SHA256,
    PRIOR_PREFIX,
    PRIOR_TERMINAL_SHA256,
)
from scripts.native_one_million_selector_cell import (
    DEVELOPMENT_IDENTITIES,
    SOURCE_IDENTITIES,
)


def expanded_worker_script(plan: object) -> str:
    """Preserve all broker checks while adding source build and paired scoring."""
    script = progressive_code_wave_worker_script(plan)
    script = _once(script, "/mnt/native-progressive-code-wave", "/mnt/native-progressive-paired")
    script = _once(
        script, "borsuk-one-million-progressive-code-wave-terminal-v1",
        "borsuk-one-million-progressive-paired-terminal-v1",
    )
    source = SOURCE_IDENTITIES["source"]
    downloads = "\n".join((
        _download(source.uri, source.sha256, source.bytes, "source.parquet"),
        _download(PRIOR_PREFIX + "terminal.json", PRIOR_TERMINAL_SHA256, 5339,
                  "prior-progressive-terminal.json"),
        _download(PRIOR_PREFIX + "artifacts/progressive-code-wave-plans.json",
                  PRIOR_PLAN_SHA256, 79321292, "prior-progressive-plans.json"),
        _download(PRIOR_PREFIX + "artifacts/progressive-code-wave-plan-seal.json",
                  PRIOR_PLAN_SEAL_SHA256, 337, "prior-progressive-plan-seal.json"),
    ))
    script = _once(script, "phase=construct\n", downloads + "\nphase=construct\n")
    script = _once(
        script, "scripts.native_one_million_page_oracle_cell construct",
        "scripts.native_one_million_progressive_paired_cell construct",
    )
    script = _once(
        script,
        "chmod 0444 generation.json base.arrow delta.arrow prior-*.json page-map.bin page-groups.bin page-bytes.bin seal.json",
        "chmod 0444 source.parquet generation.json base.arrow delta.arrow prior-*.json "
        "page-map.bin page-groups.bin page-bytes.bin seal.json mean.bin records.bin "
        "sign-base.bin sign-delta.bin magnitude-base.bin magnitude-delta.bin "
        "progressive-code-seal.json progressive-build.json",
    )
    script = _once(
        script,
        'for name in page-map.bin page-groups.bin page-bytes.bin seal.json construct-resources.txt; do publish_artifact "$name"; done',
        'for name in page-map.bin page-groups.bin page-bytes.bin seal.json mean.bin '
        'sign-base.bin sign-delta.bin magnitude-base.bin magnitude-delta.bin '
        'progressive-code-seal.json progressive-build.json construct-resources.txt; '
        'do publish_artifact "$name"; done',
    )
    query = DEVELOPMENT_IDENTITIES["queries"]
    query_download = _download(query.uri, query.sha256, query.bytes, "queries.parquet")
    script = _once(
        script, "phase=plan\n",
        query_download + "\nchmod 0444 queries.parquet\nphase=plan\n",
    )
    script = _once(script, "phase=evaluate\n" + query_download + "\n", "phase=evaluate\n")
    script = _once(
        script, "scripts.native_one_million_progressive_code_projection_cell plan",
        "scripts.native_one_million_progressive_paired_cell plan",
    )
    for old, new in (
        ("planning/progressive-code-wave-plans.json progressive-code-wave-plans.json",
         "planning/progressive-paired-plans.json progressive-paired-plans.json"),
        ("planning/progressive-code-wave-plan-seal.json progressive-code-wave-plan-seal.json",
         "planning/progressive-paired-plan-seal.json progressive-paired-plan-seal.json"),
        ("chmod 0444 progressive-code-wave-plans.json progressive-code-wave-plan-seal.json",
         "chmod 0444 progressive-paired-plans.json progressive-paired-plan-seal.json"),
        ('for name in progressive-code-wave-plans.json progressive-code-wave-plan-seal.json plan-resources.txt; do',
         'for name in progressive-paired-plans.json progressive-paired-plan-seal.json plan-resources.txt; do'),
        ("scripts.native_one_million_progressive_code_projection_cell evaluate",
         "scripts.native_one_million_progressive_paired_cell evaluate"),
        ("evaluation/progressive-code-wave-evidence.json progressive-code-wave-evidence.json",
         "evaluation/progressive-paired-evidence.json progressive-paired-evidence.json"),
        ("evaluation/progressive-code-wave-result.json progressive-code-wave-result.json",
         "evaluation/progressive-paired-result.json progressive-paired-result.json"),
        ('for name in progressive-code-wave-evidence.json progressive-code-wave-result.json evaluate-resources.txt; do',
         'for name in progressive-paired-evidence.json progressive-paired-result.json evaluate-resources.txt; do'),
        ("scripts.validate_native_one_million_progressive_code_projection",
         "scripts.validate_native_one_million_progressive_paired_cell"),
        ('for name in progressive-code-wave-validation.json validate-resources.txt; do',
         'for name in progressive-paired-validation.json validate-resources.txt; do'),
    ):
        script = _once(script, old, new)
    if (
        "phase=plan\n" not in script or "phase=evaluate\n" not in script
        or script.count(query_download) != 1
        or script.index("phase=construct\n") >= script.index(query_download)
        or script.index(query_download) >= script.index("phase=plan\n")
        or script.index("phase=plan\n") >= script.index("phase=evaluate\n")
        or script.index("phase=evaluate\n") >= script.index("truth.parquet")
    ):
        raise ValueError("progressive paired Spot phase barrier differs")
    return script


def progressive_paired_worker_script(plan: object) -> str:
    """Compress the checked shell worker below the EC2 user-data byte limit."""
    expanded = expanded_worker_script(plan)
    encoded = base64.b64encode(gzip.compress(expanded.encode(), mtime=0)).decode()
    digest = hashlib.sha256(expanded.encode()).hexdigest()
    wrapper = (
        "#!/bin/bash\nset -euo pipefail\n"
        "trap 'shutdown -h now' ERR\n"
        "base64 -d <<'BORSUK_WORKER_PAYLOAD' | gzip -d > /tmp/borsuk-progressive-worker.sh\n"
        + "\n".join(textwrap.wrap(encoded, 76))
        + "\nBORSUK_WORKER_PAYLOAD\n"
        + f"printf '%s  %s\\n' '{digest}' '/tmp/borsuk-progressive-worker.sh' | sha256sum -c -\n"
        + "exec /bin/bash /tmp/borsuk-progressive-worker.sh\n"
    )
    if len(wrapper.encode()) > 16_384:
        raise ValueError("progressive paired Spot worker exceeds EC2 user-data limit")
    return wrapper
