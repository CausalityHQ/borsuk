#!/bin/bash
set -euo pipefail
support=/opt/borsuk-reconstruction-gate
(cd "$support" && sha256sum -c support.sha256)
timeout -k 5 750 /bin/bash "$support/run-reconstruction-stage.sh" admission
