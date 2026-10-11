#!/bin/bash
set -euo pipefail
systemd-run --unit=borsuk-summary-shutdown --on-active=600s /usr/sbin/shutdown -h now
