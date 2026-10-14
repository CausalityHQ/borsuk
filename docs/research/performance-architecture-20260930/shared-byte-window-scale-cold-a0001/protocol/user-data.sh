#!/usr/bin/env bash
set -euo pipefail
systemd-run --unit=borsuk-source-width64-shutdown --on-active=14400s /sbin/shutdown -h now
