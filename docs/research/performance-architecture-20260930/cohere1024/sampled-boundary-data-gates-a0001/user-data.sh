#!/bin/bash
set -eu
systemd-run --unit=borsuk-boundary-data-shutdown --on-active=3000s /usr/sbin/shutdown -h now
