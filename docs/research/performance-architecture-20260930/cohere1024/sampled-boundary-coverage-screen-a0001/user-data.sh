#!/bin/bash
set -eu
systemd-run --unit=borsuk-boundary-data-shutdown --on-active=1800s /usr/sbin/shutdown -h now
