#!/usr/bin/env bash

set -Eeuo pipefail

# Log everything to journald with a stable identifier
#exec 1> >(systemd-cat --identifier=odin-tar-gen --priority=info)
exec > >(tee >(systemd-cat --identifier=odin-tar-gen --priority=info) >&1)
# Optional: ensure predictable locale/time (helps JSON timestamps)
export LC_ALL=C.UTF-8
export TZ=America/New_York

# One-at-a-time lock (avoid overlapping runs)
LOCK="/home/adam/Locks/restic-dropbox-job.lock"
mkdir -p "$(dirname "$LOCK")"
exec 9>"$LOCK"
flock -n 9 || { echo "Another run is in progress, exiting."; exit 0; }

source /home/adam/Projects/odin-backup-system/.venv/bin/activate

# Run from the correct directory
cd /home/adam/Projects/odin-backup-system

# Nice/ionice so it stays polite under load (optional)
nice -n 10 ionice -c2 -n7 \
  python -m scripts.restic_dropbox_job