#!/usr/bin/env bash
set -euo pipefail

# Use from an operator-scheduled service only after the backup mount is verified encrypted.
mode="${1:?usage: managed-backups.sh backup|prune}"
backup_dir="${BOAZ_HEALTH_BACKUP_DIR:?BOAZ_HEALTH_BACKUP_DIR is required}"
receiver="${BOAZ_HEALTH_RECEIVER_BINARY:-/opt/boaz-health/bin/boaz-health-receiver}"

if [[ "${BOAZ_HEALTH_BACKUP_VOLUME_ENCRYPTED:-0}" != "1" ]]; then
  echo "Encrypted backup volume gate is closed" >&2
  exit 1
fi

case "$mode" in
  backup)
    "$receiver" backup "$backup_dir/boaz-health-$(date -u +%Y%m%dT%H%M%SZ).db"
    "$receiver" prune-backups "$backup_dir"
    ;;
  prune)
    "$receiver" prune-backups "$backup_dir"
    ;;
  *)
    echo "usage: managed-backups.sh backup|prune" >&2
    exit 1
    ;;
esac
