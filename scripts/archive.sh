#!/usr/bin/env bash

set -euo pipefail

project_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
archive_name="the-perfect-wp-cron"
archive_root="$(mktemp -d "${TMPDIR:-/tmp}/${archive_name}.XXXXXX")"

cleanup() {
	rm -rf "$archive_root"
	return 0
}

trap cleanup EXIT

composer install --no-dev --working-dir="$project_dir"
composer install --no-dev --working-dir="$project_dir/bin"
pnpm --dir "$project_dir" run build

rm -f "$project_dir/$archive_name.zip"
mkdir "$archive_root/$archive_name"
rsync -a --exclude-from="$project_dir/.distignore" "$project_dir/" "$archive_root/$archive_name/"

(
	cd "$archive_root"
	zip -r "$project_dir/$archive_name.zip" "$archive_name"
)
