#!/usr/bin/env bash
# Writes THIRD_PARTY_NOTICES.md: the license (and NOTICE, if any) text of every
# third-party module compiled into the CLI. Run by GoReleaser so release archives ship it.
set -euo pipefail

out="${1:-THIRD_PARTY_NOTICES.md}"

{
  echo "# Third-party notices"
  echo
  echo "streamio is MIT-licensed (see LICENSE). It includes the following third-party modules, each under its own license, reproduced below."
  echo
} > "$out"

go list -deps -f '{{if not .Standard}}{{with .Module}}{{.Path}} {{.Version}} {{.Dir}}{{end}}{{end}}' ./cmd/streamio |
  sort -u | grep -v '^github.com/AlexAslan/streamio ' |
  while read -r path version dir; do
    lic=$(find "$dir" -maxdepth 1 -iname 'licen[sc]e*' -o -maxdepth 1 -iname 'copying*' | head -1)
    {
      echo "## $path $version"
      echo
      echo '```'
      cat "$lic"
      echo '```'
      echo
      notice=$(find "$dir" -maxdepth 1 -iname 'notice*' | head -1)
      if [ -n "$notice" ]; then
        echo "### NOTICE"
        echo
        echo '```'
        cat "$notice"
        echo '```'
        echo
      fi
    } >> "$out"
  done
