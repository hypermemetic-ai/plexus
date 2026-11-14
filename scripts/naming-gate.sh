#!/usr/bin/env bash
# A Plexus backend is a service (owner, 2026-10-29). Fails when any tracked
# file names one by the old word. The word is assembled here so this file
# passes its own gate; a foreign name we do not own (GitHub) is allowed.
set -euo pipefail
cd "$(dirname "$0")/.."
old="h""ub"
if git grep -n -i -P "(?<!git)${old}(?!\\.com)" -- . ; then
  echo "naming-gate: the lines above say '${old}'; a Plexus backend is a service" >&2
  exit 1
fi
echo "naming-gate: ok"
