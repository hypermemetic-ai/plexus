#!/bin/sh
# Run in the foreground from an operator-specific tailnet benchmark bundle.
set -eu
umask 077
cd "$(dirname "$0")"
case "$(uname -sm)" in
  "Darwin arm64") agent=./arm64/load-agent ;;
  "Darwin x86_64") agent=./x86_64/load-agent ;;
  *) echo "This bundle is for macOS." >&2; exit 1 ;;
esac
test -x "$agent" || { echo "Missing binary for $(uname -m)" >&2; exit 1; }
read -r controller < controller.pin
read -r target < ryzen.ip
read -r mac < mac.ip
# Raise only this process tree's soft descriptor limit, within local limits.
hard=$(ulimit -Hn)
kernel=$(sysctl -n kern.maxfilesperproc)
case "$hard" in unlimited) limit=$kernel ;; *) limit=$(( hard < kernel ? hard : kernel )) ;; esac
ulimit -Sn "$limit" 2>/dev/null || :
soft=$(ulimit -Sn)
case "$soft" in unlimited) soft=$kernel ;; esac
max_users=$((soft - 128))
if [ "$max_users" -gt 1000000 ]; then max_users=1000000; fi
if [ -n "${MAX_USERS:-}" ] && [ "$MAX_USERS" -lt "$max_users" ]; then max_users=$MAX_USERS; fi
test "$max_users" -gt 0 || { echo "Insufficient open file allowance ($soft)" >&2; exit 1; }
state="$HOME/Library/Application Support/Plexus/watch-capacity"
mkdir -p "$state"
key="$state/mac.key"
if [ ! -f "$key" ]; then "$agent" keygen "$key" >/dev/null; fi
pin=$("$agent" principal "$key")
# The callback sends only the public TLS pin over the tailnet.
if ! curl --noproxy '*' -fsS --max-time 10 -X POST --data-binary "$pin" "http://$target:47673/paired"; then
  echo "Automatic pairing unavailable. Share this public worker pin with the Ryzen operator: $pin" >&2
fi
echo "Ryzen fixture $target; maximum $max_users sessions; press Control-C to stop."
if [ -f dial-back ]; then
    exec "$agent" dial-back "$key" "$target:47674" "$controller" "$target" "$max_users"
fi
exec "$agent" serve "$key" "$mac:47671" "$controller" "$target" "$max_users"
