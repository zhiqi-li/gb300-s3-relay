#!/usr/bin/env bash
set -euo pipefail

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)
repo_root=$(CDPATH= cd -- "$script_dir/.." && pwd -P)
tools_dir=${GB300_RELAY_TOOLS_DIR:-"$repo_root/.tools"}
version=2.3.0

case "$(uname -m)" in
  x86_64|amd64)
    asset="s5cmd_${version}_Linux-64bit.tar.gz"
    expected="de0fdbfa3aceae55e069ba81a0fc17b2026567637603734a387b2fca06c299b4"
    ;;
  aarch64|arm64)
    asset="s5cmd_${version}_Linux-arm64.tar.gz"
    expected="1439f0d00ecedcd2a2f1f2c6749bbb0152b2257bf5086f29646ec8ae38798e24"
    ;;
  *)
    echo "unsupported architecture: $(uname -m)" >&2
    exit 2
    ;;
esac

temporary=$(mktemp -d -t gb300-relay-s5cmd.XXXXXXXX)
trap 'rm -rf -- "$temporary"' EXIT
archive="$temporary/$asset"
curl --fail --location --retry 5 --output "$archive" \
  "https://github.com/peak/s5cmd/releases/download/v${version}/${asset}"
printf '%s  %s\n' "$expected" "$archive" | sha256sum --check --status
tar -xzf "$archive" -C "$temporary" s5cmd
mkdir -p -- "$tools_dir"
install -m 0755 "$temporary/s5cmd" "$tools_dir/s5cmd"
"$tools_dir/s5cmd" version
