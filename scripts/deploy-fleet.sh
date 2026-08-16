#!/usr/bin/env bash
set -euo pipefail

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)
repo_root=$(CDPATH= cd -- "$script_dir/.." && pwd -P)
venv_dir="$repo_root/.venv"
tools_dir="$repo_root/.tools"

if [[ ! -x "$venv_dir/bin/python" ]]; then
  python3 -m venv --without-pip "$venv_dir"
fi

if "$venv_dir/bin/python" -m pip --version >/dev/null 2>&1; then
  "$venv_dir/bin/python" -m pip install --disable-pip-version-check --quiet \
    -e "${repo_root}[fleet]"
else
  mkdir -p "$tools_dir"
  if [[ ! -f "$tools_dir/pip.pyz" ]]; then
    curl --fail --location --retry 5 --silent --show-error \
      --output "$tools_dir/pip.pyz" https://bootstrap.pypa.io/pip/pip.pyz
  fi
  "$venv_dir/bin/python" "$tools_dir/pip.pyz" install \
    --disable-pip-version-check --quiet -e "${repo_root}[fleet]"
fi

exec "$venv_dir/bin/python" "$script_dir/deploy-fleet.py" "$@"
