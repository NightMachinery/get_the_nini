#!/usr/bin/env bash
set -euo pipefail

# Generate example outputs (org, md, json) for a Ninisite discussion.
# Assumes `get-the-nini` is installed and available in PATH.

if ! command -v get-the-nini >/dev/null 2>&1; then
  echo "Error: get-the-nini CLI not found in PATH. Install the package first." >&2
  exit 1
fi

# Allow overriding URL via first argument; default to the requested example.
URL="${1:-https://www.ninisite.com/discussion/topic/11473285/}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "Generating examples in: $PWD"
echo "URL: $URL"

echo "-> Generating org..."
get-the-nini "$URL" --format org

echo "-> Generating markdown..."
get-the-nini "$URL" --format md

echo "-> Generating json..."
get-the-nini "$URL" --format json

echo "Done. Files created:"
