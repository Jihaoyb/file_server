#!/usr/bin/env bash
# Simple wrapper around CMake presets for local builds.
set -euo pipefail

preset=${1:-debug}
cmake --preset "$preset"
cmake --build --preset "$preset"
