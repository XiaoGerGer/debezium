#!/usr/bin/env bash
set -euo pipefail

monitoring_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
agent_dir="$monitoring_dir/agent"
agent_name=jmx_prometheus_javaagent-1.4.0.jar
agent_sha256=db1492e95a7ee95cd5e0a969875c0d4f0ef6413148d750351a41cc71d775f59a
mkdir -p -- "$agent_dir"
download=$(mktemp "$agent_dir/.download.XXXXXX")
trap 'rm -f -- "$download"' EXIT
if ! curl --fail --location --retry 1 --connect-timeout 10 --max-time 60 \
  "https://github.com/prometheus/jmx_exporter/releases/download/1.4.0/$agent_name" \
  --output "$download"; then
  # The official release API serves the same asset when the download host is unavailable.
  curl --fail --location --retry 2 --connect-timeout 10 --max-time 90 \
    --header 'Accept: application/octet-stream' \
    https://api.github.com/repos/prometheus/jmx_exporter/releases/assets/286185605 \
    --output "$download"
fi
printf '%s  %s\n' "$agent_sha256" "$download" | sha256sum --check --status
chmod 0644 "$download"
mv -- "$download" "$agent_dir/$agent_name"
printf 'Verified JMX Exporter 1.4.0: %s\n' "$agent_dir/$agent_name"
