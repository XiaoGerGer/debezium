#!/usr/bin/env bash
# Copyright Debezium Authors.
# Licensed under the Apache Software License, Version 2.0.
set -euo pipefail

docker_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
connector_dir=$(cd -- "$docker_dir/.." && pwd)
repo_dir=$(cd -- "$connector_dir/.." && pwd)

usage() {
    cat <<'EOF'
Usage: build-image.sh [options] [-- Maven options]

Build the Dameng plugin and Docker image, check its environment configuration,
and export a compressed image archive with a SHA-256 checksum.

Options:
  --image IMAGE       Image name and tag (default: debezium-connect-dameng:<version>)
  --base-image IMAGE  Kafka base image (default: apache/kafka:3.9.1)
  --platform VALUE    Target platform (default: linux/amd64; checks must run on it)
  --output FILE       Export path (default: <module>/target/debezium-connect-dameng-<version>-<platform>.tar.gz)
  --skip-build        Reuse the existing plugin archive; Maven still resolves its version
  --no-export         Build and check the image without exporting it
  -h, --help          Show this help

Maven uses the repository wrapper and skips integration tests by default.
For a quick package without unit tests or build checks, append: -- -Dquick
EOF
}

fail() {
    printf 'Error: %s\n' "$*" >&2
    exit 1
}

image=
base_image=apache/kafka:3.9.1
platform=linux/amd64
output=
skip_build=false
export_image=true
maven_options=()

while (($#)); do
    case "$1" in
        --image|--base-image|--platform|--output)
            [[ $# -ge 2 && -n "$2" && "$2" != -* ]] || fail "$1 requires a value"
            case "$1" in
                --image) image=$2 ;;
                --base-image) base_image=$2 ;;
                --platform) platform=$2 ;;
                --output) output=$2 ;;
            esac
            shift 2
            ;;
        --skip-build) skip_build=true; shift ;;
        --no-export) export_image=false; shift ;;
        -h|--help) usage; exit 0 ;;
        --) shift; maven_options=("$@"); break ;;
        *) fail "Unknown option: $1 (see --help)" ;;
    esac
done

[[ "$platform" =~ ^linux/[a-z0-9_]+(/[a-z0-9_]+)?$ ]] || fail "Specify one Linux platform, such as linux/amd64"
[[ -z "$output" || "$export_image" == true ]] || fail "--output cannot be combined with --no-export"
for command in docker java sha256sum gzip mktemp; do
    command -v "$command" >/dev/null 2>&1 || fail "Required command not found: $command"
done
docker info >/dev/null || fail "Cannot connect to the Docker daemon"

version_file=$(mktemp)
archive_temp=
checksum_temp=
cleanup() {
    rm -f -- "$version_file"
    [[ -z "$archive_temp" ]] || rm -f -- "$archive_temp"
    [[ -z "$checksum_temp" ]] || rm -f -- "$checksum_temp"
}
trap cleanup EXIT

printf 'Resolving Dameng project version with Maven\n'
(
    cd -- "$repo_dir"
    ./mvnw -B -ntp -q -N -f "$connector_dir/pom.xml" "${maven_options[@]}" \
        help:evaluate -Dexpression=project.version -Doutput="$version_file"
)
version=$(<"$version_file")
[[ "$version" =~ ^[a-zA-Z0-9][a-zA-Z0-9._-]*$ ]] || fail "Maven did not return a usable project.version"
image=${image:-debezium-connect-dameng:$version}
plugin_archive="$connector_dir/target/debezium-connector-dameng-$version-plugin.tar.gz"
if [[ "$export_image" == true ]]; then
    output=${output:-$connector_dir/target/debezium-connect-dameng-$version-${platform//\//-}.tar.gz}
    [[ ! -d "$output" && ! -d "$output.sha256" ]] || fail "Output archive and checksum paths must be files: $output"
fi

if [[ "$skip_build" == false ]]; then
    printf 'Building Dameng plugin %s\n' "$version"
    (
        cd -- "$repo_dir"
        ./mvnw -B -ntp -pl debezium-connector-dameng,debezium-assembly-descriptors -am \
            process-sources install -Passembly -DskipITs "${maven_options[@]}"
    )
fi
[[ -s "$plugin_archive" ]] || fail "Plugin archive not found: $plugin_archive; run without --skip-build"

agent="$docker_dir/monitoring/agent/jmx_prometheus_javaagent-1.4.0.jar"
agent_sha256=db1492e95a7ee95cd5e0a969875c0d4f0ef6413148d750351a41cc71d775f59a
if [[ ! -f "$agent" ]] || ! printf '%s  %s\n' "$agent_sha256" "$agent" | sha256sum --check --status; then
    command -v curl >/dev/null 2>&1 || fail "curl is required to download the JMX Agent"
    bash "$docker_dir/monitoring/fetch-agent.sh"
fi

printf 'Building image %s for %s\n' "$image" "$platform"
docker build --platform "$platform" --build-arg "KAFKA_BASE_IMAGE=$base_image" \
    --build-arg "CONNECTOR_VERSION=$version" -f "$docker_dir/Dockerfile" \
    -t "$image" "$connector_dir"
bash "$docker_dir/test-env-config.sh" "$image"
printf 'Image ready: %s\n' "$image"

if [[ "$export_image" == true ]]; then
    mkdir -p -- "$(dirname -- "$output")"
    output_dir=$(cd -- "$(dirname -- "$output")" && pwd)
    output_name=$(basename -- "$output")
    archive_temp=$(mktemp "$output_dir/.dameng-image.XXXXXX")
    checksum_temp=$(mktemp "$output_dir/.dameng-checksum.XXXXXX")
    printf 'Exporting image to %s/%s\n' "$output_dir" "$output_name"
    docker save "$image" | gzip -1 > "$archive_temp"
    checksum=$(sha256sum "$archive_temp")
    printf '%s  %s\n' "${checksum%% *}" "$output_name" > "$checksum_temp"
    chmod 0644 "$archive_temp" "$checksum_temp"
    mv -- "$archive_temp" "$output_dir/$output_name"
    mv -- "$checksum_temp" "$output_dir/$output_name.sha256"
    printf 'Archive ready: %s/%s\nChecksum: %s/%s.sha256\n' "$output_dir" "$output_name" "$output_dir" "$output_name"
fi
