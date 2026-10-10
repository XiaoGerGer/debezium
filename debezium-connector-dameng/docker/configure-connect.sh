#!/usr/bin/env bash
set -euo pipefail

if (( $# != 2 )); then
    printf 'Usage: %s BASE_PROPERTIES OUTPUT_PROPERTIES\n' "$0" >&2
    exit 2
fi

umask 077
cp -- "$1" "$2"
chmod 0600 "$2"

declare -A properties=()

# Match the Debezium image convention: CONNECT_FOO_BAR configures foo.bar.
# NUL separators preserve values containing spaces, equals signs or newlines.
while IFS= read -r -d '' entry; do
    name=${entry%%=*}
    [[ $name == CONNECT_* ]] || continue
    suffix=${name#CONNECT_}
    [[ -n $suffix ]] || continue
    property=${suffix,,}
    property=${property//_/.}
    if [[ ! $property =~ ^[a-z0-9][a-z0-9._-]*$ ]]; then
        printf 'Invalid Kafka Connect environment variable name: %s\n' "$name" >&2
        exit 2
    fi
    properties["$property"]=${entry#*=}
done < <(env -0)

# Explicit short names take precedence over their CONNECT_ equivalents.
set_short_name() {
    local name=$1
    local property=$2
    if [[ -v $name ]]; then
        properties["$property"]=${!name}
    fi
}

set_short_name BOOTSTRAP_SERVERS bootstrap.servers
set_short_name GROUP_ID group.id
set_short_name CONFIG_STORAGE_TOPIC config.storage.topic
set_short_name OFFSET_STORAGE_TOPIC offset.storage.topic
if [[ -n ${STATUS_STORAGE_TOPIC:-} ]]; then
    properties[status.storage.topic]=$STATUS_STORAGE_TOPIC
fi
set_short_name KEY_CONVERTER key.converter
set_short_name VALUE_CONVERTER value.converter
set_short_name OFFSET_FLUSH_INTERVAL_MS offset.flush.interval.ms
set_short_name OFFSET_FLUSH_TIMEOUT_MS offset.flush.timeout.ms
set_short_name SHUTDOWN_TIMEOUT task.shutdown.graceful.timeout.ms
set_short_name REST_PORT rest.port
set_short_name REST_HOST_NAME rest.host.name
set_short_name ADVERTISED_PORT rest.advertised.port
set_short_name ADVERTISED_HOST_NAME rest.advertised.host.name
set_short_name KAFKA_CONNECT_PLUGINS_DIR plugin.path

# The bundled file sets listeners explicitly, which otherwise masks rest.port.
if [[ ${properties[rest.port]+x} || ${properties[rest.host.name]+x} ]]; then
    if [[ ! ${properties[listeners]+x} ]]; then
        properties[listeners]="http://${properties[rest.host.name]:-0.0.0.0}:${properties[rest.port]:-8083}"
    fi
fi

escape_value() {
    local value=$1
    value=${value//\\/\\\\}
    value=${value//$'\r'/\\r}
    value=${value//$'\n'/\\n}
    value=${value//$'\t'/\\t}
    value=${value// /\\ }
    printf '%s' "$value"
}

# Appended properties override the mounted or bundled base configuration.
# Do not log values because CONNECT_* can contain passwords and JAAS strings.
printf '\n' >> "$2"
if (( ${#properties[@]} > 0 )); then
    while IFS= read -r property; do
        printf '%s=%s\n' "$property" "$(escape_value "${properties[$property]}")" >> "$2"
    done < <(printf '%s\n' "${!properties[@]}" | LC_ALL=C sort)
fi
