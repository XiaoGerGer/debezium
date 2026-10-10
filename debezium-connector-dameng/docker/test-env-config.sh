#!/usr/bin/env bash
set -euo pipefail

image=${1:-debezium-connect-dameng:kafka2.0}
base_config=$(mktemp)
trap 'rm -f "$base_config"' EXIT
printf 'bootstrap.servers=file-broker:9092\ngroup.id=file-group\nplugin.path=/opt/kafka/plugins\n' > "$base_config"

docker run --rm --entrypoint /bin/bash \
    -v "$base_config:/etc/kafka/connect-distributed.properties:ro" \
    -e BOOTSTRAP_SERVERS=bare-broker:9092 \
    -e CONNECT_BOOTSTRAP_SERVERS=prefixed-broker:9092 \
    -e GROUP_ID=bare-group \
    -e CONFIG_STORAGE_TOPIC=env-configs \
    -e OFFSET_STORAGE_TOPIC=env-offsets \
    -e STATUS_STORAGE_TOPIC=env-status \
    -e CONNECT_OFFSET_FLUSH_INTERVAL_MS=1234 \
    -e CONNECT_SECURITY_PROTOCOL=SASL_PLAINTEXT \
    -e CONNECT_CLIENT_ID=worker@dameng \
    "$image" -euc '
        config=$(mktemp)
        /opt/kafka/bin/configure-connect.sh /etc/kafka/connect-distributed.properties "$config"
        property() {
            awk -v key="$1" "index(\$0, key \"=\") == 1 { value = substr(\$0, length(key) + 2) } END { print value }" "$config"
        }
        [[ $(property bootstrap.servers) == bare-broker:9092 ]]
        [[ $(property group.id) == bare-group ]]
        [[ $(property config.storage.topic) == env-configs ]]
        [[ $(property offset.storage.topic) == env-offsets ]]
        [[ $(property status.storage.topic) == env-status ]]
        [[ $(property offset.flush.interval.ms) == 1234 ]]
        [[ $(property security.protocol) == SASL_PLAINTEXT ]]
        [[ $(property client.id) == worker@dameng ]]
        [[ $(property plugin.path) == /opt/kafka/plugins ]]
        [[ $(stat -c %a "$config") == 600 ]]
    '

docker run --rm --entrypoint /bin/bash \
    -e CONNECT_BOOTSTRAP_SERVERS=prefixed-only:9092 \
    -e CONNECT_GROUP_ID=prefixed-group \
    -e CONNECT_CONFIG_STORAGE_TOPIC=prefixed-configs \
    "$image" -euc '
        config=$(mktemp)
        /opt/kafka/bin/configure-connect.sh /etc/kafka/connect-distributed.properties "$config"
        property() {
            awk -v key="$1" "index(\$0, key \"=\") == 1 { value = substr(\$0, length(key) + 2) } END { print value }" "$config"
        }
        [[ $(property bootstrap.servers) == prefixed-only:9092 ]]
        [[ $(property group.id) == prefixed-group ]]
        [[ $(property config.storage.topic) == prefixed-configs ]]
        [[ $(property plugin.path) == /opt/kafka/plugins ]]
    '

printf 'Environment configuration checks passed for %s\n' "$image"
