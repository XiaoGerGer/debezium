#!/usr/bin/env bash
set -euo pipefail

if [[ -v HEAP_OPTS ]]; then
    export KAFKA_HEAP_OPTS="$HEAP_OPTS"
fi

worker_config=$(mktemp /tmp/dameng-connect.XXXXXX.properties)
/opt/kafka/bin/configure-connect.sh /etc/kafka/connect-distributed.properties "$worker_config"
exec /opt/kafka/bin/connect-distributed.sh "$worker_config"
