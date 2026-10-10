# Dameng Kafka Connect image

The default image contains Kafka Connect 4.3.0, the current Dameng connector
plugin, and `DmJdbcDriver18`. It runs on Linux amd64 with Java 21. The included worker
configuration supplies single-broker defaults. Environment variables can override
those defaults when the container starts.

## Build and export

From the repository root, with JDK 21 or newer:

```sh
./mvnw -pl debezium-connector-dameng -am -Dquick -Passembly package
cd debezium-connector-dameng
docker build --build-arg KAFKA_BASE_IMAGE=apache/kafka:4.3.0 \
  -f docker/Dockerfile -t debezium-connect-dameng:3.6.4-snapshot .
bash docker/test-env-config.sh debezium-connect-dameng:3.6.4-snapshot
docker save debezium-connect-dameng:3.6.4-snapshot | gzip -1 \
  > target/debezium-connect-dameng-3.6.4-SNAPSHOT-linux-amd64.tar.gz
cd target
sha256sum debezium-connect-dameng-3.6.4-SNAPSHOT-linux-amd64.tar.gz \
  > debezium-connect-dameng-3.6.4-SNAPSHOT-linux-amd64.tar.gz.sha256
```

The Maven command builds the plugin from the current source tree and skips
tests. Run the required tests separately before using a new build for anything
beyond evaluation.

## Import and start in the isolated environment

Transfer the `.tar.gz` and `.sha256` files together, then run:

```sh
sha256sum -c debezium-connect-dameng-3.6.4-SNAPSHOT-linux-amd64.tar.gz.sha256
docker load -i debezium-connect-dameng-3.6.4-SNAPSHOT-linux-amd64.tar.gz
```

Set the Kafka Connect worker properties through environment variables. The five
short names below follow the official Debezium Connect image convention:

```sh
docker run -d --name dameng-connect -p 8083:8083 \
  -e BOOTSTRAP_SERVERS=kafka:9092 \
  -e GROUP_ID=dameng-connect \
  -e CONFIG_STORAGE_TOPIC=dameng-connect-configs \
  -e OFFSET_STORAGE_TOPIC=dameng-connect-offsets \
  -e STATUS_STORAGE_TOPIC=dameng-connect-status \
  -e CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR=1 \
  -e CONNECT_OFFSET_STORAGE_REPLICATION_FACTOR=1 \
  -e CONNECT_STATUS_STORAGE_REPLICATION_FACTOR=1 \
  debezium-connect-dameng:3.6.4-snapshot
curl -fsS http://localhost:8083/connector-plugins
```

Any `CONNECT_` variable becomes a worker property by removing the prefix,
lowercasing the name, and replacing every underscore with a dot. For example,
`CONNECT_OFFSET_FLUSH_INTERVAL_MS=1000` sets `offset.flush.interval.ms=1000`;
`CONNECT_SECURITY_PROTOCOL` configures Kafka security. Explicit short names
such as `BOOTSTRAP_SERVERS` take precedence over their `CONNECT_` equivalents.
The image also accepts `KEY_CONVERTER`, `VALUE_CONVERTER`, `REST_PORT`,
`REST_HOST_NAME`, `ADVERTISED_PORT`, `ADVERTISED_HOST_NAME`,
`OFFSET_FLUSH_INTERVAL_MS`, `OFFSET_FLUSH_TIMEOUT_MS`, `SHUTDOWN_TIMEOUT`,
`HEAP_OPTS`, and `KAFKA_CONNECT_PLUGINS_DIR`.

You can still mount a read-only `worker.properties` at
`/etc/kafka/connect-distributed.properties`. It supplies the base settings;
environment variables override matching properties. Keep
`plugin.path=/opt/kafka/plugins` so the Dameng plugin remains discoverable.
The generated worker file is readable only by the container user, and its
values are not printed during startup.

The response should include `io.debezium.connector.dameng.DamengConnector`.
Create the connector through the Connect REST API using the configuration
example in [dameng.adoc](../../documentation/modules/ROOT/pages/connectors/dameng.adoc).
The container must be able to reach both Kafka and the active Dameng primary.
Kafka broker 2.0 is outside Kafka Connect 4.3's supported broker range.

## Kafka 2.0 broker test image

From the connector module directory, build the same plugin with a Kafka Connect 3.9.1 worker when the broker is Kafka
2.0. Kafka 4.x clients removed protocol versions needed by Kafka 2.0. The
connector JAR is still built from this repository and uses the Kafka API
provided by the 3.9.1 worker:

```sh
docker build --build-arg KAFKA_BASE_IMAGE=apache/kafka:3.9.1 \
  -f docker/Dockerfile -t debezium-connect-dameng:kafka2.0 .
bash docker/test-env-config.sh debezium-connect-dameng:kafka2.0
docker save debezium-connect-dameng:kafka2.0 | gzip -1 \
  > target/debezium-connect-dameng-kafka2.0-linux-amd64.tar.gz
cd target
sha256sum debezium-connect-dameng-kafka2.0-linux-amd64.tar.gz \
  > debezium-connect-dameng-kafka2.0-linux-amd64.tar.gz.sha256
```

On the isolated host, verify and import the archive:

```sh
sha256sum -c debezium-connect-dameng-kafka2.0-linux-amd64.tar.gz.sha256
docker load -i debezium-connect-dameng-kafka2.0-linux-amd64.tar.gz
```

Before starting a new connector, create its **schema history** topic on the
Kafka 2.0 cluster. Use the topic named by
`schema.history.internal.kafka.topic`. The topic must have one partition,
`cleanup.policy=delete`, and unlimited retention. Set the replication factor
for the target cluster; `1` is for the single-broker test only:

```sh
kafka-topics --zookeeper ZOOKEEPER_HOST:2181 --create \
  --topic dameng-schema-history --partitions 1 --replication-factor 1 \
  --config cleanup.policy=delete --config retention.ms=-1 \
  --config retention.bytes=-1
```

The current Debezium schema history code first requests broker-default topic
settings. Kafka 2.0 cannot process that CreateTopics request, so allowing the
connector to create this topic fails with `UnsupportedVersionException`.
Precreating the topic also avoids giving the connector topic-creation rights.
Do not use compaction for schema history; its records have null keys.

The supplied worker settings use replication factor `1`. Set suitable
replication factors and Kafka security settings for the target cluster, and
keep `exactly.once.source.support=disabled`. If the broker disables automatic
topic creation, precreate the connector's data topics as well. A Kafka 2.0
broker must be tested with the target authentication, topology, load, and
restart procedures before this image is used beyond evaluation.

Start the Kafka 2.0 test image with environment variables after creating the
required schema history topic. Use the addresses and replication factors for
the target cluster:

```sh
docker run -d --name dameng-connect -p 8083:8083 \
  -e BOOTSTRAP_SERVERS=kafka:9092 \
  -e GROUP_ID=dameng-connect \
  -e CONFIG_STORAGE_TOPIC=dameng-connect-configs \
  -e OFFSET_STORAGE_TOPIC=dameng-connect-offsets \
  -e STATUS_STORAGE_TOPIC=dameng-connect-status \
  -e CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR=1 \
  -e CONNECT_OFFSET_STORAGE_REPLICATION_FACTOR=1 \
  -e CONNECT_STATUS_STORAGE_REPLICATION_FACTOR=1 \
  -e CONNECT_EXACTLY_ONCE_SOURCE_SUPPORT=disabled \
  debezium-connect-dameng:kafka2.0
curl -fsS http://localhost:8083/connector-plugins
```

A mounted read-only `worker.properties` remains supported as described above.
