ARG KAFKA_BASE_IMAGE=apache/kafka:3.9.1
FROM ${KAFKA_BASE_IMAGE}

ARG CONNECTOR_VERSION=3.6.4-xingchuang-kafka2.0
LABEL org.opencontainers.image.title="Debezium Connect with Dameng connector" \
    org.opencontainers.image.version="${CONNECTOR_VERSION}"

USER root
COPY target/debezium-connector-dameng-${CONNECTOR_VERSION}-plugin.tar.gz /tmp/dameng-plugin.tar.gz
COPY docker/connect-distributed.properties /etc/kafka/connect-distributed.properties
COPY docker/configure-connect.sh docker/run-connect.sh /opt/kafka/bin/
COPY docker/monitoring/agent/jmx_prometheus_javaagent-1.4.0.jar /opt/jmx/jmx_prometheus_javaagent-1.4.0.jar
COPY docker/monitoring/jmx-exporter.yaml /opt/jmx/jmx-exporter.yaml
RUN printf '%s  %s\n' db1492e95a7ee95cd5e0a969875c0d4f0ef6413148d750351a41cc71d775f59a /opt/jmx/jmx_prometheus_javaagent-1.4.0.jar | sha256sum -c - \
    && chmod 0644 /opt/jmx/jmx_prometheus_javaagent-1.4.0.jar /opt/jmx/jmx-exporter.yaml \
    && mkdir -p /opt/kafka/plugins \
    && tar -xzf /tmp/dameng-plugin.tar.gz -C /opt/kafka/plugins \
    && chmod 0755 /opt/kafka/bin/configure-connect.sh /opt/kafka/bin/run-connect.sh \
    && chown -R appuser:appuser /opt/kafka/plugins /etc/kafka/connect-distributed.properties

USER appuser
ENV KAFKA_HEAP_OPTS="-Xms4G -Xmx4G"
ENV KAFKA_OPTS="-javaagent:/opt/jmx/jmx_prometheus_javaagent-1.4.0.jar=9404:/opt/jmx/jmx-exporter.yaml"
EXPOSE 8083 9404
CMD ["/opt/kafka/bin/run-connect.sh"]
