ARG KAFKA_BASE_IMAGE=apache/kafka:4.3.0
FROM ${KAFKA_BASE_IMAGE}

ARG CONNECTOR_VERSION=3.6.4-SNAPSHOT

USER root
COPY target/debezium-connector-dameng-${CONNECTOR_VERSION}-plugin.tar.gz /tmp/dameng-plugin.tar.gz
COPY docker/connect-distributed.properties /etc/kafka/connect-distributed.properties
COPY docker/configure-connect.sh docker/run-connect.sh /opt/kafka/bin/
RUN mkdir -p /opt/kafka/plugins \
    && tar -xzf /tmp/dameng-plugin.tar.gz -C /opt/kafka/plugins \
    && chmod 0755 /opt/kafka/bin/configure-connect.sh /opt/kafka/bin/run-connect.sh \
    && chown -R appuser:appuser /opt/kafka/plugins /etc/kafka/connect-distributed.properties

USER appuser
EXPOSE 8083
CMD ["/opt/kafka/bin/run-connect.sh"]
