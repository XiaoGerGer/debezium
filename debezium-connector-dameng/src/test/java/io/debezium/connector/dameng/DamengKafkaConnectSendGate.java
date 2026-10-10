/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * A worker-runtime-only failpoint, packaged separately from the connector.
 * Earlier sends complete on Kafka's I/O thread while the source task is blocked
 * before its next send. Only the selected data topic is gated; history and offset
 * writes continue. The test kills this worker and starts an ungated replacement.
 */
public final class DamengKafkaConnectSendGate implements ProducerInterceptor<byte[], byte[]> {
    private String topic;
    private int allowedRecords;
    private int sentRecords;

    @Override
    public void configure(Map<String, ?> configs) {
        topic = configs.get("dameng.test.gate.topic").toString();
        allowedRecords = Integer.parseInt(configs.get("dameng.test.gate.allowed.records").toString());
    }

    @Override
    public ProducerRecord<byte[], byte[]> onSend(ProducerRecord<byte[], byte[]> record) {
        if (record.topic().equals(topic) && ++sentRecords > allowedRecords) {
            try {
                Files.writeString(Path.of("/tmp/dameng-send-gate-blocked"), topic);
                while (!Thread.currentThread().isInterrupted()) {
                    Thread.sleep(250);
                }
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            catch (IOException e) {
                throw new IllegalStateException("Could not mark the source producer send gate", e);
            }
        }
        return record;
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
    }

    @Override
    public void close() {
    }
}
