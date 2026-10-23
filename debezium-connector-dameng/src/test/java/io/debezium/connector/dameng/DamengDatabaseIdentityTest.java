/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.connect.errors.ConnectException;
import org.junit.jupiter.api.Test;

import io.debezium.connector.dameng.DamengDatabaseIdentity.OpenRecord;

class DamengDatabaseIdentityTest {
    @Test
    void shouldPersistSourceLineageAndValidateLocalArchivesSeparatelyFromTheReplicaIdentity() {
        final var config = new DamengConnectorConfig(DamengConnectorConfigTest.configuration().with("database.standby.hosts", "replica:5236").build());
        final var identity = new DamengDatabaseIdentity(1, 10, 200, List.of(open("a", 1, 10, 100)), 20);
        final var offset = new DamengOffsetContext(config, 150);
        offset.validatePrimaryStandbyIdentity(identity);
        assertThat(offset.getOffset().get(DamengOffsetContext.DATABASE_MAGIC_KEY)).isEqualTo(10L);
        assertThat(identity.readingDatabaseMagic()).isEqualTo(20);
        assertThat(identity.containsArchiveSource(10, 100, 200)).isTrue();
        assertThat(identity.containsArchiveSource(20, 100, 200)).isFalse();
        final var otherReader = new DamengDatabaseIdentity(1, 10, 200, identity.history(), 30);
        offset.validatePrimaryStandbyIdentity(otherReader);
        assertThat(offset.getOffset().get(DamengOffsetContext.DATABASE_MAGIC_KEY)).isEqualTo(10L);
    }

    @Test
    void shouldRejectClockOrderThatWouldHideAnUnsynchronizedPromotion() {
        assertThatThrownBy(() -> identity(20, 500, open("a", 2, 10, 100), open("b-promotion", 1, 20, 200), open("b-restart", 3, 20, 300)))
                .isInstanceOf(ConnectException.class).hasMessageContaining("ambiguously ordered");
    }

    @Test
    void shouldResumeFromTheOldPrimaryOnlyWhenThePromotionRetainsItsAcknowledgedHistory() {
        final var identity = identity(20, 250, open("a", 1, 10, 100), open("b", 2, 20, 200));
        assertThatCode(() -> identity.validateResume(1, 10, "a", 200)).doesNotThrowAnyException();
        assertThatThrownBy(() -> identity.validateResume(1, 10, "a", 201)).isInstanceOf(ConnectException.class).hasMessageContaining("only applied");
        assertThatThrownBy(() -> identity.validateResume(2, 10, "a", 150)).hasMessageContaining("PERMANENT_MAGIC");
        assertThatThrownBy(() -> identity.validateResume(1, 11, "a", 150)).hasMessageContaining("another database branch");
        assertThatThrownBy(() -> identity.validateResume(1, 10, "absent", 150)).hasMessageContaining("anchor is absent");
    }

    @Test
    void shouldRejectDataLossEvenIfANewerPrimaryHasSinceAdvancedPastTheOldOffset() {
        final var identity = identity(30, 500, open("a", 1, 10, 100), open("b", 2, 20, 150), open("c", 3, 30, 400));
        assertThatThrownBy(() -> identity.validateResume(1, 10, "a", 200)).hasMessageContaining("only applied through LSN 150");
    }

    @Test
    void shouldRejectAConnectionBehindItsCurrentPrimaryOffset() {
        final var identity = identity(10, 150, open("a", 1, 10, 100));
        assertThatThrownBy(() -> identity.validateResume(1, 10, "a", 160)).hasMessageContaining("behind the acknowledged");
    }

    @Test
    void shouldMatchArchiveSourcesAcrossSwitchoversAndSwitchbackWithoutAcceptingDivergence() {
        final var identity = identity(10, 450, open("a", 1, 10, 100), open("a-restart", 2, 10, 150),
                open("b", 3, 20, 200), open("a-back", 4, 10, 300));
        assertThat(identity.containsArchiveSource(10, 100, 200)).isTrue();
        assertThat(identity.containsArchiveSource(20, 200, 300)).isTrue();
        assertThat(identity.containsArchiveSource(10, 300, 450)).isTrue();
        assertThat(identity.containsArchiveSource(10, 180, 220)).isFalse();
        assertThat(identity.containsArchiveSource(20, 299, 301)).isFalse();
        assertThat(identity.containsArchiveSource(99, 200, 300)).isFalse();
        assertThat(identity.containsArchiveSource(10, 50, 100)).isFalse();
    }

    @Test
    void shouldPersistAndReloadThePrimaryAnchorAndRejectUnanchoredSingleDatabaseOffsets() {
        final var config = new DamengConnectorConfig(DamengConnectorConfigTest.configuration().build());
        final var offset = new DamengOffsetContext(config, 120);
        offset.validatePrimaryStandbyIdentity(identity(10, 180, open("a", 1, 10, 100)));
        final var loaded = new DamengOffsetContext.Loader(config).load(offset.getOffset());
        loaded.validatePrimaryStandbyIdentity(identity(20, 250, open("a", 1, 10, 100), open("b", 2, 20, 200)));
        assertThat(loaded.getOffset().get(DamengOffsetContext.PRIMARY_OPEN_GUID_KEY)).isEqualTo("b");
        assertThat(loaded.getOffset().get(DamengOffsetContext.DATABASE_MAGIC_KEY)).isEqualTo(20L);
        final var oldOffset = new DamengOffsetContext(config, 120);
        oldOffset.validateDatabaseMagic(10);
        assertThatThrownBy(() -> oldOffset.validatePrimaryStandbyIdentity(identity(10, 180, open("a", 1, 10, 100))))
                .hasMessageContaining("no primary/standby lineage anchor");
    }

    private static DamengDatabaseIdentity identity(long magic, long lsn, OpenRecord... history) {
        final List<OpenRecord> chain = new ArrayList<>();
        for (int index = 0; index < history.length; index++) {
            final var record = history[index];
            chain.add(new OpenRecord(record.guid(), record.openedAt(), record.databaseMagic(), record.appliedLsn(),
                    index == 0 ? record.databaseMagic() : history[index - 1].databaseMagic()));
        }
        return new DamengDatabaseIdentity(1, magic, lsn, chain);
    }

    private static OpenRecord open(String guid, long time, long magic, long applied) {
        return new OpenRecord(guid, Instant.ofEpochSecond(time), magic, applied);
    }
}
