/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng;

import java.nio.charset.Charset;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import io.debezium.DebeziumException;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.dameng.logminer.LogMinerSql;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Partition;
import io.debezium.relational.Attribute;
import io.debezium.relational.Column;
import io.debezium.relational.ColumnEditor;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables.ColumnNameFilter;

/** JDBC connection and DM-specific LogMiner operations. */
public class DamengConnection extends JdbcConnection {
    public static final String URL = "url";
    // DM treats the path component as a schema name. The database name is not part of
    // the JDBC URL and is retained by the connector only as source metadata.
    private static final String URL_PATTERN = "jdbc:dm://${hostname}:${port}";
    private boolean logMiningStarted;
    private Charset databaseCharset;
    private DamengDatabaseIdentity primaryStandbyIdentity;

    @Override
    protected void initializeStatement(Statement statement) throws SQLException {
        // JDBC accepts whole seconds. Round up so a positive subsecond timeout
        // never becomes zero (unlimited) in the driver.
        final long timeoutMs = config().getQueryTimeout().toMillis();
        statement.setQueryTimeout((int) ((timeoutMs + 999L) / 1_000L));
    }

    public DamengConnection(Configuration config) {
        this(JdbcConfiguration.adapt(config));
    }

    public DamengConnection(Configuration config, boolean autoCommit) {
        this(JdbcConfiguration.adapt(config), autoCommit);
    }

    public DamengConnection(JdbcConfiguration config) {
        super(config, connectionFactory(config), "\"", "\"");
        logMiningStarted = false;
    }

    public DamengConnection(JdbcConfiguration config, boolean autoCommit) {
        super(config, connectionFactory(config),
                statement -> statement.getConnection().setAutoCommit(autoCommit), "\"", "\"");
        logMiningStarted = false;
    }

    private static ConnectionFactory connectionFactory(JdbcConfiguration config) {
        final var factory = JdbcConnection.patternBasedFactory(connectionUrlPattern(config), "dm.jdbc.driver.DmDriver", DamengConnection.class.getClassLoader());
        return jdbcConfig -> {
            final var driverConfig = JdbcConfiguration.copy(jdbcConfig).without("failover.hosts");
            if (isFailoverEnabled(config)) {
                // The endpoint list in the URL owns routing. The raw hostname and
                // port properties must not change the driver's service resolution.
                driverConfig.without("hostname").without("port");
            }
            return factory.connect(driverConfig.build());
        };
    }

    static String connectionUrlPattern(JdbcConfiguration config) {
        if (isFailoverEnabled(config)) {
            return "jdbc:dm://debezium?debezium=(" + config.getString("failover.hosts") + ")";
        }
        return URL_PATTERN;
    }

    private static boolean isFailoverEnabled(JdbcConfiguration config) {
        return !config.getString("failover.hosts", "").isBlank();
    }

    /** Checks the live session because an existing primary connection can be demoted in place. */
    public void validateServerRole() throws SQLException {
        queryAndMap("SELECT MODE$, STATUS$ FROM V$INSTANCE", result -> {
            if (!result.next()) {
                throw new SQLException("Dameng V$INSTANCE did not return its server role and status", "08006");
            }
            final var mode = result.getString(1);
            final var status = result.getString(2);
            final boolean primary = "PRIMARY".equalsIgnoreCase(mode);
            final boolean singleServer = !isFailoverEnabled(config()) && "NORMAL".equalsIgnoreCase(mode);
            if (!(primary || singleServer) || !"OPEN".equalsIgnoreCase(status)) {
                throw new SQLException("Dameng CDC requires an OPEN " + (isFailoverEnabled(config()) ? "PRIMARY" : "NORMAL or PRIMARY")
                        + " server; actual MODE$=" + mode + ", STATUS$=" + status + ". Reconnect to the writable primary before mining.", "08006");
            }
            return null;
        });
    }

    public long currentLsn() throws SQLException {
        return queryAndMap("SELECT CUR_LSN FROM V$RLOG", rs -> {
            if (!rs.next()) {
                throw new SQLException("V$RLOG did not return the current LSN");
            }
            final long lsn = rs.getLong(1);
            if (rs.wasNull()) {
                throw new SQLException("V$RLOG.CUR_LSN is null");
            }
            return lsn;
        });
    }

    public String getRealDatabaseName() throws SQLException {
        return queryAndMap("SELECT NAME FROM V$DATABASE", rs -> rs.next() ? rs.getString(1) : null);
    }

    public String getDatabaseName() throws SQLException {
        return getRealDatabaseName();
    }

    public Charset getDatabaseCharset() throws SQLException {
        if (databaseCharset == null) {
            databaseCharset = queryAndMap("SELECT SF_GET_UNICODE_FLAG()", result -> {
                if (!result.next()) {
                    throw new SQLException("DM did not return its database character set");
                }
                final int flag = result.getInt(1);
                return switch (flag) {
                    case 0 -> Charset.forName("GB18030");
                    case 1 -> Charset.forName("UTF-8");
                    case 2 -> Charset.forName("EUC-KR");
                    default -> throw new SQLException("Unsupported DM database character set flag " + flag);
                };
            });
        }
        return databaseCharset;
    }

    /** Returns the object identity used in V$LOGMNR_CONTENTS.DATA_OBJ#. */
    public long getTableObjectId(TableId tableId) throws SQLException {
        return prepareQueryAndMap(
                "SELECT T.ID FROM SYS.SYSOBJECTS T JOIN SYS.SYSOBJECTS S ON T.SCHID=S.ID "
                        + "WHERE S.NAME=? AND S.TYPE$='SCH' AND T.NAME=? AND T.TYPE$='SCHOBJ' AND T.SUBTYPE$='UTAB'",
                statement -> {
                    statement.setString(1, tableId.schema());
                    statement.setString(2, tableId.table());
                },
                result -> {
                    if (!result.next()) {
                        throw new SQLException("No Dameng table object exists for " + tableId);
                    }
                    final long objectId = result.getLong(1);
                    if (result.wasNull() || objectId <= 0) {
                        throw new SQLException("Invalid Dameng table object identity for " + tableId);
                    }
                    return objectId;
                });
    }

    @Override
    protected Map<TableId, List<Attribute>> getAttributeDetails(TableId tableId, String tableType) {
        try {
            final long objectId = getTableObjectId(tableId);
            final List<Attribute> attributes = prepareQueryAndMap(
                    "SELECT O.NAME, C.TYPE$ FROM SYS.SYSCONS C JOIN SYS.SYSOBJECTS O ON O.ID=C.ID WHERE C.TABLEID=?",
                    statement -> statement.setLong(1, objectId), result -> {
                        final List<Attribute> constraints = new ArrayList<>();
                        while (result.next()) {
                            final String kind = switch (result.getString(2)) {
                                case "P" -> "PRIMARY";
                                case "U" -> "UNIQUE";
                                case "F" -> "FOREIGN";
                                case "C" -> "CHECK";
                                default -> throw new SQLException("Unsupported Dameng constraint kind for " + tableId + ": " + result.getString(2));
                            };
                            constraints.add(Attribute.editor().name(DamengDdlParser.CONSTRAINT_PREFIX + result.getString(1)).value(kind).create());
                        }
                        return constraints;
                    });
            return Map.of(tableId, attributes);
        }
        catch (SQLException e) {
            throw new DebeziumException("Failed to read Dameng constraint attributes for " + tableId, e);
        }
    }

    public boolean isArchiveModeEnabled() throws SQLException {
        return queryAndMap("SELECT ARCH_MODE FROM V$DATABASE", rs -> {
            if (!rs.next()) {
                return false;
            }
            String mode = rs.getString(1);
            return "Y".equalsIgnoreCase(mode) || "1".equals(mode) || "ARCHIVE".equalsIgnoreCase(mode);
        });
    }

    public String getIniParameter(String name) throws SQLException {
        return queryAndMap("SELECT PARA_VALUE FROM V$DM_INI WHERE UPPER(PARA_NAME)=UPPER('" + LogMinerSql.quoteLiteral(name) + "')",
                rs -> rs.next() ? rs.getString(1) : null);
    }

    public void validateLogMiningConfiguration() throws SQLException {
        validateCompatibilityMode();
        if (!isArchiveModeEnabled()) {
            throw new IllegalStateException("Dameng ARCH_MODE is disabled; DBMS_LOGMNR only mines archived logs");
        }
        String logic = getIniParameter("RLOG_APPEND_LOGIC");
        if (!"2".equals(logic)) {
            throw new IllegalStateException("Dameng RLOG_APPEND_LOGIC must be 2 for complete UPDATE and DELETE images; actual value was " + logic);
        }
        if (!"1".equals(getIniParameter("RLOG_IGNORE_TABLE_SET"))) {
            throw new IllegalStateException("Dameng RLOG_IGNORE_TABLE_SET must be 1 so logical logging covers every captured table, including newly created tables");
        }
        final var compression = getIniParameter("RLOG_LLOG_COMPRESS");
        if (compression != null && !"0".equals(compression)) {
            throw new IllegalStateException("Dameng RLOG_LLOG_COMPRESS must be 0; compressed INSERT logical logs cannot provide complete mined values");
        }
    }

    public void validateCompatibilityMode() throws SQLException {
        final var compatibilityMode = getIniParameter("COMPATIBLE_MODE");
        if (compatibilityMode != null && !"0".equals(compatibilityMode)) {
            throw new IllegalStateException("Dameng connector currently requires COMPATIBLE_MODE=0 to preserve native data type semantics");
        }
        final var lengthInChar = getIniParameter("LENGTH_IN_CHAR");
        if (!"0".equals(lengthInChar)) {
            // Redo DDL does not identify the original session's default length semantics.
            // Reinterpreting it with the mining session's defaults can corrupt CHAR padding.
            throw new IllegalStateException("Dameng connector currently requires LENGTH_IN_CHAR=0 for verified native byte length semantics; actual value was "
                    + lengthInChar);
        }
    }

    public static final class ArchiveLogFile {
        private final String path;
        private final long firstLsn;
        private final long lastLsn;
        private final long databaseMagic;
        private final long previousLsn;
        private final long permanentMagic;
        private final long sourceMagic;
        private final int nodeSequence;

        public ArchiveLogFile(String path, long firstLsn, long lastLsn, long databaseMagic, long previousLsn) {
            this(path, firstLsn, lastLsn, databaseMagic, previousLsn, 0, 0, 0);
        }

        public ArchiveLogFile(String path, long firstLsn, long lastLsn, long databaseMagic, long previousLsn,
                              long permanentMagic, long sourceMagic, int nodeSequence) {
            this.path = path;
            this.firstLsn = firstLsn;
            this.lastLsn = lastLsn;
            this.databaseMagic = databaseMagic;
            this.previousLsn = previousLsn;
            this.permanentMagic = permanentMagic;
            this.sourceMagic = sourceMagic;
            this.nodeSequence = nodeSequence;
        }

        public String path() {
            return path;
        }

        public long firstLsn() {
            return firstLsn;
        }

        public long lastLsn() {
            return lastLsn;
        }

        public long databaseMagic() {
            return databaseMagic;
        }

        public long previousLsn() {
            return previousLsn;
        }
    }

    public List<ArchiveLogFile> readArchiveLogFiles() throws SQLException {
        return queryAndMap("SELECT PATH, ARCH_LSN, CLSN, DB_MAGIC, PREV_LSN, PMNT_MAGIC, SRC_DB_MAGIC, DSC_SEQNO "
                + "FROM V$ARCH_FILE WHERE PATH IS NOT NULL ORDER BY ARCH_LSN", rs -> {
                    List<ArchiveLogFile> result = new ArrayList<>();
                    while (rs.next()) {
                        long first = rs.getLong(2);
                        long last = rs.getLong(3);
                        if (!rs.wasNull() && last >= first) {
                            result.add(new ArchiveLogFile(rs.getString(1), first, last, rs.getLong(4), rs.getLong(5), rs.getLong(6), rs.getLong(7), rs.getInt(8)));
                        }
                    }
                    return result;
                });
    }

    /**
     * Returns the archive files covering the requested LSN interval. DM LogMiner cannot
     * reconstruct a transaction when an archive is missing, so gaps and mixed database
     * generations are reported instead of silently mining a partial interval.
     */
    public List<ArchiveLogFile> archiveLogsForRange(long startLsn, long endLsn) throws SQLException {
        if (endLsn < startLsn) {
            throw new IllegalArgumentException("end LSN must not be less than start LSN");
        }
        List<ArchiveLogFile> files = readArchiveLogFiles().stream()
                .filter(file -> file.lastLsn() >= startLsn && file.firstLsn() <= endLsn)
                .sorted(Comparator.comparingLong(ArchiveLogFile::firstLsn))
                .collect(Collectors.toList());
        if (files.isEmpty()) {
            throw new IllegalStateException("No archived log contains LSN range " + startLsn + ".." + endLsn);
        }
        final long magic = databaseMagic();
        final var identity = primaryStandbyIdentity();
        if (magic == 0) {
            throw new IllegalStateException("Archive log DB_MAGIC is unavailable; refusing to mine an unverified archive range");
        }
        long coveredThrough = startLsn;
        if (files.get(0).firstLsn() > startLsn) {
            throw new IllegalStateException("Archive logs no longer contain the restart LSN " + startLsn + "; a new snapshot is required");
        }
        ArchiveLogFile previousFile = null;
        for (ArchiveLogFile file : files) {
            if (file.databaseMagic() == 0 || file.databaseMagic() != magic) {
                throw new IllegalStateException("Archive logs belong to different Dameng databases (DB_MAGIC mismatch)");
            }
            // Validate the whole file. Clipping to this mining window could hide
            // an old primary's divergent tail across a promotion boundary.
            if (identity != null && (file.nodeSequence != 0 || file.permanentMagic != identity.permanentMagic()
                    || !identity.containsArchiveSource(file.sourceMagic, file.firstLsn(), file.lastLsn()))) {
                throw new IllegalStateException("Dameng archive does not belong to the verified primary history at LSN " + file.firstLsn
                        + "; refusing foreign, divergent or unsupported multi-node archive data");
            }
            if (identity != null && previousFile != null && file.firstLsn() < previousFile.lastLsn()) {
                // A source magic may occur again after switching back to a former
                // primary. Overlapping files cannot safely identify which branch
                // owns the redo; only the shared inclusive endpoint is allowed.
                throw new IllegalStateException("Overlapping Dameng primary/standby archives " + previousFile.path() + " and " + file.path()
                        + "; refusing an ambiguous or divergent redo history");
            }
            if (file.firstLsn() > coveredThrough && file.firstLsn() - coveredThrough > 1
                    && file.previousLsn() != coveredThrough) {
                throw new IllegalStateException("Archive log gap before LSN " + file.firstLsn() + " (needed " + coveredThrough + ")");
            }
            if (file.firstLsn() > startLsn && file.previousLsn() > coveredThrough) {
                throw new IllegalStateException("Archive log chain is incomplete before " + file.path() + ": missing predecessor LSN " + file.previousLsn());
            }
            // Archive boundaries are inclusive in V$ARCH_FILE. Consecutive files can
            // therefore share the boundary LSN without representing a gap.
            coveredThrough = Math.max(coveredThrough, file.lastLsn());
            previousFile = file;
        }
        if (coveredThrough < endLsn) {
            throw new IllegalStateException("Archive logs end at LSN " + coveredThrough + ", before requested " + endLsn);
        }
        return files;
    }

    DamengDatabaseIdentity primaryStandbyIdentity() {
        return primaryStandbyIdentity;
    }

    public Optional<ArchiveLogFile> archiveLogContaining(long lsn) throws SQLException {
        return readArchiveLogFiles().stream().filter(f -> f.firstLsn() <= lsn && lsn <= f.lastLsn()).findFirst();
    }

    public long earliestArchiveLsn() throws SQLException {
        return readArchiveLogFiles().stream().mapToLong(ArchiveLogFile::firstLsn).min().orElse(0L);
    }

    public long latestArchivedLsn() throws SQLException {
        return readArchiveLogFiles().stream().mapToLong(ArchiveLogFile::lastLsn).max().orElse(0L);
    }

    /** Checks whether an existing offset can still be resumed before recovering schema history. */
    public boolean validateLogPosition(Partition partition, OffsetContext offset, CommonConnectorConfig config) {
        final long restartLsn = ((DamengOffsetContext) offset).getLsn();
        try {
            if (restartLsn < 0 || restartLsn > currentLsn()) {
                return false;
            }
            final var archives = readArchiveLogFiles();
            if (archives.isEmpty()) {
                return false;
            }
            // The newest redo may still be waiting for archival. Validate the lower
            // retention boundary here; the mining loop waits for the upper boundary.
            final long firstAvailableLsn = archives.stream().mapToLong(ArchiveLogFile::firstLsn).min().orElseThrow();
            return firstAvailableLsn <= restartLsn;
        }
        catch (SQLException e) {
            throw new DebeziumException("Unable to validate the stored Dameng log position", e);
        }
    }

    public long databaseMagic() throws SQLException {
        return queryAndMap("SELECT DB_MAGIC FROM V$RLOG", rs -> {
            if (!rs.next()) {
                throw new SQLException("V$RLOG did not return the database identity");
            }
            final long magic = rs.getLong(1);
            if (rs.wasNull()) {
                throw new SQLException("V$RLOG.DB_MAGIC is null");
            }
            return magic;
        });
    }

    public void validatePrimaryStandbyIdentity(DamengOffsetContext offset) throws SQLException {
        final var identity = readPrimaryStandbyIdentity();
        offset.validatePrimaryStandbyIdentity(identity);
        primaryStandbyIdentity = identity;
    }

    public void validateUnchangedPrimary() throws SQLException {
        validateServerRole();
        if (primaryStandbyIdentity != null) {
            final var current = readPrimaryStandbyIdentity();
            if (!current.openGuid().equals(primaryStandbyIdentity.openGuid()) || current.databaseMagic() != primaryStandbyIdentity.databaseMagic()
                    || current.permanentMagic() != primaryStandbyIdentity.permanentMagic()) {
                throw new SQLException("Dameng primary changed while reading a mining window; reconnect and replay the window", "08006");
            }
        }
    }

    private DamengDatabaseIdentity readPrimaryStandbyIdentity() throws SQLException {
        validateServerRole();
        final long magic = databaseMagic();
        final long permanentMagic = queryAndMap("SELECT PERMANENT_MAGIC()", result -> {
            if (!result.next()) {
                throw new SQLException("Dameng did not return its permanent database identity");
            }
            return result.getLong(1);
        });
        final List<DamengDatabaseIdentity.OpenRecord> history = queryAndMap(
                "SELECT RGUID, OPEN_TIME, CUR_DB_MAGIC, APPLY_LSN_0, N_EP, PRIMARY_DB_MAGIC FROM SYS.SYSOPENHISTORY "
                        + "WHERE OPEN_TIME IS NOT NULL ORDER BY OPEN_TIME",
                result -> {
                    final List<DamengDatabaseIdentity.OpenRecord> records = new ArrayList<>();
                    while (result.next()) {
                        if (result.getInt(5) != 1) {
                            throw new IllegalStateException("Dameng primary/standby recovery currently requires single-instance members (N_EP=1)");
                        }
                        records.add(new DamengDatabaseIdentity.OpenRecord(result.getString(1), result.getTimestamp(2).toInstant(),
                                result.getLong(3), result.getLong(4), result.getLong(6)));
                    }
                    return records;
                });
        final long lsn = currentLsn();
        // A switchover can update SYSOPENHISTORY while this session is still
        // attached to the former primary. Retry that transient role change
        // before interpreting the mixed reads as a divergent history.
        validateServerRole();
        if (databaseMagic() != magic) {
            throw new SQLException("Dameng database changed while validating its primary history", "08006");
        }
        return new DamengDatabaseIdentity(permanentMagic, magic, lsn, history);
    }

    public void startLogMining(long startLsn, long endLsn, boolean committedOnly) throws SQLException {
        List<ArchiveLogFile> files = archiveLogsForRange(startLsn, endLsn);
        if (logMiningStarted) {
            execute(LogMinerSql.end());
            logMiningStarted = false;
        }
        for (ArchiveLogFile file : files) {
            execute(LogMinerSql.addLogFile(file.path()));
        }
        execute(LogMinerSql.start(startLsn, endLsn, committedOnly));
        logMiningStarted = true;
    }

    public void endLogMining() throws SQLException {
        if (logMiningStarted) {
            execute(LogMinerSql.end());
            logMiningStarted = false;
        }
    }

    public String logMinerContentsQuery(List<TableId> tables, boolean committedOnly) {
        return LogMinerSql.contentsQuery(tables, committedOnly);
    }

    public String logMinerContentsQuery(boolean committedOnly) {
        return LogMinerSql.contentsQuery(null, committedOnly);
    }

    @Override
    protected String resolveCatalogName(String catalogName) {
        return null;
    }

    @Override
    protected List<String> readPrimaryKeyOrUniqueIndexNames(DatabaseMetaData metadata, TableId id) throws SQLException {
        // Streaming tracks actual PRIMARY KEY constraints. Promoting a snapshot's
        // unique index to a primary key would retain an invalid event key after
        // DROP INDEX or DROP CONSTRAINT, and differ from tables created in redo.
        return readPrimaryKeyNames(metadata, id);
    }

    @Override
    protected Optional<ColumnEditor> readTableColumn(ResultSet metadata, TableId tableId, ColumnNameFilter filter) throws SQLException {
        final var column = super.readTableColumn(metadata, tableId, filter);
        column.ifPresent(editor -> {
            // DM's driver uses its own codes for the SQL standard time-zone types.
            if (editor.jdbcType() == 94) {
                editor.jdbcType(Types.TIME_WITH_TIMEZONE);
            }
            else if (editor.jdbcType() == 95) {
                editor.jdbcType(Types.TIMESTAMP_WITH_TIMEZONE);
            }
        });
        if (column.isPresent() && (column.get().jdbcType() == Types.CHAR || column.get().jdbcType() == Types.NCHAR)) {
            final var editor = column.get();
            editor.charsetName(getDatabaseCharset().name());
            prepareQuery("SELECT CHAR_USED, CHAR_LENGTH, DATA_LENGTH FROM ALL_TAB_COLUMNS WHERE OWNER=? AND TABLE_NAME=? AND COLUMN_NAME=?",
                    statement -> {
                        statement.setString(1, tableId.schema());
                        statement.setString(2, tableId.table());
                        statement.setString(3, editor.name());
                    }, result -> {
                        if (!result.next()) {
                            throw new SQLException("Cannot resolve character length semantics for " + tableId + "." + editor.name());
                        }
                        final boolean characterLength = "C".equals(result.getString(1));
                        final int length = result.getInt(characterLength ? 2 : 3);
                        editor.length(length).type(editor.typeName(), editor.typeName() + "(" + length + (characterLength ? " CHAR)" : " BYTE)"));
                    });
        }
        return column;
    }

    @Override
    public Object getColumnValue(ResultSet resultSet, int columnIndex, Column column, Table table) throws SQLException {
        return switch (column.jdbcType()) {
            case Types.TIME -> resultSet.getObject(columnIndex, LocalTime.class);
            case Types.TIMESTAMP_WITH_TIMEZONE -> resultSet.getObject(columnIndex, OffsetDateTime.class);
            case Types.TIME_WITH_TIMEZONE -> resultSet.getObject(columnIndex, OffsetTime.class);
            case Types.CLOB, Types.NCLOB -> resultSet.getString(columnIndex);
            case Types.BLOB -> resultSet.getBytes(columnIndex);
            default -> super.getColumnValue(resultSet, columnIndex, column, table);
        };
    }

    @Override
    public Set<TableId> readTableNames(String catalogName, String schemaNamePattern, String tableNamePattern, String[] tableTypes) throws SQLException {
        return super.readTableNames(null, schemaNamePattern, tableNamePattern, tableTypes).stream().map(t -> new TableId(null, t.schema(), t.table()))
                .collect(Collectors.toSet());
    }

    @Override
    public Set<TableId> getAllTableIds(String catalogName) throws SQLException {
        Set<TableId> ids = new HashSet<>();
        query("SELECT OWNER, TABLE_NAME FROM ALL_TABLES", rs -> {
            while (rs.next()) {
                ids.add(new TableId(null, rs.getString(1), rs.getString(2)));
            }
        });
        return ids;
    }

    @Override
    public Optional<java.time.Instant> getCurrentTimestamp() throws SQLException {
        return queryAndMap("SELECT CURRENT_TIMESTAMP", rs -> rs.next() ? Optional.of(rs.getTimestamp(1).toInstant()) : Optional.empty());
    }
}
