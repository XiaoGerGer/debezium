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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.dameng.compatibility.DamengCompatibilityEnvironment;
import io.debezium.connector.dameng.compatibility.DamengCompatibilityMode;
import io.debezium.connector.dameng.compatibility.DamengCompatibilityRegistry;
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
import io.debezium.relational.Tables.TableFilter;

/** JDBC connection and DM-specific LogMiner operations. */
public class DamengConnection extends JdbcConnection {
    private static final Logger LOGGER = LoggerFactory.getLogger(DamengConnection.class);
    public static final String URL = "url";
    // DM treats the path component as a schema name. The database name is not part of
    // the JDBC URL and is retained by the connector only as source metadata.
    private static final String URL_PATTERN = "jdbc:dm://${hostname}:${port}";
    private boolean logMiningStarted;
    private Charset databaseCharset;
    private Boolean caseInsensitive;
    private DamengDatabaseIdentity primaryStandbyIdentity;
    private DamengCompatibilityEnvironment compatibilityEnvironment;

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
            final var driverConfig = JdbcConfiguration.copy(jdbcConfig).without("failover.hosts").without("standby.hosts");
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

    public boolean isStandbyMining() {
        return !config().getString("standby.hosts", "").isBlank();
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
            final boolean expectedRole = isStandbyMining() ? "STANDBY".equalsIgnoreCase(mode) : primary || singleServer;
            if (!expectedRole || !"OPEN".equalsIgnoreCase(status)) {
                throw new SQLException("Dameng CDC requires an OPEN " + (isStandbyMining() ? "STANDBY" : isFailoverEnabled(config()) ? "PRIMARY" : "NORMAL or PRIMARY")
                        + " server; actual MODE$=" + mode + ", STATUS$=" + status + ". Reconnect to the configured capture role.", "08006");
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

    public DamengDatabaseVersion getDatabaseVersion() throws SQLException {
        return DamengDatabaseVersion.read(connection().getMetaData());
    }

    @Override
    public String quotedTableIdString(TableId tableId) {
        // Metadata contains raw names. Always escape them, including names that
        // start or end with a literal quote rather than an SQL quoting delimiter.
        return (tableId.schema() == null ? "" : quoteIdentifier(tableId.schema()) + ".") + quoteIdentifier(tableId.table());
    }

    /** Ensures signaling is ready before the initial snapshot discovers table schemas. */
    public boolean ensureSignalTable(TableId tableId) throws SQLException {
        if (signalTableExists(tableId)) {
            validateSignalTable(tableId);
            return false;
        }
        try {
            execute("CREATE TABLE " + quotedTableIdString(tableId)
                    + " (ID VARCHAR(64) PRIMARY KEY, TYPE VARCHAR(64) NOT NULL, DATA VARCHAR(2000))");
        }
        catch (SQLException creationFailure) {
            // Another task may have created the same table after our metadata lookup.
            // Reuse it only after checking that it is visible and has the expected shape.
            try {
                if (signalTableExists(tableId)) {
                    validateSignalTable(tableId);
                    return false;
                }
            }
            catch (SQLException validationFailure) {
                creationFailure.addSuppressed(validationFailure);
            }
            throw new SQLException("Could not create Dameng signal table " + tableId
                    + ". The configured schema must already exist and the connector account needs CREATE TABLE permission in that schema. "
                    + "Alternatively, pre-create the signal table and grant SELECT and INSERT (and DELETE for insert_delete watermarking). "
                    + "An existing table that is not visible to this account also requires appropriate grants.",
                    creationFailure.getSQLState(), creationFailure.getErrorCode(), creationFailure);
        }
        validateSignalTable(tableId);
        return true;
    }

    private boolean signalTableExists(TableId tableId) throws SQLException {
        // JDBC treats underscores and percent signs as patterns; compare the full
        // identity so a similarly named table cannot hide a missing signal table.
        return readTableNames(null, tableId.schema(), tableId.table(), new String[]{ "TABLE" })
                .contains(createTableId(null, tableId.schema(), tableId.table()));
    }

    private void validateSignalTable(TableId tableId) throws SQLException {
        query("SELECT * FROM " + quotedTableIdString(tableId) + " WHERE 1=0", result -> {
            final var metadata = result.getMetaData();
            final List<String> names = List.of("ID", "TYPE", "DATA");
            if (metadata.getColumnCount() != names.size()) {
                throw new SQLException("Dameng signal table " + tableId + " must contain exactly three columns in order: ID, TYPE, DATA; found "
                        + metadata.getColumnCount() + ". Existing tables are not altered automatically.");
            }
            for (int index = 1; index <= names.size(); index++) {
                final int type = metadata.getColumnType(index);
                if (!names.get(index - 1).equals(normalizeIdentifier(metadata.getColumnName(index))) || (type != Types.VARCHAR && type != Types.NVARCHAR)) {
                    throw new SQLException("Dameng signal table " + tableId + " column " + index + " must be named " + names.get(index - 1)
                            + " and use VARCHAR or NVARCHAR; found " + metadata.getColumnName(index) + " " + metadata.getColumnTypeName(index)
                            + ". Existing tables are not altered automatically.");
                }
            }
        });
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
                "SELECT T.ID, T.INFO3 FROM SYS.SYSOBJECTS T JOIN SYS.SYSOBJECTS S ON T.SCHID=S.ID "
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
                    // DM's system dictionary defines INFO3 byte 0 as the table kind.
                    // The verified image uses 0 for ordinary row tables and 6 for
                    // partition roots. HUGE/temporary/external kinds are not row CDC.
                    final long tableKind = result.getLong(2) & 255;
                    if (result.wasNull() || (tableKind != 0 && tableKind != 6)) {
                        throw new ConnectException("Captured Dameng table " + tableId + " has unsupported storage kind " + tableKind
                                + "; only ordinary row tables and partition roots have verified complete LogMiner images. "
                                + "Temporary, external and HUGE tables must be excluded");
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
                            constraints.add(Attribute.editor().name(DamengDdlParser.CONSTRAINT_PREFIX + normalizeIdentifier(result.getString(1))).value(kind).create());
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
        final var ignoreTableSet = getIniParameter("RLOG_IGNORE_TABLE_SET");
        if ("0".equals(ignoreTableSet)) {
            LOGGER.warn("Dameng RLOG_IGNORE_TABLE_SET=0: table-level logical logging must be enabled manually for every captured table, "
                    + "any configured signal table, and any tables added later. The connector does not verify table-level logging; "
                    + "missing logs can cause silent CDC data loss. Startup will continue.");
        }
        else if (!"1".equals(ignoreTableSet)) {
            throw new IllegalStateException("Dameng RLOG_IGNORE_TABLE_SET must be 0 (manual table-level logical logging) or 1; actual value was " + ignoreTableSet);
        }
        final var compression = getIniParameter("RLOG_LLOG_COMPRESS");
        if (compression != null && !"0".equals(compression)) {
            throw new IllegalStateException("Dameng RLOG_LLOG_COMPRESS must be 0; compressed INSERT logical logs cannot provide complete mined values");
        }
    }

    public void validateCompatibilityMode() throws SQLException {
        getCompatibilityEnvironment();
    }

    public DamengCompatibilityEnvironment getCompatibilityEnvironment() throws SQLException {
        final var mode = new DamengCompatibilityMode(getIniParameter("COMPATIBLE_MODE"));
        final var profile = DamengCompatibilityRegistry.builtIn().resolve(mode);
        final Map<String, String> parameters = new HashMap<>();
        for (var name : profile.parameterNames()) {
            final var value = getIniParameter(name);
            if (value != null) {
                parameters.put(name, value);
            }
        }
        parameters.put("CASE_SENSITIVE", getCaseSensitiveFlag());
        final var environment = new DamengCompatibilityEnvironment(mode, parameters);
        profile.validate(environment);
        return environment;
    }

    public String getCaseSensitiveFlag() throws SQLException {
        return queryAndMap("SELECT SF_GET_CASE_SENSITIVE_FLAG()", result -> {
            if (!result.next() || result.getString(1) == null) {
                throw new SQLException("Cannot determine Dameng CASE_SENSITIVE database initialization setting");
            }
            return result.getString(1);
        });
    }

    /** The flag is immutable for the lifetime of a database, unlike session lexical settings. */
    private String normalizeIdentifier(String name) {
        if (name == null) {
            return null;
        }
        if (caseInsensitive == null) {
            try {
                caseInsensitive = "0".equals(getCaseSensitiveFlag());
            }
            catch (SQLException e) {
                throw new ConnectException("Cannot determine Dameng identifier case semantics", e);
            }
        }
        return caseInsensitive ? name.toUpperCase(Locale.ROOT) : name;
    }

    @Override
    public TableId createTableId(String databaseName, String schemaName, String tableName) {
        return new TableId(null, normalizeIdentifier(schemaName), normalizeIdentifier(tableName));
    }

    public void validateCompatibilityEnvironment(DamengOffsetContext offset) throws SQLException {
        final var observed = getCompatibilityEnvironment();
        if (compatibilityEnvironment != null && !compatibilityEnvironment.equals(observed)) {
            throw new ConnectException("Dameng compatibility parameters changed during snapshot or streaming initialization; refusing to replace the original settings");
        }
        offset.validateCompatibilityEnvironment(observed);
        compatibilityEnvironment = observed;
    }

    /** One query per boundary, including idle windows; never silently replace the established parser. */
    public void validateUnchangedCompatibility() throws SQLException {
        if (compatibilityEnvironment == null) {
            throw new IllegalStateException("Dameng compatibility environment has not been bound to the offset");
        }
        final var names = new HashSet<>(DamengCompatibilityRegistry.builtIn().resolve(compatibilityEnvironment.mode()).parameterNames());
        names.add("COMPATIBLE_MODE");
        final var literals = names.stream().sorted().map(name -> "'" + LogMinerSql.quoteLiteral(name) + "'").collect(Collectors.joining(","));
        final Map<String, String> observed = queryAndMap("SELECT PARA_NAME, PARA_VALUE, SF_GET_CASE_SENSITIVE_FLAG() FROM V$DM_INI WHERE PARA_NAME IN (" + literals + ")",
                result -> {
                    final Map<String, String> parameters = new HashMap<>();
                    while (result.next()) {
                        final var value = result.getString(2);
                        if (value != null) {
                            parameters.put(result.getString(1), value);
                        }
                        final var sensitive = result.getString(3);
                        if (sensitive != null) {
                            parameters.put("CASE_SENSITIVE", sensitive);
                        }
                    }
                    return parameters;
                });
        final Map<String, String> expected = new HashMap<>(compatibilityEnvironment.parameters());
        expected.put("COMPATIBLE_MODE", compatibilityEnvironment.mode().value());
        if (!expected.equals(observed)) {
            throw new ConnectException("Dameng compatibility parameters changed while the task was running; refusing to publish this mining window. "
                    + "Restore the original settings only if no affected history was generated while they differed; otherwise start a new connector identity "
                    + "and initial snapshot. Expected " + expected + "; observed "
                    + observed);
        }
    }

    public DamengDdlParser createDdlParser() throws SQLException {
        final var environment = getCompatibilityEnvironment();
        return DamengCompatibilityRegistry.builtIn().resolve(environment.mode()).createDdlParser(environment, getDatabaseCharset());
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
        private final long bytes;

        public ArchiveLogFile(String path, long firstLsn, long lastLsn, long databaseMagic, long previousLsn) {
            this(path, firstLsn, lastLsn, databaseMagic, previousLsn, 0, 0, 0);
        }

        public ArchiveLogFile(String path, long firstLsn, long lastLsn, long databaseMagic, long previousLsn,
                              long permanentMagic, long sourceMagic, int nodeSequence) {
            this(path, firstLsn, lastLsn, databaseMagic, previousLsn, permanentMagic, sourceMagic, nodeSequence, 0);
        }

        public ArchiveLogFile(String path, long firstLsn, long lastLsn, long databaseMagic, long previousLsn,
                              long permanentMagic, long sourceMagic, int nodeSequence, long bytes) {
            this.path = path;
            this.firstLsn = firstLsn;
            this.lastLsn = lastLsn;
            this.databaseMagic = databaseMagic;
            this.previousLsn = previousLsn;
            this.permanentMagic = permanentMagic;
            this.sourceMagic = sourceMagic;
            this.nodeSequence = nodeSequence;
            this.bytes = bytes;
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

        /** V$ARCH_FILE.LEN, the whole file length in bytes, including its already scanned portion. */
        public long bytes() {
            return bytes;
        }
    }

    public List<ArchiveLogFile> readArchiveLogFiles() throws SQLException {
        return queryAndMap("SELECT PATH, ARCH_LSN, CLSN, DB_MAGIC, PREV_LSN, PMNT_MAGIC, SRC_DB_MAGIC, DSC_SEQNO, LEN "
                + "FROM V$ARCH_FILE WHERE PATH IS NOT NULL ORDER BY ARCH_LSN", rs -> {
                    List<ArchiveLogFile> result = new ArrayList<>();
                    while (rs.next()) {
                        long first = rs.getLong(2);
                        long last = rs.getLong(3);
                        if (!rs.wasNull() && last >= first) {
                            result.add(new ArchiveLogFile(rs.getString(1), first, last, rs.getLong(4), rs.getLong(5), rs.getLong(6), rs.getLong(7), rs.getInt(8),
                                    rs.getLong(9)));
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
        // A file boundary need not be numerically contiguous. A forward read
        // starts at the previous end + 1; PREV_LSN can prove that no log was
        // skipped before this file's larger first LSN. An inclusive restart AT
        // the missing predecessor still fails because that row is required.
        if (files.get(0).firstLsn() > startLsn && (startLsn <= 0 || files.get(0).previousLsn() <= 0 || files.get(0).previousLsn() != startLsn - 1)) {
            throw new IllegalStateException("Archive logs no longer contain the restart LSN " + startLsn + "; a new snapshot is required");
        }
        if (files.get(0).firstLsn() > startLsn) {
            coveredThrough = startLsn - 1;
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

    /** Both values describe the source primary's LSN domain, not this reader's local file position. */
    public record StandbyProgress(long replayableLsn, long replayedLsn) {
        public long safeLsn() {
            return Math.min(replayableLsn, replayedLsn);
        }
    }

    public StandbyProgress readStandbyProgress() throws SQLException {
        validateServerRole();
        final long replayable = queryAndMap("SELECT SLSN FROM V$RAPPLY_SYS", rows -> readSingleProgress(rows, "SLSN"));
        final long replayed = queryAndMap("SELECT RPKG_LSN FROM V$RAPPLY_PARALLEL_INFO WHERE DSC_SEQNO=0",
                rows -> readSingleProgress(rows, "RPKG_LSN"));
        validateServerRole();
        return new StandbyProgress(replayable, replayed);
    }

    private static long readSingleProgress(ResultSet rows, String field) throws SQLException {
        if (!rows.next()) {
            throw new SQLException("Dameng standby replay progress is unavailable: " + field, "08006");
        }
        final long lsn = rows.getLong(1);
        if (rows.wasNull() || lsn < 0 || rows.next()) {
            throw new SQLException("Dameng standby replay progress must contain one non-negative LSN: " + field, "08006");
        }
        return lsn;
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
        if (isStandbyMining() && identity.currentLsn() < Math.max(offset.getLsn(), Math.max(offset.getChangeLsn(), offset.getCommitLsn()))) {
            throw new SQLException("Dameng standby has not replayed through the stored offset; waiting for replay before recovery", "08006");
        }
        offset.validatePrimaryStandbyIdentity(identity);
        primaryStandbyIdentity = identity;
    }

    public void validateUnchangedPrimary() throws SQLException {
        validateServerRole();
        if (primaryStandbyIdentity != null) {
            final var current = readPrimaryStandbyIdentity();
            if (!current.openGuid().equals(primaryStandbyIdentity.openGuid()) || current.databaseMagic() != primaryStandbyIdentity.databaseMagic()
                    || current.permanentMagic() != primaryStandbyIdentity.permanentMagic()
                    || current.readingDatabaseMagic() != primaryStandbyIdentity.readingDatabaseMagic()) {
                throw new SQLException("Dameng primary changed while reading a mining window; reconnect and replay the window", "08006");
            }
        }
    }

    DamengDatabaseIdentity readPrimaryStandbyIdentity() throws SQLException {
        validateServerRole();
        final long magic = databaseMagic();
        final long sourceMagic = isStandbyMining() ? readSourcePrimaryMagic() : magic;
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
        final long lsn = isStandbyMining() ? readStandbyProgress().safeLsn() : currentLsn();
        // A switchover can update SYSOPENHISTORY while this session is still
        // attached to the former primary. Retry that transient role change
        // before interpreting the mixed reads as a divergent history.
        validateServerRole();
        if (databaseMagic() != magic || (isStandbyMining() && readSourcePrimaryMagic() != sourceMagic)) {
            throw new SQLException("Dameng database changed while validating its primary history", "08006");
        }
        if (isStandbyMining() && !history.isEmpty() && history.get(history.size() - 1).databaseMagic() != sourceMagic) {
            throw new SQLException("Dameng standby has not replayed the current primary-open record; retry after replication catches up", "08006");
        }
        return new DamengDatabaseIdentity(permanentMagic, sourceMagic, lsn, history, magic);
    }

    private long readSourcePrimaryMagic() throws SQLException {
        return queryAndMap("SELECT PRIMARY_DB_MAGIC FROM V$RLOG", result -> {
            if (!result.next()) {
                throw new SQLException("Dameng standby did not return its source primary identity", "08006");
            }
            final long magic = result.getLong(1);
            if (result.wasNull() || magic == 0 || result.next()) {
                throw new SQLException("Dameng standby source primary identity is missing or ambiguous", "08006");
            }
            return magic;
        });
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
        return readPrimaryKeyNames(metadata, id).stream().map(this::normalizeIdentifier).toList();
    }

    @Override
    protected Map<TableId, List<Column>> getColumnsDetails(String catalogName, String schemaName, String tableName,
                                                           TableFilter tableFilter, ColumnNameFilter columnFilter, DatabaseMetaData metadata,
                                                           Set<TableId> viewIds)
            throws SQLException {
        // The common table model indexes columns without case sensitivity. Never
        // let a valid DM table with "id" and "ID" silently lose a column.
        return super.getColumnsDetails(catalogName, schemaName, tableName, tableFilter, columnFilter, metadata, viewIds, true);
    }

    @Override
    protected Optional<ColumnEditor> readTableColumn(ResultSet metadata, TableId tableId, ColumnNameFilter filter) throws SQLException {
        final var column = super.readTableColumn(metadata, tableId, filter == null ? null
                : (catalog, schema, table, name) -> filter.matches(catalog, schema, table, normalizeIdentifier(name)));
        column.ifPresent(editor -> editor.name(normalizeIdentifier(editor.name())));
        if (column.isPresent() && column.get().isGenerated()) {
            throw new ConnectException("Captured Dameng table " + tableId + " contains virtual/generated column " + column.get().name()
                    + "; LogMiner returns NULL rather than its computed value. Snapshot/streaming consistency cannot be guaranteed");
        }
        column.ifPresent(editor -> {
            // DM's driver uses its own codes for the SQL standard time-zone types.
            if (editor.jdbcType() == 94) {
                editor.jdbcType(Types.TIME_WITH_TIMEZONE);
            }
            else if (editor.jdbcType() == 95) {
                editor.jdbcType(Types.TIMESTAMP_WITH_TIMEZONE);
            }
            else if (DamengInterval.isInterval(editor.typeName())) {
                // DatabaseMetaData uses DM-specific interval codes, while ResultSetMetaData uses OTHER.
                editor.jdbcType(Types.OTHER);
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
        if (DamengInterval.isInterval(column)) {
            // Use the standard textual interface, avoiding production dependencies on driver interval classes.
            return resultSet.getString(columnIndex);
        }
        return switch (column.jdbcType()) {
            // The driver's java.sql.Date/Timestamp conversion loses BCE era information.
            // Its textual representation retains the stored calendar, including the 1582 transition.
            case Types.DATE -> DamengTemporalValues.date(resultSet.getString(columnIndex));
            case Types.TIMESTAMP -> DamengTemporalValues.timestamp(resultSet.getString(columnIndex));
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
        return super.readTableNames(null, schemaNamePattern, tableNamePattern, tableTypes).stream().map(t -> createTableId(null, t.schema(), t.table()))
                .collect(Collectors.toSet());
    }

    @Override
    public Set<TableId> getAllTableIds(String catalogName) throws SQLException {
        Set<TableId> ids = new HashSet<>();
        query("SELECT OWNER, TABLE_NAME FROM ALL_TABLES", rs -> {
            while (rs.next()) {
                ids.add(createTableId(null, rs.getString(1), rs.getString(2)));
            }
        });
        return ids;
    }

    @Override
    public Optional<java.time.Instant> getCurrentTimestamp() throws SQLException {
        return queryAndMap("SELECT CURRENT_TIMESTAMP", rs -> rs.next() ? Optional.of(rs.getTimestamp(1).toInstant()) : Optional.empty());
    }
}
