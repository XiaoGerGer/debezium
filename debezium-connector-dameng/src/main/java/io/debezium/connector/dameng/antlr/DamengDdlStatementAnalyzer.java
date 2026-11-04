/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng.antlr;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.antlr.v4.runtime.ParserRuleContext;

import io.debezium.connector.dameng.antlr.DamengParseTree.Parsed;
import io.debezium.connector.dameng.compatibility.DamengIdentifierRules;
import io.debezium.ddl.parser.dameng.generated.DamengSqlLexer;
import io.debezium.ddl.parser.dameng.generated.DamengSqlParser;
import io.debezium.relational.TableId;

/** Determines the scope of one archived statement before capture filtering or model updates. */
public final class DamengDdlStatementAnalyzer {
    private final boolean nestedComments;
    private final DamengIdentifierRules identifiers;

    public DamengDdlStatementAnalyzer() {
        this(false, new DamengIdentifierRules(false));
    }

    public DamengDdlStatementAnalyzer(boolean nestedComments, DamengIdentifierRules identifiers) {
        this.nestedComments = nestedComments;
        this.identifiers = Objects.requireNonNull(identifiers, "identifiers");
    }

    public enum Impact {
        TABLE,
        NO_TABLE_CHANGE,
        UNSUPPORTED
    }

    /** Recognized cascading object drops require their dedicated opt-in before capture filtering. */
    public static final class CascadeDropException extends IllegalArgumentException {
        private static final long serialVersionUID = 1L;

        private CascadeDropException(ParserRuleContext context, String operation, String target) {
            super(DamengParseTree.error(context, "Unsupported cascading " + operation + " on " + target
                    + ": cannot determine all affected tables from this redo; dependent tables or other objects may have been removed. "
                    + "Ignoring leaves historical schemas and object-ID mappings unchanged and emits no deletion events; downstream data may remain, "
                    + "and same-name recreation or object-ID reuse may cause later failures or incorrect decoding. "
                    + "Set cascade.drop.ignore=true to ignore this statement and continue CDC while accepting these consequences").getMessage());
        }
    }

    /** Parsed partition drops and truncates require their dedicated opt-in, not the generic syntax-error switch. */
    public static final class PartitionDataRemovalException extends IllegalArgumentException {
        private static final long serialVersionUID = 1L;

        private PartitionDataRemovalException(ParserRuleContext context, TableId table, String operation) {
            super(DamengParseTree.error(context, "Unsupported " + operation + " on " + table + ": DM can remove partition rows without per-row DELETE redo; "
                    + "set partition.drop.ignore=true to ignore this statement and continue CDC; removed rows may remain downstream").getMessage());
        }
    }

    public record Analysis(String sql, List<DamengSqlToken> tokens, Impact impact, TableId tableId, TableId renamedTableId,
            IllegalArgumentException failure, DamengSqlParser.TableStatementContext tableStatement, String defaultSchema) {
        public Analysis(String sql, List<DamengSqlToken> tokens, Impact impact, TableId tableId, TableId renamedTableId,
                        IllegalArgumentException failure) {
            this(sql, tokens, impact, tableId, renamedTableId, failure, null, tableId == null ? null : tableId.schema());
        }

        public Analysis requireSupported() {
            if (failure != null) {
                throw failure;
            }
            return this;
        }

        public boolean isCtas() {
            return tableStatement != null && tableStatement.createTable() != null && tableStatement.createTable().ctasQuery() != null;
        }

        public boolean isTemporaryTable() {
            return tableStatement != null && tableStatement.createTable() != null && tableStatement.createTable().TEMPORARY() != null;
        }
    }

    public Analysis analyze(String sql, String defaultSchema) {
        // Lexical errors retain their original throwing contract, before scope classification.
        final var lexed = DamengAntlrSqlLexer.lex(sql, nestedComments);
        try {
            return analyzeScope(DamengParseTree.parseScope(sql, lexed), defaultSchema);
        }
        catch (IllegalArgumentException failure) {
            return unsupported(sql, lexed.tokens(), defaultSchema, failure);
        }
    }

    public Analysis analyzeTokens(String sql, List<DamengSqlToken> tokens, String defaultSchema) {
        try {
            return analyzeScope(DamengParseTree.parseTokens(sql, tokens), defaultSchema);
        }
        catch (IllegalArgumentException failure) {
            return unsupported(sql, tokens, defaultSchema, failure);
        }
    }

    /** Parses complete scripts, including stored program bodies, before planning any historical changes. */
    public List<Analysis> analyzeScript(String sql, String defaultSchema) {
        final var parsed = DamengParseTree.parseScript(sql, nestedComments);
        final List<Analysis> result = new ArrayList<>();
        for (var script : parsed.tree().scriptStatement()) {
            result.add(analyzeStatement(sql, script.statement(), defaultSchema));
        }
        return List.copyOf(result);
    }

    private Analysis analyzeScope(Parsed<DamengSqlParser.DdlScopeContext> parsed, String defaultSchema) {
        final var scope = parsed.tree();
        for (var statement : scope.statement()) {
            rejectCascadeDrop(statement, scope.statement().size() == 1);
            final var nonTable = statement.nonTableStatement();
            if (scope.statement().size() != 1 && nonTable != null
                    && (nonTable.programDefinition() != null || nonTable.javaClassDefinition() != null)) {
                throw DamengParseTree.error(statement,
                        "Unsupported compound DDL: a stored definition must occupy the complete LogMiner record");
            }
            if (statement.tableStatement() != null) {
                if (scope.statement().size() != 1) {
                    throw DamengParseTree.error(statement,
                            "Unsupported table DDL: one LogMiner record must contain exactly one table statement");
                }
                return analyzeStatement(parsed.sql(), statement, defaultSchema);
            }
            rejectPrebuiltMaterializedView(statement);
        }
        return noTableChange(parsed.sql(), parsed.tokens(), defaultSchema);
    }

    private Analysis analyzeStatement(String sql, DamengSqlParser.StatementContext statement, String defaultSchema) {
        rejectCascadeDrop(statement, true);
        final var table = statement.tableStatement();
        if (table == null) {
            rejectPrebuiltMaterializedView(statement);
            return noTableChange(sql, DamengParseTree.tokens(statement), defaultSchema);
        }
        final var target = tableId(target(table), defaultSchema);
        if (table.alterTable() != null && table.alterTable().alterTableClause() != null) {
            final var maintenance = table.alterTable().alterTableClause().partitionMaintenanceClause();
            if (maintenance != null) {
                if (maintenance.partitionDropClause() != null) {
                    throw new PartitionDataRemovalException(maintenance, target,
                            maintenance.partitionDropClause().SUBPARTITION() != null ? "DROP SUBPARTITION" : "DROP PARTITION");
                }
                if (maintenance.TRUNCATE() != null) {
                    throw new PartitionDataRemovalException(maintenance, target,
                            maintenance.SUBPARTITION().isEmpty() ? "TRUNCATE PARTITION" : "TRUNCATE SUBPARTITION");
                }
                if (maintenance.EXCHANGE() != null) {
                    throw DamengParseTree.error(maintenance,
                            "Unsupported partition " + maintenance.getStart().getText()
                                    + ": changes logical rows without complete per-row DML; cannot treat it as whole-table TRUNCATE or physical maintenance");
                }
                return noTableChange(sql, DamengParseTree.tokens(statement), defaultSchema);
            }
        }
        if (table.truncateTable() != null && (table.truncateTable().PARTITION() != null || table.truncateTable().LPAREN() != null)) {
            throw new PartitionDataRemovalException(table.truncateTable(), target, "TRUNCATE PARTITION");
        }
        final var destination = table.alterTable() != null ? table.alterTable().destination
                : table.renameTable() != null ? table.renameTable().destination : null;
        final var renamed = destination == null ? null : tableId(destination, target.schema());
        final var analysis = new Analysis(sql, DamengParseTree.tokens(table), Impact.TABLE, target, renamed, null, table,
                identifiers.canonicalName(defaultSchema));
        rejectAdvancedLog(analysis);
        return analysis;
    }

    /** Reject hidden log-table changes before capture filtering or historical model updates. */
    public static void rejectAdvancedLog(Analysis analysis) {
        final var table = analysis.tableStatement();
        ParserRuleContext advancedLog = null;
        if (table.createTable() != null) {
            for (var option : table.createTable().tableOption()) {
                if (option.ADVANCED() != null) {
                    advancedLog = option;
                    break;
                }
            }
        }
        else if (table.alterTable() != null && table.alterTable().alterTableClause() != null) {
            advancedLog = table.alterTable().alterTableClause().advancedLogClause();
        }
        if (advancedLog != null) {
            final var target = analysis.tableId();
            final var auxiliary = new TableId(target.catalog(), target.schema(), target.table() + "$ALOG");
            final var action = switch (advancedLog.getStart().getType()) {
                case DamengSqlLexer.WITHOUT -> "drops";
                case DamengSqlLexer.TRUNCATE -> "truncates";
                default -> "creates";
            };
            throw DamengParseTree.error(advancedLog, "Unsupported ADVANCED LOG: this statement also " + action
                    + " implicit auxiliary table " + auxiliary + "; historical processing currently supports one table per DDL record");
        }
    }

    private static void rejectCascadeDrop(DamengSqlParser.StatementContext statement, boolean standalone) {
        final var nonTable = statement.nonTableStatement();
        if (nonTable == null) {
            return;
        }
        final ParserRuleContext context;
        final ParserRuleContext target;
        final String operation;
        if (nonTable.dropUser() != null && nonTable.dropUser().CASCADE() != null) {
            context = nonTable.dropUser();
            target = nonTable.dropUser().target;
            operation = "DROP USER";
        }
        else if (nonTable.dropSchema() != null && nonTable.dropSchema().CASCADE() != null) {
            context = nonTable.dropSchema();
            target = nonTable.dropSchema().target;
            operation = "DROP SCHEMA";
        }
        else if (nonTable.dropDomain() != null && nonTable.dropDomain().CASCADE() != null) {
            context = nonTable.dropDomain();
            target = nonTable.dropDomain().target;
            operation = "DROP DOMAIN";
        }
        else if (nonTable.dropType() != null && nonTable.dropType().BODY() == null && nonTable.dropType().CASCADE() != null) {
            context = nonTable.dropType();
            target = nonTable.dropType().target;
            operation = "DROP TYPE";
        }
        else if (nonTable.dropClass() != null && nonTable.dropClass().CASCADE() != null) {
            context = nonTable.dropClass();
            target = nonTable.dropClass().target;
            operation = "DROP CLASS";
        }
        else {
            return;
        }
        if (!standalone) {
            throw DamengParseTree.error(context, "Unsupported compound DDL: a cascading object drop must occupy the complete LogMiner record");
        }
        throw new CascadeDropException(context, operation, DamengParseTree.source(target));
    }

    private static void rejectPrebuiltMaterializedView(DamengSqlParser.StatementContext statement) {
        final var nonTable = statement.nonTableStatement();
        final var view = nonTable == null ? null : nonTable.materializedViewStatement();
        if (view != null && view.materializedViewDefinition() != null && view.materializedViewDefinition().materializedViewPrebuiltClause() != null) {
            throw DamengParseTree.error(view, "Unsupported PREBUILT materialized view: its backing table can be a captured business table");
        }
    }

    private Analysis noTableChange(String sql, List<DamengSqlToken> tokens, String defaultSchema) {
        return new Analysis(sql, tokens, Impact.NO_TABLE_CHANGE, null, null, null, null, identifiers.canonicalName(defaultSchema));
    }

    private Analysis unsupported(String sql, List<DamengSqlToken> tokens, String defaultSchema, IllegalArgumentException failure) {
        return new Analysis(sql, tokens, Impact.UNSUPPORTED, null, null, failure, null, identifiers.canonicalName(defaultSchema));
    }

    private static DamengSqlParser.QualifiedNameContext target(DamengSqlParser.TableStatementContext statement) {
        if (statement.createTable() != null) {
            return statement.createTable().target;
        }
        if (statement.createExternalTable() != null) {
            return statement.createExternalTable().target;
        }
        if (statement.createHugeTable() != null) {
            return statement.createHugeTable().target;
        }
        if (statement.alterTable() != null) {
            return statement.alterTable().target;
        }
        if (statement.dropTable() != null) {
            return statement.dropTable().target;
        }
        if (statement.truncateTable() != null) {
            return statement.truncateTable().target;
        }
        return statement.renameTable().target;
    }

    private TableId tableId(DamengSqlParser.QualifiedNameContext name, String defaultSchema) {
        final var parts = name.identifier();
        final var first = DamengParseTree.identifier(parts.get(0), identifiers);
        return parts.size() == 1 ? new TableId(null, identifiers.canonicalName(defaultSchema), first)
                : new TableId(null, first, DamengParseTree.identifier(parts.get(1), identifiers));
    }
}
