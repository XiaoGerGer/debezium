/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng.antlr;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import io.debezium.connector.dameng.antlr.DamengDdlStatementAnalyzer.Analysis;
import io.debezium.connector.dameng.antlr.DamengDdlStatementAnalyzer.Impact;
import io.debezium.connector.dameng.antlr.listener.ColumnDefinitionParser;
import io.debezium.connector.dameng.antlr.listener.DamengDdlParserListener;
import io.debezium.connector.dameng.compatibility.DamengIdentifierRules;
import io.debezium.connector.dameng.compatibility.DamengTypeRules;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;

/** Coordinates complete-input analysis and atomic historical schema changes. */
public final class DamengDdlParser {
    public static final String CONSTRAINT_PREFIX = ColumnDefinitionParser.CONSTRAINT_PREFIX;

    private final DamengDdlStatementAnalyzer analyzer;
    private final ColumnDefinitionParser columns;
    private final DamengIdentifierRules identifierRules;

    public DamengDdlParser() {
        this(StandardCharsets.UTF_8, true, false, false);
    }

    public DamengDdlParser(Charset databaseCharset, boolean characterSemanticsEnabled, boolean defaultCharacterLength, boolean nationalCharacterLength) {
        this(databaseCharset, characterSemanticsEnabled, defaultCharacterLength, nationalCharacterLength, false);
    }

    public DamengDdlParser(Charset databaseCharset, boolean characterSemanticsEnabled, boolean defaultCharacterLength, boolean nationalCharacterLength,
                           boolean nestedComments) {
        this(databaseCharset, characterSemanticsEnabled, defaultCharacterLength, nationalCharacterLength, nestedComments, new DamengIdentifierRules(false));
    }

    public DamengDdlParser(Charset databaseCharset, boolean characterSemanticsEnabled, boolean defaultCharacterLength, boolean nationalCharacterLength,
                           boolean nestedComments, DamengIdentifierRules identifierRules) {
        this(databaseCharset, characterSemanticsEnabled, defaultCharacterLength, nationalCharacterLength, nestedComments, identifierRules, DamengTypeRules.NATIVE);
    }

    public DamengDdlParser(Charset databaseCharset, boolean characterSemanticsEnabled, boolean defaultCharacterLength, boolean nationalCharacterLength,
                           boolean nestedComments, DamengIdentifierRules identifierRules, DamengTypeRules typeRules) {
        this.identifierRules = identifierRules;
        this.analyzer = new DamengDdlStatementAnalyzer(nestedComments, identifierRules);
        this.columns = new ColumnDefinitionParser(databaseCharset, characterSemanticsEnabled, defaultCharacterLength,
                nationalCharacterLength, identifierRules, typeRules);
    }

    public enum Type {
        CREATE,
        ALTER,
        DROP,
        TRUNCATE,
        RENAME
    }

    public record SchemaChange(Type type, Table table, TableId tableId, TableId previousTableId) {
    }

    /** Analyze one complete LogMiner record before filtering or changing its historical model. */
    public Analysis analyze(String ddl, String defaultSchema) {
        return analyzer.analyze(ddl, defaultSchema).requireSupported();
    }

    public TableId resolveTableId(String ddl, String defaultSchema) {
        return analyze(ddl, defaultSchema).tableId();
    }

    public TableId resolveRenamedTableId(String ddl, String defaultSchema) {
        return analyze(ddl, defaultSchema).renamedTableId();
    }

    public boolean isNonTableDdl(String ddl) {
        return analyzer.analyze(ddl, null).impact() == Impact.NO_TABLE_CHANGE;
    }

    /** Computes a single historical change without modifying the live model. */
    public List<SchemaChange> plan(Analysis analysis, Tables tables) {
        analysis.requireSupported();
        if (analysis.impact() == Impact.NO_TABLE_CHANGE) {
            return List.of();
        }
        final var parsed = analysis.tableStatement() == null
                ? analyzer.analyzeTokens(analysis.sql(), analysis.tokens(), analysis.defaultSchema()).requireSupported()
                : analysis;
        return List.of(walk(parsed, tables.clone()));
    }

    public List<SchemaChange> parse(String ddl, Tables tables, String defaultSchema) {
        Objects.requireNonNull(ddl, "ddl");
        Objects.requireNonNull(tables, "tables");
        final var statements = analyzer.analyzeScript(ddl, defaultSchema);
        final var working = tables.clone();
        final List<SchemaChange> changes = new ArrayList<>();
        for (var statement : statements) {
            statement.requireSupported();
            if (statement.impact() == Impact.TABLE) {
                changes.add(walk(statement, working));
            }
        }
        // Apply only after every statement has passed syntax and model validation.
        for (var change : changes) {
            if (change.previousTableId() != null) {
                tables.removeTable(change.previousTableId());
            }
            if (change.type() == Type.DROP) {
                tables.removeTable(change.tableId());
            }
            else {
                tables.overwriteTable(change.table());
            }
        }
        return changes;
    }

    private SchemaChange walk(Analysis analysis, Tables tables) {
        return new DamengDdlParserListener(analysis, tables, columns, identifierRules).walk();
    }
}
