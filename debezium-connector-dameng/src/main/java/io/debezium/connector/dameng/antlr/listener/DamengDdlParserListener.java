/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng.antlr.listener;

import static io.debezium.connector.dameng.antlr.DamengParseTree.error;

import java.util.List;
import java.util.Objects;

import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.tree.ParseTreeWalker;

import io.debezium.connector.dameng.antlr.DamengDdlParser.SchemaChange;
import io.debezium.connector.dameng.antlr.DamengDdlStatementAnalyzer.Analysis;
import io.debezium.connector.dameng.compatibility.DamengIdentifierRules;
import io.debezium.ddl.parser.dameng.generated.DamengSqlParser;
import io.debezium.ddl.parser.dameng.generated.DamengSqlParserBaseListener;
import io.debezium.relational.Tables;

/** Dispatches one analyzed table statement to its historical-model listeners. */
public final class DamengDdlParserListener extends DamengSqlParserBaseListener {
    final Analysis analysis;
    final Tables tables;
    final ColumnDefinitionParser columns;
    final DamengIdentifierRules identifierRules;

    private final List<DamengSqlParserBaseListener> listeners;
    private SchemaChange change;

    public DamengDdlParserListener(Analysis analysis, Tables tables, ColumnDefinitionParser columns, DamengIdentifierRules identifierRules) {
        this.analysis = analysis;
        this.tables = tables;
        this.columns = columns;
        this.identifierRules = identifierRules;
        this.listeners = List.of(new CreateTableParserListener(this), new AlterTableParserListener(this), new TableLifecycleParserListener(this));
    }

    public SchemaChange walk() {
        ParseTreeWalker.DEFAULT.walk(this, analysis.tableStatement());
        return Objects.requireNonNull(change, "A table statement must produce a historical schema change");
    }

    @Override
    public void enterTableStatement(DamengSqlParser.TableStatementContext statement) {
        if (statement.createHugeTable() != null) {
            throw error(statement, "Captured HUGE tables do not have verified complete LogMiner row images");
        }
    }

    @Override
    public void enterEveryRule(ParserRuleContext context) {
        listeners.forEach(context::enterRule);
    }

    @Override
    public void exitEveryRule(ParserRuleContext context) {
        listeners.forEach(context::exitRule);
    }

    void recordChange(SchemaChange change) {
        if (this.change != null) {
            throw new IllegalStateException("One table statement must produce exactly one historical schema change");
        }
        this.change = change;
    }
}
