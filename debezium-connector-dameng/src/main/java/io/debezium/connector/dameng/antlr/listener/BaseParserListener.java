/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng.antlr.listener;

import io.debezium.connector.dameng.antlr.DamengDdlParser.SchemaChange;
import io.debezium.connector.dameng.antlr.DamengDdlParser.Type;
import io.debezium.connector.dameng.antlr.DamengDdlStatementAnalyzer.Analysis;
import io.debezium.connector.dameng.compatibility.DamengIdentifierRules;
import io.debezium.ddl.parser.dameng.generated.DamengSqlParser;
import io.debezium.ddl.parser.dameng.generated.DamengSqlParserBaseListener;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;

/** Shared historical table lookup and identifier handling for statement listeners. */
abstract class BaseParserListener extends DamengSqlParserBaseListener {
    final DamengDdlParserListener listener;
    final Analysis analysis;
    final Tables tables;
    final ColumnDefinitionParser columns;
    final DamengIdentifierRules identifierRules;

    BaseParserListener(DamengDdlParserListener listener) {
        this.listener = listener;
        this.analysis = listener.analysis;
        this.tables = listener.tables;
        this.columns = listener.columns;
        this.identifierRules = listener.identifierRules;
    }

    final Table requireTable(TableId id) {
        final var table = tables.forTable(id);
        if (table == null) {
            throw new IllegalArgumentException("DDL refers to unknown table " + id);
        }
        return table;
    }

    final TableId tableId(DamengSqlParser.QualifiedNameContext context, String defaultSchema) {
        final var names = context.identifier();
        return names.size() == 1 ? new TableId(null, identifierRules.canonicalName(defaultSchema), columns.identifier(names.get(0)))
                : new TableId(null, columns.identifier(names.get(0)), columns.identifier(names.get(1)));
    }

    final void rename(TableId from, TableId to) {
        final var original = requireTable(from);
        if (tables.forTable(to) != null) {
            throw new IllegalArgumentException("Cannot rename table to existing table " + to);
        }
        final var renamed = original.edit().tableId(to).addAttributes(original.attributes()).create();
        tables.removeTable(from);
        tables.overwriteTable(renamed);
        listener.recordChange(new SchemaChange(Type.RENAME, renamed, to, from));
    }
}
