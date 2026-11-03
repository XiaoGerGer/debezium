/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng.antlr.listener;

import static io.debezium.connector.dameng.antlr.DamengDdlStatementAnalyzer.rejectAdvancedLog;
import static io.debezium.connector.dameng.antlr.DamengParseTree.error;
import static io.debezium.connector.dameng.antlr.listener.ColumnDefinitionParser.CONSTRAINT_PREFIX;
import static io.debezium.connector.dameng.antlr.listener.ColumnDefinitionParser.requireColumn;

import java.util.List;

import org.antlr.v4.runtime.ParserRuleContext;

import io.debezium.connector.dameng.DamengTableType;
import io.debezium.connector.dameng.antlr.DamengDdlParser.SchemaChange;
import io.debezium.connector.dameng.antlr.DamengDdlParser.Type;
import io.debezium.connector.dameng.antlr.DamengParseTree;
import io.debezium.ddl.parser.dameng.generated.DamengSqlParser;
import io.debezium.relational.Attribute;
import io.debezium.relational.TableEditor;

/** Applies parsed ALTER TABLE operations to historical table definitions. */
final class AlterTableParserListener extends BaseParserListener {
    AlterTableParserListener(DamengDdlParserListener listener) {
        super(listener);
    }

    @Override
    public void exitAlterTable(DamengSqlParser.AlterTableContext statement) {
        rejectAdvancedLog(analysis);
        final var id = analysis.tableId();
        if (analysis.renamedTableId() != null) {
            rename(id, analysis.renamedTableId());
            return;
        }
        final var clause = statement.alterTableClause();
        final var original = requireTable(id);
        if (DamengTableType.isTemporary(original)) {
            listener.recordChange(new SchemaChange(Type.ALTER, original, id, null));
            return;
        }
        if (DamengTableType.isExternal(original)) {
            throw error(statement, "Unsupported external table ALTER; only table renaming is tracked");
        }
        final var editor = original.edit().addAttributes(original.attributes());
        if (clause.LOGIC() != null) {
            // Logical logging policy does not change the historical row schema.
        }
        else if (clause.ADD() != null) {
            if (clause.tableConstraintDefinition() != null) {
                columns.addConstraint(editor, clause.tableConstraintDefinition(), null);
            }
            else {
                columns.addDefinitions(editor, clause.tableElement(), clause.ifNotExists() != null);
            }
        }
        else if (clause.MODIFY() != null) {
            for (var definition : clause.modifyColumnDefinition()) {
                columns.modifyColumn(editor, definition);
            }
        }
        else if (clause.DROP() != null) {
            if (clause.PRIMARY() != null) {
                dropPrimaryKey(editor, true);
            }
            else if (clause.CONSTRAINT() != null) {
                final var name = columns.identifier(clause.identifier(0));
                final var attribute = editor.attributeWithName(CONSTRAINT_PREFIX + name);
                if (attribute == null) {
                    throw error(clause, "Cannot resolve historical constraint " + name);
                }
                if ("PRIMARY".equals(attribute.value())) {
                    dropPrimaryKey(editor, true);
                }
                editor.removeAttribute(CONSTRAINT_PREFIX + name);
            }
            else {
                final var names = clause.identifierList() == null ? clause.identifier() : clause.identifierList().identifier();
                for (var name : names) {
                    final var canonical = columns.identifier(name);
                    if (clause.ifExists() != null && editor.columnWithName(canonical) == null) {
                        continue;
                    }
                    requireColumn(editor, canonical);
                    if (editor.primaryKeyColumnNames().contains(canonical)) {
                        // DM removes the whole composite key, retaining NOT NULL on surviving columns.
                        dropPrimaryKey(editor, false);
                    }
                    editor.removeColumn(canonical);
                }
            }
        }
        else if (clause.ALTER() != null) {
            alterColumn(editor, clause);
        }
        else if (clause.RENAME() != null && clause.COLUMN() != null) {
            renameColumn(editor, columns.identifier(clause.identifier(0)), columns.identifier(clause.identifier(1)), clause);
        }
        else if (clause.RENAME() != null && clause.CONSTRAINT() != null) {
            final var from = columns.identifier(clause.identifier(0));
            final var to = columns.identifier(clause.identifier(1));
            final var attribute = editor.attributeWithName(CONSTRAINT_PREFIX + from);
            if (attribute == null) {
                throw error(clause, "Cannot resolve historical constraint " + from);
            }
            editor.removeAttribute(CONSTRAINT_PREFIX + from);
            editor.addAttribute(Attribute.editor().name(CONSTRAINT_PREFIX + to).value(attribute.value()).create());
        }
        else {
            throw error(clause, "Unsupported ALTER TABLE operation");
        }
        final var table = editor.create();
        tables.overwriteTable(table);
        listener.recordChange(new SchemaChange(Type.ALTER, table, id, null));
    }

    private void alterColumn(TableEditor table, DamengSqlParser.AlterTableClauseContext statement) {
        final var name = columns.identifier(statement.identifier(0));
        final var operation = statement.alterColumnClause();
        if (operation.RENAME() != null) {
            renameColumn(table, name, columns.identifier(operation.identifier()), operation);
            return;
        }
        final var column = requireColumn(table, name).edit();
        if (operation.DEFAULT() != null) {
            if (operation.SET() != null) {
                column.defaultValueExpression(DamengParseTree.defaultExpression(operation.expression()));
                if (operation.ON() != null) {
                    column.optional(false);
                }
            }
            else {
                column.unsetDefaultValueExpression();
            }
        }
        else if (operation.NULL() != null) {
            column.optional(operation.DROP() != null || operation.NOT() == null);
        }
        else {
            throw error(operation, "Unsupported ALTER COLUMN operation");
        }
        table.updateColumn(column.create());
    }

    private static void dropPrimaryKey(TableEditor table, boolean restoreNullable) {
        if (table.primaryKeyColumnNames().isEmpty()) {
            throw new IllegalArgumentException("Cannot resolve historical primary key for " + table.tableId());
        }
        if (restoreNullable) {
            for (final var name : table.primaryKeyColumnNames()) {
                table.updateColumn(requireColumn(table, name).edit().optional(true).create());
            }
        }
        table.setPrimaryKeyNames(List.of());
        for (final var attribute : table.attributes()) {
            if (attribute.name().startsWith(CONSTRAINT_PREFIX) && "PRIMARY".equals(attribute.value())) {
                table.removeAttribute(attribute.name());
            }
        }
    }

    private static void renameColumn(TableEditor table, String from, String to, ParserRuleContext context) {
        requireColumn(table, from);
        if (table.columnWithName(to) != null) {
            throw error(context, "Column already exists: " + to);
        }
        table.renameColumn(from, to);
    }
}
