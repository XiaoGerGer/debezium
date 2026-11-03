/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng.antlr.listener;

import static io.debezium.connector.dameng.antlr.DamengDdlStatementAnalyzer.rejectAdvancedLog;
import static io.debezium.connector.dameng.antlr.DamengParseTree.error;
import static io.debezium.connector.dameng.antlr.listener.ColumnDefinitionParser.requireColumn;

import java.util.List;

import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.antlr.v4.runtime.tree.Trees;

import io.debezium.connector.dameng.DamengTableType;
import io.debezium.connector.dameng.antlr.DamengDdlParser.SchemaChange;
import io.debezium.connector.dameng.antlr.DamengDdlParser.Type;
import io.debezium.ddl.parser.dameng.generated.DamengSqlLexer;
import io.debezium.ddl.parser.dameng.generated.DamengSqlParser;
import io.debezium.relational.Column;
import io.debezium.relational.ColumnEditor;
import io.debezium.relational.Table;
import io.debezium.relational.TableEditor;

/** Creates historical table definitions from parsed CREATE TABLE statements. */
final class CreateTableParserListener extends BaseParserListener {
    CreateTableParserListener(DamengDdlParserListener listener) {
        super(listener);
    }

    @Override
    public void exitCreateExternalTable(DamengSqlParser.CreateExternalTableContext statement) {
        final var id = analysis.tableId();
        if (tables.forTable(id) != null) {
            throw error(statement, "CREATE EXTERNAL TABLE refers to an existing table " + id);
        }
        final var table = DamengTableType.externalTable(id);
        tables.overwriteTable(table);
        listener.recordChange(new SchemaChange(Type.CREATE, table, id, null));
    }

    @Override
    public void exitCreateTable(DamengSqlParser.CreateTableContext statement) {
        rejectAdvancedLog(analysis);
        final var id = analysis.tableId();
        if (statement.TEMPORARY() != null) {
            if (tables.forTable(id) != null) {
                throw error(statement, "CREATE TEMPORARY TABLE refers to an existing table " + id);
            }
            final var table = DamengTableType.temporaryTable(id);
            tables.overwriteTable(table);
            listener.recordChange(new SchemaChange(Type.CREATE, table, id, null));
            return;
        }
        if (id.table().startsWith("#")) {
            throw error(statement, "Captured temporary tables do not have complete LogMiner row images");
        }
        if (tables.forTable(id) != null) {
            throw error(statement, "CREATE TABLE refers to an existing table " + id);
        }
        final var editor = Table.editor().tableId(id);
        if (statement.LIKE() != null) {
            copyColumns(editor, requireTable(tableId(statement.qualifiedName(1), analysis.defaultSchema())));
        }
        else if (statement.selectStatement() != null) {
            if (statement.tableDefinition() != null) {
                throw error(statement, "Unsupported CTAS explicit column definitions");
            }
            selectColumns(editor, statement.selectStatement());
            tableOptions(statement.tableOption(), editor);
        }
        else {
            columns.addDefinitions(editor, statement.tableDefinition());
            tableOptions(statement.tableOption(), editor);
        }
        final var table = editor.create();
        tables.overwriteTable(table);
        listener.recordChange(new SchemaChange(Type.CREATE, table, id, null));
    }

    private void tableOptions(List<DamengSqlParser.TableOptionContext> options, TableEditor table) {
        for (var option : options) {
            if (option.ON() != null) {
                throw error(option, "Unsupported ON COMMIT table policy for captured tables");
            }
            if (option.NUMBER() != null) {
                integer(option.NUMBER());
            }
            if (option.storageClause() != null) {
                validateIntegers(option.storageClause());
            }
            if (option.parallelClause() != null) {
                validateIntegers(option.parallelClause());
            }
            if (option.COMPRESS() != null && option.identifierList() != null) {
                option.identifierList().identifier().forEach(name -> requireColumn(table, columns.identifier(name)));
            }
            final var partition = option.partitionClause();
            if (partition != null) {
                partition.identifierList().identifier().forEach(name -> requireColumn(table, columns.identifier(name)));
                if (partition.PARTITIONS() != null && integer(partition.NUMBER()) <= 0) {
                    throw error(partition, "Partition count must be positive");
                }
                for (var storage : Trees.findAllRuleNodes(partition, DamengSqlParser.RULE_storageClause)) {
                    validateIntegers((ParserRuleContext) storage);
                }
                for (var node : Trees.findAllRuleNodes(partition, DamengSqlParser.RULE_subpartitionClause)) {
                    final var subpartition = (DamengSqlParser.SubpartitionClauseContext) node;
                    subpartition.identifierList().identifier().forEach(name -> requireColumn(table, columns.identifier(name)));
                    if (subpartition.NUMBER() != null && integer(subpartition.NUMBER()) <= 0) {
                        throw error(subpartition, "Subpartition count must be positive");
                    }
                }
                for (var node : Trees.findAllRuleNodes(partition, DamengSqlParser.RULE_subpartitionDetails)) {
                    final var subpartition = (DamengSqlParser.SubpartitionDetailsContext) node;
                    if (subpartition.NUMBER() != null && integer(subpartition.NUMBER()) <= 0) {
                        throw error(subpartition, "Subpartition count must be positive");
                    }
                }
            }
        }
    }

    private static void validateIntegers(ParserRuleContext context) {
        for (var node : Trees.findAllTokenNodes(context, DamengSqlLexer.NUMBER)) {
            integer((TerminalNode) node);
        }
    }

    private static int integer(TerminalNode node) {
        try {
            return Integer.parseInt(node.getText());
        }
        catch (NumberFormatException failure) {
            throw error((ParserRuleContext) node.getParent(), "Expected SQL integer");
        }
    }

    private void selectColumns(TableEditor target, DamengSqlParser.SelectStatementContext select) {
        final var queries = select.queryExpression().selectQuery();
        if (!select.commonTableExpression().isEmpty() || select.orderByClause() != null || queries.size() != 1) {
            throw error(select, "Unsupported CTAS query scope; only a single historical source table is supported");
        }
        final var query = queries.get(0);
        if (query.tableReference().size() != 1 || query.GROUP() != null || query.HAVING() != null || query.DISTINCT() != null || query.ALL() != null) {
            throw error(query, "Unsupported CTAS query scope; only a single historical source table is supported");
        }
        final var reference = query.tableReference(0);
        final var primary = reference.tablePrimary();
        if (!reference.joinClause().isEmpty() || primary.qualifiedName() == null
                || query.expression().stream().anyMatch(CreateTableParserListener::hasSubquery)) {
            throw error(query, "Unsupported CTAS query scope; only a single historical source table is supported");
        }
        final var source = requireTable(tableId(primary.qualifiedName(), analysis.defaultSchema()));
        if (DamengTableType.isNonRow(source)) {
            throw error(primary, "Cannot derive a captured CTAS schema from an external or temporary table " + source.id());
        }
        final var alias = primary.regularIdentifier() == null ? source.id().table() : columns.identifier(primary.regularIdentifier());
        for (var item : query.selectItem()) {
            if (item.ASTERISK() != null) {
                if (item.expressionIdentifier() != null && !columns.identifier(item.expressionIdentifier()).equals(alias)) {
                    throw error(item, "Unknown CTAS source qualifier " + item.expressionIdentifier().getText());
                }
                copyColumns(target, source);
                continue;
            }
            final var expression = projectedExpression(item.expression());
            final ColumnEditor column;
            if (expression.CAST() != null) {
                if (item.AS() == null) {
                    throw error(item, "CAST projection requires an explicit output name");
                }
                column = copiedColumn(projectedColumn(projectedExpression(expression.expression(0)), source, alias)).optional(true);
                columns.dataType(column, expression.dataType());
            }
            else {
                column = copiedColumn(projectedColumn(expression, source, alias));
            }
            if (item.identifier() != null) {
                column.name(columns.identifier(item.identifier()));
            }
            else if (item.regularIdentifier() != null) {
                column.name(columns.identifier(item.regularIdentifier()));
            }
            addCopiedColumn(target, column, item);
        }
    }

    private static boolean hasSubquery(ParserRuleContext context) {
        return context != null && !Trees.findAllRuleNodes(context, DamengSqlParser.RULE_selectStatement).isEmpty();
    }

    private static DamengSqlParser.PrimaryExpressionContext projectedExpression(DamengSqlParser.ExpressionContext expression) {
        ParserRuleContext current = expression;
        while (!(current instanceof DamengSqlParser.PrimaryExpressionContext) && current.getChildCount() == 1
                && current.getChild(0) instanceof ParserRuleContext child) {
            current = child;
        }
        if (!(current instanceof DamengSqlParser.PrimaryExpressionContext primary)) {
            throw error(expression, "Unsupported CTAS projection expression");
        }
        return primary;
    }

    private Column projectedColumn(DamengSqlParser.PrimaryExpressionContext expression, Table source, String alias) {
        final var name = expression.expressionName();
        if (name == null || expression.LPAREN() != null || name.identifier().size() > 1) {
            throw error(expression, "Unsupported CTAS projection; a historical column is required");
        }
        final String columnName;
        if (name.identifier().isEmpty()) {
            columnName = columns.identifier(name.expressionIdentifier());
        }
        else {
            if (!columns.identifier(name.expressionIdentifier()).equals(alias)) {
                throw error(name, "Unknown CTAS source qualifier " + name.expressionIdentifier().getText());
            }
            columnName = columns.identifier(name.identifier(0));
        }
        final var column = source.columnWithName(columnName);
        if (column == null) {
            throw error(name, "Unknown historical CTAS source column " + columnName);
        }
        return column;
    }

    private static ColumnEditor copiedColumn(Column source) {
        return source.edit().unsetDefaultValueExpression().autoIncremented(false).generated(false).comment(null);
    }

    private static void copyColumns(TableEditor target, Table source) {
        if (DamengTableType.isNonRow(source)) {
            throw new IllegalArgumentException("Cannot copy a captured schema from an external or temporary table " + source.id());
        }
        for (var column : source.columns()) {
            addCopiedColumn(target, copiedColumn(column), null);
        }
    }

    private static void addCopiedColumn(TableEditor target, ColumnEditor column, ParserRuleContext context) {
        if (target.columnWithName(column.name()) != null) {
            final var message = "Duplicate CTAS output column " + column.name();
            throw context == null ? new IllegalArgumentException(message) : error(context, message);
        }
        target.addColumn(column.create());
    }
}
