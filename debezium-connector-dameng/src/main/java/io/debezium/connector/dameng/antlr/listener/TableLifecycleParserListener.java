/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng.antlr.listener;

import static io.debezium.connector.dameng.antlr.DamengParseTree.error;

import io.debezium.connector.dameng.DamengTableType;
import io.debezium.connector.dameng.antlr.DamengDdlParser.SchemaChange;
import io.debezium.connector.dameng.antlr.DamengDdlParser.Type;
import io.debezium.ddl.parser.dameng.generated.DamengSqlParser;

/** Applies drop, truncate and rename operations without changing column definitions. */
final class TableLifecycleParserListener extends BaseParserListener {
    TableLifecycleParserListener(DamengDdlParserListener listener) {
        super(listener);
    }

    @Override
    public void exitDropTable(DamengSqlParser.DropTableContext statement) {
        final var id = analysis.tableId();
        final var original = requireTable(id);
        tables.removeTable(id);
        listener.recordChange(new SchemaChange(Type.DROP, original, id, null));
    }

    @Override
    public void exitTruncateTable(DamengSqlParser.TruncateTableContext statement) {
        final var id = analysis.tableId();
        final var original = requireTable(id);
        if (DamengTableType.isExternal(original)) {
            throw error(statement, "External tables cannot be truncated");
        }
        if (statement.PARTITION() != null || statement.LPAREN() != null || statement.CASCADE() != null) {
            throw error(statement, "Unsupported table DDL: partition or cascading TRUNCATE");
        }
        listener.recordChange(new SchemaChange(Type.TRUNCATE, original, id, null));
    }

    @Override
    public void exitRenameTable(DamengSqlParser.RenameTableContext statement) {
        rename(analysis.tableId(), analysis.renamedTableId());
    }
}
