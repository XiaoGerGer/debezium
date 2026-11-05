/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.dameng.antlr.listener;

import java.nio.charset.Charset;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import org.antlr.v4.runtime.ParserRuleContext;

import io.debezium.connector.dameng.DamengInterval;
import io.debezium.connector.dameng.antlr.DamengParseTree;
import io.debezium.connector.dameng.compatibility.DamengIdentifierRules;
import io.debezium.ddl.parser.dameng.generated.DamengSqlParser.ColumnAttributeContext;
import io.debezium.ddl.parser.dameng.generated.DamengSqlParser.ColumnConstraintContext;
import io.debezium.ddl.parser.dameng.generated.DamengSqlParser.ColumnDefinitionContext;
import io.debezium.ddl.parser.dameng.generated.DamengSqlParser.DataTypeContext;
import io.debezium.ddl.parser.dameng.generated.DamengSqlParser.IdentifierContext;
import io.debezium.ddl.parser.dameng.generated.DamengSqlParser.IdentifierListContext;
import io.debezium.ddl.parser.dameng.generated.DamengSqlParser.ModifyColumnDefinitionContext;
import io.debezium.ddl.parser.dameng.generated.DamengSqlParser.ReferencesClauseContext;
import io.debezium.ddl.parser.dameng.generated.DamengSqlParser.TableConstraintContext;
import io.debezium.ddl.parser.dameng.generated.DamengSqlParser.TableConstraintDefinitionContext;
import io.debezium.ddl.parser.dameng.generated.DamengSqlParser.TableDefinitionContext;
import io.debezium.ddl.parser.dameng.generated.DamengSqlParser.TableElementContext;
import io.debezium.ddl.parser.dameng.generated.DamengSqlParser.UniqueConstraintKeyContext;
import io.debezium.relational.Attribute;
import io.debezium.relational.Column;
import io.debezium.relational.ColumnEditor;
import io.debezium.relational.TableEditor;

/** Maps parsed column and constraint definitions to the historical table model. */
public final class ColumnDefinitionParser {
    public static final String CONSTRAINT_PREFIX = "dameng.constraint.";

    private final Charset databaseCharset;
    private final boolean characterSemanticsEnabled;
    private final boolean defaultCharacterLength;
    private final boolean nationalCharacterLength;
    private final DamengIdentifierRules identifierRules;

    public ColumnDefinitionParser(Charset databaseCharset, boolean characterSemanticsEnabled,
                                  boolean defaultCharacterLength, boolean nationalCharacterLength,
                                  DamengIdentifierRules identifierRules) {
        this.databaseCharset = databaseCharset;
        this.characterSemanticsEnabled = characterSemanticsEnabled;
        this.defaultCharacterLength = defaultCharacterLength;
        this.nationalCharacterLength = nationalCharacterLength;
        this.identifierRules = identifierRules;
    }

    public void addDefinitions(TableEditor table, TableDefinitionContext definition) {
        addDefinitions(table, definition.tableElement());
    }

    public void addDefinitions(TableEditor table, List<TableElementContext> definitions) {
        addDefinitions(table, definitions, false);
    }

    public void addDefinitions(TableEditor table, List<TableElementContext> definitions, boolean ifNotExists) {
        final List<TableConstraintContext> constraints = new ArrayList<>();
        for (var definition : definitions) {
            if (definition.tableConstraint() == null) {
                final var column = definition.columnDefinition();
                if (!ifNotExists || table.columnWithName(identifier(column.identifier())) == null) {
                    addColumn(table, column);
                }
            }
            else {
                constraints.add(definition.tableConstraint());
            }
        }
        // A table constraint may precede the columns that it references.
        for (var constraint : constraints) {
            addConstraint(table, constraint);
        }
    }

    public void addColumn(TableEditor table, ColumnDefinitionContext definition) {
        column(table, definition, definition.identifier(), definition.dataType(), definition.columnAttribute(), false, definition.AS() != null);
    }

    public void modifyColumn(TableEditor table, ModifyColumnDefinitionContext definition) {
        column(table, definition, definition.identifier(), definition.dataType(), definition.columnAttribute(), true, false);
    }

    private void column(TableEditor table, ParserRuleContext definition, IdentifierContext identifier, DataTypeContext type,
                        List<ColumnAttributeContext> attributes, boolean modifying, boolean generated) {
        final var name = identifier(identifier);
        final var existing = table.columnWithName(name);
        if (modifying && existing == null) {
            throw error(definition, "Cannot modify unknown column " + name);
        }
        if (!modifying && existing != null) {
            throw error(definition, "Column already exists: " + name);
        }
        if (generated) {
            throw generatedColumn(definition);
        }
        final var column = existing == null ? Column.editor().name(name).optional(true) : existing.edit();
        if (type == null && !modifying) {
            throw error(definition, "Column type is required");
        }
        if (type != null) {
            dataType(column, type);
        }
        boolean primaryKey = false;
        for (var attribute : attributes) {
            if (attribute.DEFAULT() != null) {
                if (attribute.ON() != null) {
                    column.optional(false);
                }
                column.defaultValueExpression(defaultExpression(attribute.expression()));
            }
            else if (attribute.COMMENT() != null) {
                final var tokens = DamengParseTree.tokens(attribute);
                column.comment(tokens.get(tokens.size() - 1).stringValue());
            }
            else if (attribute.AS() != null) {
                throw generatedColumn(attribute);
            }
            else if (attribute.IDENTITY() != null || attribute.AUTO_INCREMENT() != null) {
                for (var number : attribute.signedNumber()) {
                    integer(number);
                }
                column.autoIncremented(true);
            }
            else if (attribute.columnConstraint() != null) {
                primaryKey |= columnConstraint(table, column, attribute.columnConstraint());
            }
            else if (attribute.ENCRYPT() != null) {
                // Bare ENCRYPT uses transparent storage encryption without changing the logical column definition.
            }
            else {
                throw error(attribute, "Unsupported column clause");
            }
        }
        table.addColumn(column.create());
        if (primaryKey) {
            setPrimaryKey(table, List.of(name));
        }
    }

    private boolean columnConstraint(TableEditor table, ColumnEditor column, ColumnConstraintContext constraint) {
        if (constraint.DISABLE() != null || constraint.VISIBLE() != null) {
            throw error(constraint, "Unsupported column clause");
        }
        final var name = constraint.CONSTRAINT() == null ? null : identifier(constraint.identifier());
        if (constraint.NULL() != null) {
            column.optional(constraint.NOT() == null);
        }
        else if (constraint.uniqueConstraint() != null) {
            final var unique = constraint.uniqueConstraint();
            final var kind = uniqueKind(unique.uniqueConstraintKey());
            recordConstraint(table, name, kind);
            if ("PRIMARY".equals(kind)) {
                column.optional(false);
                return true;
            }
        }
        else if (constraint.referencesClause() != null) {
            references(constraint.referencesClause());
            recordConstraint(table, name, "FOREIGN");
        }
        else if (constraint.CHECK() != null) {
            expression(constraint.expression());
            recordConstraint(table, name, "CHECK");
        }
        else {
            throw error(constraint, "Unsupported column clause");
        }
        return false;
    }

    public void addConstraint(TableEditor table, TableConstraintContext constraint) {
        addConstraint(table, constraint.tableConstraintDefinition(), constraint.identifier() == null ? null : identifier(constraint.identifier()));
    }

    public void addConstraint(TableEditor table, TableConstraintDefinitionContext constraint, String name) {
        if (constraint.DISABLE() != null) {
            throw error(constraint, "Unsupported disabled table constraint");
        }
        final String kind;
        if (constraint.uniqueConstraintKey() != null) {
            kind = uniqueKind(constraint.uniqueConstraintKey());
            final var names = identifiers(constraint.identifierList());
            if ("PRIMARY".equals(kind)) {
                setPrimaryKey(table, names);
            }
            else {
                names.forEach(column -> requireColumn(table, column));
            }
        }
        else if (constraint.FOREIGN() != null) {
            identifiers(constraint.identifierList()).forEach(column -> requireColumn(table, column));
            references(constraint.referencesClause());
            kind = "FOREIGN";
        }
        else if (constraint.CHECK() != null) {
            expression(constraint.expression());
            kind = "CHECK";
        }
        else {
            throw error(constraint, "Unsupported table constraint");
        }
        recordConstraint(table, name, kind);
    }

    private static String uniqueKind(UniqueConstraintKeyContext key) {
        // CLUSTER KEY alone defines a non-unique physical index, not a constraint.
        return key.PRIMARY() != null ? "PRIMARY" : key.UNIQUE() != null ? "UNIQUE" : null;
    }

    private void references(ReferencesClauseContext references) {
        references.qualifiedName().identifier().forEach(this::identifier);
        if (references.identifierList() != null) {
            identifiers(references.identifierList());
        }
        if (references.referenceAction().stream().filter(action -> action.DELETE() != null).count() > 1
                || references.referenceAction().stream().filter(action -> action.UPDATE() != null).count() > 1) {
            throw error(references, "Duplicate reference action");
        }
    }

    private static void recordConstraint(TableEditor table, String name, String kind) {
        if (name != null && kind != null) {
            table.addAttribute(Attribute.editor().name(CONSTRAINT_PREFIX + name).value(kind).create());
        }
    }

    public static void setPrimaryKey(TableEditor table, List<String> names) {
        if (!table.primaryKeyColumnNames().isEmpty()) {
            throw new IllegalArgumentException("Multiple primary keys for " + table.tableId());
        }
        for (var name : names) {
            table.updateColumn(requireColumn(table, name).edit().optional(false).create());
        }
        table.setPrimaryKeyNames(names);
    }

    public void dataType(ColumnEditor column, DataTypeContext type) {
        String name = type.getStart().getText().toUpperCase(Locale.ROOT);
        if (type.INTERVAL() != null) {
            final var declaration = expression(type);
            final var interval = DamengInterval.type(declaration);
            column.jdbcType(Types.OTHER).type(interval.typeName(), declaration)
                    .length(interval.leadingPrecision()).scale(interval.fractionalPrecision());
            return;
        }
        if (type.typeName() != null) {
            if (type.typeName().DOT() != null) {
                throw error(type, "Unsupported DM column type " + expression(type));
            }
            name = identifier(type.typeName().regularIdentifier()).toUpperCase(Locale.ROOT);
        }
        if (name.equals("CHARACTER")) {
            name = type.VARYING() == null ? "CHAR" : "VARCHAR";
        }
        else if (type.VARYING() != null) {
            throw error(type, "Unsupported DM column type " + expression(type));
        }
        int length = -1;
        Integer scale = null;
        boolean characterLength = defaultCharacterLength || name.equals("NCHAR") && nationalCharacterLength;
        if (!type.typePrecision().isEmpty()) {
            final var precision = type.typePrecision(0);
            length = integer(precision.NUMBER().getText(), precision);
            if (precision.signedNumber() != null) {
                scale = integer(precision.signedNumber());
            }
            if (precision.CHAR() != null) {
                characterLength = true;
            }
            else if (precision.BYTE() != null) {
                characterLength = false;
            }
        }
        boolean timeZone = false;
        boolean localTimeZone = false;
        if (name.equals("TIMESTAMP") || name.equals("DATETIME") || name.equals("TIME") || name.equals("DATETIME2")) {
            timeZone = type.WITH() != null;
            localTimeZone = type.LOCAL() != null;
            if (localTimeZone && !name.equals("TIMESTAMP")) {
                throw error(type, "LOCAL TIME ZONE requires TIMESTAMP");
            }
            if (scale != null) {
                throw error(type, "Temporal types accept one precision parameter");
            }
            scale = length < 0 ? (name.equals("TIME") ? 0 : name.equals("DATETIME2") ? 9 : 6) : length;
            length = (name.equals("TIME") ? 15 : 29) + (scale == 0 ? 0 : scale + 1) + (timeZone ? 7 : 0);
        }
        final int jdbcType = switch (name) {
            case "INT", "INTEGER", "PLS_INTEGER" -> Types.INTEGER;
            case "BIGINT" -> Types.BIGINT;
            case "SMALLINT" -> Types.SMALLINT;
            case "TINYINT", "BYTE" -> Types.TINYINT;
            case "NUMERIC", "NUMBER", "DEC", "DECIMAL" -> Types.DECIMAL;
            case "FLOAT" -> length > 0 && length <= 24 ? Types.REAL : Types.FLOAT;
            case "DOUBLE" -> Types.DOUBLE;
            case "REAL" -> Types.REAL;
            case "BIT" -> Types.BIT;
            case "BOOLEAN" -> Types.BOOLEAN;
            case "CHAR", "NCHAR" -> Types.CHAR;
            case "VARCHAR", "VARCHAR2", "NVARCHAR", "NVARCHAR2" -> Types.VARCHAR;
            case "CLOB", "TEXT", "LONGVARCHAR" -> Types.CLOB;
            case "BLOB", "IMAGE", "LONGVARBINARY" -> Types.BLOB;
            case "BINARY" -> Types.BINARY;
            case "RAW", "VARBINARY" -> Types.VARBINARY;
            case "DATE" -> Types.DATE;
            case "TIME" -> timeZone ? Types.TIME_WITH_TIMEZONE : Types.TIME;
            case "TIMESTAMP", "DATETIME", "DATETIME2" -> timeZone && !localTimeZone ? Types.TIMESTAMP_WITH_TIMEZONE : Types.TIMESTAMP;
            default -> throw error(type, "Unsupported DM column type " + name);
        };
        if (length < 0) {
            length = switch (jdbcType) {
                case Types.INTEGER -> 10;
                case Types.SMALLINT -> 5;
                case Types.TINYINT -> 3;
                case Types.BIGINT -> 19;
                case Types.CHAR, Types.NCHAR, Types.BINARY, Types.BIT, Types.BOOLEAN -> 1;
                case Types.VARCHAR, Types.NVARCHAR, Types.VARBINARY -> 8188;
                case Types.NUMERIC, Types.DECIMAL -> 0;
                case Types.REAL -> 24;
                case Types.FLOAT, Types.DOUBLE -> 53;
                case Types.DATE -> 13;
                case Types.CLOB, Types.BLOB -> Integer.MAX_VALUE;
                default -> -1;
            };
        }
        if (scale == null && Set.of(Types.DECIMAL, Types.NUMERIC, Types.INTEGER, Types.BIGINT, Types.SMALLINT,
                Types.TINYINT, Types.DATE, Types.BIT, Types.BOOLEAN).contains(jdbcType)) {
            scale = 0;
        }
        if (name.equals("RAW")) {
            name = "VARBINARY";
        }
        else if (name.equals("FLOAT") && jdbcType == Types.REAL) {
            name = "REAL";
            length = 24;
        }
        else if (jdbcType == Types.FLOAT || jdbcType == Types.DOUBLE) {
            length = 53;
        }
        if (timeZone && !localTimeZone && !name.equals("TIME")) {
            name = "DATETIME";
        }
        column.jdbcType(jdbcType).type(name + (localTimeZone ? " WITH LOCAL TIME ZONE" : timeZone ? " WITH TIME ZONE" : ""), expression(type))
                .length(length).scale(scale);
        if (jdbcType == Types.CHAR || jdbcType == Types.NCHAR) {
            // Historical character semantics cannot be inferred from the current session.
            column.charsetName(databaseCharset.name()).type(name,
                    name + "(" + length + (characterSemanticsEnabled && characterLength ? " CHAR)" : " BYTE)"));
        }
    }

    public static Column requireColumn(TableEditor table, String name) {
        final var column = table.columnWithName(name);
        if (column == null) {
            throw new IllegalArgumentException("DDL refers to unknown column " + table.tableId() + "." + name);
        }
        return column;
    }

    public String identifier(ParserRuleContext context) {
        return identifierRules.identifier(DamengParseTree.tokens(context).get(0));
    }

    public List<String> identifiers(IdentifierListContext context) {
        return context.identifier().stream().map(this::identifier).toList();
    }

    public static String expression(ParserRuleContext context) {
        return DamengParseTree.source(context);
    }

    public static String defaultExpression(ParserRuleContext context) {
        return DamengParseTree.defaultExpression(context);
    }

    public static int integer(ParserRuleContext context) {
        return integer(context.getText(), context);
    }

    private static int integer(String value, ParserRuleContext context) {
        try {
            return Integer.parseInt(value);
        }
        catch (NumberFormatException failure) {
            throw error(context, "Expected integer");
        }
    }

    private static IllegalArgumentException generatedColumn(ParserRuleContext context) {
        return error(context, "Virtual/generated columns contain NULL rather than computed values in LogMiner redo; capture is unsupported");
    }

    private static IllegalArgumentException error(ParserRuleContext context, String message) {
        return DamengParseTree.error(context, message);
    }
}
