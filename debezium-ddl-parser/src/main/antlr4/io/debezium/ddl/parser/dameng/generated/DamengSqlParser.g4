/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
parser grammar DamengSqlParser;

options { tokenVocab = DamengSqlLexer; }

// Complete-input DDL entry. Program bodies own their internal semicolons.
// The analyzer enforces the single-record boundary and table history semantics.
ddlScope
    : SEMICOLON* statement (SEMICOLON+ statement)* SEMICOLON* EOF
    ;

// Convenience/history scripts may contain several definitions. LogMiner records
// still use ddlScope so a program cannot hide a subsequent table change.
ddlScript
    : SEMICOLON* (scriptStatement (SEMICOLON+ scriptStatement)* SEMICOLON*)? EOF
    ;

scriptStatement
    : statement
    ;

statement
    : tableStatement
    | nonTableStatement
    ;

// https://eco.dameng.com/document/dm/zh-cn/pm/definition-statement.html
// Base, external and HUGE tables have separate productions in the DM manual.
tableStatement
    : createTable
    | createExternalTable
    | createHugeTable
    | alterTable
    | dropTable
    | truncateTable
    | renameTable
    ;

createTable
    : CREATE (GLOBAL? TEMPORARY)? TABLE ifNotExists? target=qualifiedName
        (tableDefinition tableOption* (AS selectStatement)?
        | LIKE qualifiedName
        | tableOption* AS selectStatement) autoIncrementStart?
    ;

autoIncrementStart
    : AUTO_INCREMENT EQUALS_OP? NUMBER
    ;

createExternalTable
    : CREATE EXTERNAL TABLE ifNotExists? target=qualifiedName
        LPAREN externalColumnDefinition (COMMA externalColumnDefinition)* RPAREN
        FROM (externalFile | DATAFILE externalFile externalParameters?)
    ;

externalColumnDefinition
    : identifier dataType (COMMENT STRING)?
    ;

externalFile
    : DEFAULT DIRECTORY identifier LOCATION LPAREN STRING (ON identifier)? RPAREN
    ;

externalParameters
    : PARMS LPAREN externalParameter (COMMA externalParameter)* RPAREN
    ;

externalParameter
    : (FIELDS | RECORDS) DELIMITED BY expression
    | ERRORS NUMBER
    | BADFILE STRING
    | LOG STRING
    | NULL_STR STRING
    | SKIP_KEYWORD NUMBER
    | CHARACTER_CODE STRING
    | MISSING FIELD VALUES ARE NULL
    ;

createHugeTable
    : CREATE HUGE TABLE ifNotExists? target=qualifiedName
        (LPAREN hugeTableElement (COMMA hugeTableElement)* RPAREN | AS selectStatement)
        partitionClause? (TABLESPACE identifier)? hugeStorageClause? hugeCompressionClause?
        (LOG (NONE | LAST | ALL))?
    ;

hugeTableElement
    : identifier dataType (DEFAULT (ON NULL)? expression)? hugeColumnConstraint*
        (STORAGE LPAREN STAT NONE RPAREN)? (COMMENT STRING)?
    | (CONSTRAINT identifier)?
        ((PRIMARY KEY | UNIQUE) LPAREN identifierList RPAREN (USING INDEX TABLESPACE identifier)?
        | CHECK LPAREN expression RPAREN)
    ;

hugeColumnConstraint
    : (CONSTRAINT identifier)? (NOT? NULL | PRIMARY KEY | UNIQUE)
        (USING INDEX TABLESPACE identifier)?
    ;

hugeStorageClause
    : STORAGE LPAREN (hugeStorageItem COMMA)* ON identifier RPAREN
    ;

hugeStorageItem
    : SECTION LPAREN NUMBER RPAREN
    | INITIAL NUMBER
    | FILESIZE LPAREN NUMBER RPAREN
    | STAT (NONE | SYNCHRONOUS | ASYNCHRONOUS)? ((ON | EXCEPT) LPAREN identifierList RPAREN)?
    | (WITH | WITHOUT) DELTA
    ;

hugeCompressionClause
    : COMPRESS (LEVEL NUMBER)? (FOR STRING)?
        (LPAREN hugeCompressedColumn (COMMA hugeCompressedColumn)* RPAREN
        | EXCEPT LPAREN identifierList RPAREN)?
    ;

hugeCompressedColumn
    : identifier (LEVEL NUMBER)? (FOR STRING)?
    ;

tableDefinition
    : LPAREN tableElement (COMMA tableElement)* RPAREN
    ;

tableElement
    : tableConstraint
    | columnDefinition
    ;

columnDefinition
    : identifier dataType columnAttribute*
    | identifier (GENERATED ALWAYS)? AS LPAREN expression RPAREN VIRTUAL? VISIBLE?
    ;

modifyColumnDefinition
    : identifier (dataType columnAttribute* | columnAttribute+)
    ;

columnAttribute
    : DEFAULT (ON NULL)? expression
    | IDENTITY (LPAREN signedNumber COMMA signedNumber RPAREN)?
    | AUTO_INCREMENT
    | columnConstraint
    | (GENERATED ALWAYS)? AS LPAREN expression RPAREN VIRTUAL? VISIBLE?
    | COMMENT STRING
    | storageClause
    ;

columnConstraint
    : (CONSTRAINT identifier)?
        (NOT? NULL | uniqueConstraint | referencesClause | CHECK LPAREN expression RPAREN | NOT VISIBLE)
        (ENABLE | DISABLE)?
    ;

uniqueConstraint
    : uniqueConstraintKey
        (USING INDEX TABLESPACE (identifier | DEFAULT))?
    ;

uniqueConstraintKey
    : PRIMARY KEY | NOT? CLUSTER PRIMARY KEY | CLUSTER UNIQUE? KEY | UNIQUE
    ;

tableConstraint
    : (CONSTRAINT identifier)? tableConstraintDefinition
    ;

tableConstraintDefinition
    : (uniqueConstraintKey LPAREN identifierList RPAREN (USING INDEX TABLESPACE (identifier | DEFAULT))? GLOBAL?
        | FOREIGN KEY LPAREN identifierList RPAREN referencesClause
        | CHECK LPAREN expression RPAREN)
        (ENABLE | DISABLE)? (VALIDATE | NOVALIDATE)?
    ;

referencesClause
    : (FOREIGN KEY)? REFERENCES PENDANT? qualifiedName (LPAREN identifierList RPAREN)?
        (MATCH (FULL | PARTIAL | SIMPLE))? referenceAction* (WITH INDEX)?
    ;

referenceAction
    : ON (DELETE | UPDATE) (CASCADE | SET (NULL | DEFAULT) | NO ACTION)
    ;

// Physical clauses are explicit even though the history model retains only
// logical columns and constraints. Unsupported clauses fail before filtering.
tableOption
    : TABLESPACE identifier
    | storageClause
    | DISKSPACE (UNLIMITED | LIMIT NUMBER)
    | ON COMMIT (DELETE | PRESERVE) ROWS
    | SEGMENT CREATION (IMMEDIATE | DEFERRED)
    | COMPRESS (EXCEPT? LPAREN identifierList RPAREN)?
    | parallelClause
    | partitionClause
    | ADD LOGIC LOG
    | WITH ADVANCED LOG
    | rowMovementClause
    ;

storageClause
    : STORAGE LPAREN storageItem (COMMA storageItem)* RPAREN
    ;

storageItem
    : ON identifier
    | (INITIAL | NEXT | MINEXTENTS | FILLFACTOR) NUMBER
    | BRANCH (NUMBER | LPAREN NUMBER COMMA NUMBER RPAREN)
    | NOBRANCH
    | CLUSTERBTR
    | (WITH | WITHOUT) COUNTER
    | DISABLE? USING LONG ROW
    | HASHPARTMAP LPAREN NUMBER RPAREN
    ;

parallelClause
    : PARALLEL NUMBER?
    | NOPARALLEL
    ;

// DM SQL Language Manual 3.5.1.4: row-store partitions, interval and multilevel
// definitions. Physical boundaries are not columns in the captured table model.
partitionClause
    : PARTITION BY HASH LPAREN identifierList RPAREN
        subpartitionClauses?
        (PARTITIONS NUMBER hashPartitionStorage?
        | LPAREN hashPartition (COMMA hashPartition)* RPAREN)
    | PARTITION BY RANGE LPAREN identifierList RPAREN
        (INTERVAL LPAREN expression RPAREN)? subpartitionClauses?
        LPAREN rangePartition (COMMA rangePartition)* RPAREN
    | PARTITION BY LIST LPAREN identifierList RPAREN
        subpartitionClauses?
        LPAREN listPartition (COMMA listPartition)* RPAREN
    ;

hashPartition
    : PARTITION identifier partitionStorage* subpartitionDetails?
    ;

rangePartition
    : PARTITION identifier VALUES (EQU OR)? LESS THAN LPAREN expressionList RPAREN partitionStorage* subpartitionDetails?
    ;

listPartition
    : PARTITION identifier VALUES LPAREN (DEFAULT | expressionList) RPAREN partitionStorage* subpartitionDetails?
    ;

partitionStorage
    : TABLESPACE identifier
    | storageClause
    ;

hashPartitionStorage
    : STORE IN LPAREN identifierList RPAREN
    ;

subpartitionClauses
    : subpartitionClause (COMMA subpartitionClause)*
    ;

subpartitionClause
    : SUBPARTITION BY (RANGE | LIST) LPAREN identifierList RPAREN
        (SUBPARTITION TEMPLATE LPAREN subpartitionDefinition (COMMA subpartitionDefinition)* RPAREN)?
    | SUBPARTITION BY HASH LPAREN identifierList RPAREN
        (SUBPARTITION TEMPLATE (SUBPARTITIONS NUMBER hashPartitionStorage?
            | LPAREN subpartitionDefinition (COMMA subpartitionDefinition)* RPAREN)
        | SUBPARTITIONS NUMBER hashPartitionStorage?)?
    ;

subpartitionDetails
    : LPAREN subpartitionDefinition (COMMA subpartitionDefinition)* RPAREN
    | SUBPARTITIONS NUMBER hashPartitionStorage?
    ;

subpartitionDefinition
    : SUBPARTITION identifier (VALUES ((EQU OR)? LESS THAN LPAREN expressionList RPAREN
        | LPAREN (DEFAULT | expressionList) RPAREN))? partitionStorage* subpartitionDetails?
    ;

rowMovementClause
    : (ENABLE | DISABLE) ROW MOVEMENT
    ;

// The analyzer distinguishes physical maintenance from row-loss operations.
// A complete production is required before any operation can be ignored.
partitionMaintenanceClause
    : ADD PARTITION ifNotExists? (rangePartitionBody | listPartitionBody | hashPartitionBody)
    | MODIFY (PARTITION | SUBPARTITION) identifier ADD SUBPARTITION ifNotExists? (rangePartitionBody | listPartitionBody)
    | MODIFY (PARTITION | SUBPARTITION) identifier (ADD | DROP) VALUES LPAREN expressionList RPAREN
    | partitionDropClause
    | MERGE PARTITIONS (identifier COMMA identifier | NUMBER COMMA NUMBER) INTO PARTITION identifier globalIndexClause?
    | SPLIT PARTITION identifier
        ((AT | VALUES) LPAREN expressionList RPAREN INTO LPAREN splitPartition COMMA splitPartition RPAREN
        | INTO LPAREN (rangePartition COMMA)+ splitPartition RPAREN
        | INTO LPAREN (listPartition COMMA)+ splitPartition RPAREN) globalIndexClause?
    | SET SUBPARTITION TEMPLATE
        (LPAREN (subpartitionDefinition (COMMA subpartitionDefinition)*)? RPAREN
        | SUBPARTITIONS? NUMBER hashPartitionStorage?)
    | rowMovementClause
    | RENAME (PARTITION | SUBPARTITION) identifier TO identifier
    | MOVE (PARTITION | SUBPARTITION) identifier TABLESPACE identifier
    | EXCHANGE (PARTITION | SUBPARTITION) identifier WITH TABLE qualifiedName
    | TRUNCATE (PARTITION | SUBPARTITION)
        (identifier | LPAREN identifier RPAREN | FOR LPAREN expressionList RPAREN)
        ((DROP | REUSE) STORAGE)? globalIndexClause?
    ;

partitionDropClause
    : DROP PARTITION (ifExists? identifier | FOR ifExists? LPAREN expressionList RPAREN) globalIndexClause?
    | DROP SUBPARTITION (ifExists? identifier | FOR ifExists? LPAREN expressionList RPAREN) globalIndexClause?
    ;

rangePartitionBody
    : identifier VALUES (EQU OR)? LESS THAN LPAREN expressionList RPAREN partitionStorage* subpartitionDetails?
    ;

listPartitionBody
    : identifier VALUES LPAREN (DEFAULT | expressionList) RPAREN partitionStorage* subpartitionDetails?
    ;

hashPartitionBody
    : identifier partitionStorage* subpartitionDetails?
    ;

splitPartition
    : PARTITION identifier partitionStorage*
    ;

globalIndexClause
    : (UPDATE | IGNORE) GLOBAL INDEXES
    ;

alterTable
    : ALTER TABLE target=qualifiedName
        (RENAME TO destination=qualifiedName | alterTableClause)
    ;

alterTableClause
    : partitionMaintenanceClause
    | ADD LOGIC LOG
    | DROP LOGIC LOG
    | advancedLogClause
    | ADD COLUMN? ifNotExists? (tableElement | LPAREN tableElement (COMMA tableElement)* RPAREN)
    | ADD CONSTRAINT tableConstraintDefinition
    | MODIFY COLUMN? (modifyColumnDefinition | LPAREN modifyColumnDefinition (COMMA modifyColumnDefinition)* RPAREN)
    | DROP PRIMARY KEY cascadeClause?
    | DROP CONSTRAINT identifier (cascadeClause | CASCADE? dropConstraintIndexClause)?
    | DROP COLUMN? ifExists? (identifier | LPAREN identifierList RPAREN) cascadeClause?
    | ALTER COLUMN? identifier alterColumnClause
    | RENAME (COLUMN | CONSTRAINT) identifier TO identifier
    | (ENABLE | DISABLE) CONSTRAINT identifier (VALIDATE | NOVALIDATE)? cascadeClause?
    | (ENABLE | DISABLE) ALL TRIGGERS
    | (WITH | WITHOUT) COUNTER
    | (ENABLE | DISABLE) USING LONG ROW
    | MOVE TABLESPACE identifier
    | parallelClause
    | READ (WRITE | ONLY)
    ;

dropConstraintIndexClause
    : (DROP | KEEP)? INDEX
    ;

// These forms also affect the implicit $ALOG table. Recognition does not imply
// that the connector can represent those changes in single-table history.
advancedLogClause
    : (WITH | WITHOUT | TRUNCATE) ADVANCED LOG
    ;

alterColumnClause
    : SET DEFAULT (ON NULL)? expression
    | DROP DEFAULT
    | SET NOT? NULL
    | DROP NOT NULL
    | RENAME TO identifier
    | SET NOT? VISIBLE
    ;

cascadeClause
    : RESTRICT
    | CASCADE CONSTRAINTS?
    ;

dropTable
    : DROP TABLE ifExists? target=qualifiedName cascadeClause? PURGE?
    ;

truncateTable
    : TRUNCATE TABLE target=qualifiedName
        (PARTITION identifier | LPAREN identifier RPAREN)?
        ((DROP | REUSE) STORAGE)? CASCADE? ((UPDATE | IGNORE) GLOBAL INDEXES)?
    ;

renameTable
    : RENAME TABLE? target=qualifiedName TO destination=qualifiedName
    ;

// Non-table statements follow the DM SQL Language Manual, rather than Oracle-only clauses.
// https://eco.dameng.com/document/dm/zh-cn/pm/definition-statement.html
// https://eco.dameng.com/document/dm/zh-cn/pm/view.html
// https://eco.dameng.com/document/dm/zh-cn/pm/synonym.html
// https://eco.dameng.com/document/dm/zh-cn/pm/discretionary-access-control.html
// https://eco.dameng.com/document/dm/zh-cn/pm/dm8_sql-definition-deletion.html
// https://eco.dameng.com/document/dm/zh-cn/pm/trigger.html

nonTableStatement
    : createIndex
    | alterIndex
    | dropIndex
    | createView
    | alterView
    | dropView
    | createSequence
    | alterSequence
    | dropSequence
    | createSynonym
    | dropSynonym
    | createRole
    | dropRole
    | alterProgram
    | dropProgram
    | createUser
    | alterUser
    | dropUser
    | createSchema
    | dropSchema
    | createDirectory
    | dropDirectory
    | contextIndexStatement
    | createContext
    | dropContext
    | createLibrary
    | alterLibrary
    | dropLibrary
    | createDomain
    | dropDomain
    | createProfile
    | alterProfile
    | dropProfile
    | statisticsStatement
    | createType
    | dropType
    | dropClass
    | compileObject
    | dropPackage
    | materializedViewStatement
    | linkStatement
    | operatorStatement
    | administrativeStatement
    | tablespaceStatement
    | setStatement
    | grantStatement
    | revokeStatement
    | commentStatement
    | programDefinition
    | javaClassDefinition
    ;

// Object DDL follows the DM SQL Language Manual. Only successful RESTRICT
// deletion is harmless without consulting a later online dictionary.
// https://eco.dameng.com/document/dm/zh-cn/pm/definition-statement.html
createUser
    : CREATE USER ifNotExists? nonTableIdentifier IDENTIFIED userAuthentication userOption*
    ;

alterUser
    : ALTER USER nonTableIdentifier
        (IDENTIFIED userAuthentication userOption* | userOption+)
    ;

dropUser
    : DROP USER ifExists? nonTableIdentifier RESTRICT?
    ;

userAuthentication
    : BY userPassword
    | EXTERNALLY (AS userPassword | nonTableIdentifier)?
    ;

userPassword
    : nonTableIdentifier
    | STRING
    ;

// DROP PROFILE, IP removal and proxy privileges are options inside USER DDL.
// Other command words must remain visible as a separate statement boundary.
userOption
    : DROP PROFILE
    | userIpOption
    | (GRANT | REVOKE) CONNECT THROUGH nonTableIdentifier
    | userOptionElement
    ;

userIpOption
    : (ALLOW_IP | NOT_ALLOW_IP) DROP userPassword (COMMA userPassword)*
    ;

userOptionElement
    : LPAREN userOptionElement* RPAREN
    | ~(LPAREN | RPAREN | SEMICOLON | CREATE | ALTER | DROP | TRUNCATE | RENAME | GRANT | REVOKE)
    ;

// Embedded CREATE/GRANT clauses can affect tables and are not non-table DDL.
createSchema
    : CREATE SCHEMA (nonTableIdentifier (AUTHORIZATION nonTableIdentifier)? | AUTHORIZATION nonTableIdentifier)
    ;

dropSchema
    : DROP SCHEMA ifExists? nonTableIdentifier RESTRICT?
    ;

createDirectory
    : CREATE replacement? DIRECTORY ifNotExists? nonTableIdentifier AS STRING
    ;

dropDirectory
    : DROP DIRECTORY ifExists? nonTableIdentifier
    ;

createContext
    : CREATE replacement? CONTEXT ifNotExists? {_input.LA(1) != INDEX}? nonTableIdentifier nonTableTail
    ;

dropContext
    : DROP CONTEXT ifExists? {_input.LA(1) != INDEX}? nonTableName
    ;

// Full-text indexes belong to an existing table; an application CONTEXT is a
// different object. Do not let its opaque tail consume INDEX as the object name.
// https://eco.dameng.com/document/dm/zh-cn/pm/definition-statement.html (3.8)
contextIndexStatement
    : CREATE CONTEXT INDEX ifNotExists? nonTableName ON nonTableName nonTableTail
    | ALTER CONTEXT INDEX nonTableName ON nonTableName nonTableTail
    | DROP CONTEXT INDEX ifExists? nonTableName ON nonTableName
    ;

createLibrary
    : CREATE replacement? LIBRARY ifNotExists? nonTableName nonTableTail
    ;

alterLibrary
    : ALTER LIBRARY nonTableName nonTableTail
    ;

dropLibrary
    : DROP LIBRARY ifExists? nonTableName
    ;

// class-type.html (12.3) requires CASCADE to drop a class used by table columns
// or another class's members. custom-type.html (13.5.1) applies the same rule
// to object types; dropping BODY removes its implementation, not the type.
// A successful default/RESTRICT drop therefore cannot remove dependent tables.
// custom-type.html (13.4.2): TYPE BODY CASCADE also preserves the type itself.
dropType
    : DROP TYPE ifExists? nonTableName RESTRICT?
    | DROP TYPE BODY ifExists? nonTableName (RESTRICT | CASCADE)?
    ;

dropClass
    : DROP CLASS BODY? ifExists? nonTableName RESTRICT?
    ;

// Section 3.11: DOMAIN columns still require historical type resolution.
// Defining a domain alone does not create columns; cascading its removal is
// deliberately not treated as a harmless object operation.
createDomain
    : CREATE DOMAIN ifNotExists? nonTableName nonTableTail
    ;

dropDomain
    : DROP DOMAIN ifExists? nonTableName RESTRICT?
    ;

// Section 3.20: PROFILE CASCADE removes user/profile associations, not tables.
createProfile
    : CREATE PROFILE ifNotExists? nonTableIdentifier nonTableTail
    ;

alterProfile
    : ALTER PROFILE nonTableIdentifier nonTableTail
    ;

dropProfile
    : DROP PROFILE ifExists? nonTableIdentifier (RESTRICT | CASCADE)?
    ;

// Sections 3.18/3.19: hints are already consumed by the lexer. STAT only
// collects optimizer statistics; the optional rate can be fractional.
statisticsStatement
    : STAT (NUMBER (SIZE NUMBER)? ON
        (INDEX nonTableName | nonTableName LPAREN identifierList RPAREN)
        | ON nonTableName) GLOBAL?
    ;

// https://eco.dameng.com/document/dm/zh-cn/pm/custom-type.html
// A type declaration has no executable body. TYPE BODY and cascading drops
// are separate forms and must not be swallowed by this declaration tail.
createType
    : CREATE replacement? TYPE nonTableName encryptionClause? authidClause?
        ((AS | IS) nonTableElement+ | UNDER nonTableName LPAREN nonTableElement+ RPAREN nonTableTail)
    ;

// Recompilation changes stored code, not a captured table's row definition.
compileObject
    : ALTER PACKAGE nonTableName COMPILE CASCADE? DEBUG?
    | ALTER (TYPE | CLASS) nonTableName COMPILE DEBUG?
    ;

dropPackage
    : DROP PACKAGE BODY? ifExists? nonTableName
    ;

// These objects do not change the captured table's row schema. Only their
// kind, name and statement boundary matter here; DM validates the SQL itself.
createIndex
    : CREATE replacement? (ARRAY | (CLUSTER | NOT PARTIAL)? (UNIQUE | BITMAP JOIN?)?) INDEX
        ifNotExists? nonTableName nonTableTail
    ;

alterIndex
    : ALTER INDEX nonTableName (RENAME TO nonTableName | nonTableTail)
    ;

dropIndex
    : DROP INDEX ifExists? nonTableName nonTableTail
    ;

createView
    : CREATE replacement? FORCE? VIEW ifNotExists? nonTableName nonTableTail
    ;

alterView
    : ALTER VIEW nonTableName nonTableTail
    ;

dropView
    : DROP VIEW ifExists? nonTableName nonTableTail
    ;

// https://eco.dameng.com/document/dm/zh-cn/pm/materialized-view.html
// Internal MTAB$_/MLOG$_ tables are excluded by the connector's default filter.
// Sections 7.1-7.4 and 7.9 define the view options. PREBUILT is parsed but rejected
// by the analyzer because the backing table can be an ordinary captured table.
materializedViewStatement
    : CREATE MATERIALIZED VIEW LOG ON nonTableName nonTableTail
    | CREATE MATERIALIZED VIEW nonTableName materializedViewDefinition
    | ALTER MATERIALIZED VIEW nonTableName
        (materializedViewRefreshOptions materializedViewQueryRewrite? | materializedViewQueryRewrite | COMPILE)
    | DROP MATERIALIZED VIEW LOG ifExists? ON nonTableName
    | DROP MATERIALIZED VIEW ifExists? nonTableName
    | REFRESH MATERIALIZED VIEW nonTableName (FAST | (COMPLETE | FORCE) materializedViewCompleteRefresh?)?
    ;

materializedViewDefinition
    : (LPAREN identifierList RPAREN)?
        (materializedViewPrebuiltClause | (BUILD (IMMEDIATE | DEFERRED))? materializedViewPartitionClause?
            (TABLESPACE nonTableIdentifier)? materializedViewStorageClause?)
        materializedViewRefreshOptions? materializedViewQueryRewrite? AS materializedViewQuery
    ;

materializedViewPrebuiltClause
    : FOR nonTableName ON PREBUILT TABLE ((WITH | WITHOUT) REDUCED PRECISION)?
    ;

materializedViewRefreshOptions
    : REFRESH materializedViewRefreshOption+
    | NEVER REFRESH
    ;

materializedViewRefreshOption
    : FAST | COMPLETE | FORCE
    | ON (DEMAND | COMMIT)
    | START WITH expression
    | NEXT expression
    | WITH (PRIMARY KEY | ROWID)
    | materializedViewCompleteRefresh
    ;

materializedViewCompleteRefresh
    : USING (DEFAULT | TRUNCATE | DELETE)
    ;

materializedViewQueryRewrite
    : (ENABLE | DISABLE) QUERY REWRITE
    ;

// Physical options can include interval/multilevel partitions and storage
// attributes not represented in captured row schemas. Keep their boundaries
// and balanced payloads; DM validates the physical option values.
materializedViewPartitionClause
    : PARTITION BY (HASH | RANGE | LIST) LPAREN identifierList RPAREN materializedViewPartitionElement+
    ;

materializedViewPartitionElement
    : LPAREN materializedViewStorageElement+ RPAREN
    | ~(SEMICOLON | LPAREN | RPAREN | AS | TABLESPACE | STORAGE | REFRESH | NEVER | ENABLE | DISABLE
        | CREATE | ALTER | DROP | TRUNCATE | RENAME | INSERT | UPDATE | DELETE | MERGE | CALL | EXEC | EXECUTE
        | GRANT | REVOKE | SELECT | FOR | ON | PREBUILT)
    ;

materializedViewStorageClause
    : STORAGE LPAREN materializedViewStorageElement+ RPAREN
    ;

materializedViewStorageElement
    : LPAREN materializedViewStorageElement+ RPAREN
    | ~(SEMICOLON | LPAREN | RPAREN | CREATE | ALTER | DROP | TRUNCATE | RENAME | INSERT | UPDATE | DELETE
        | MERGE | CALL | EXEC | EXECUTE | GRANT | REVOKE | SELECT | AS | PREBUILT)
    ;

// Query results do not become captured columns. Preserve analytic functions,
// CTEs and other DM SELECT extensions without interpreting the result schema.
materializedViewQuery
    : (WITH materializedViewCte (COMMA materializedViewCte)*)? SELECT materializedViewQueryElement+
    | LPAREN materializedViewQuery RPAREN
    ;

materializedViewCte
    : identifier (LPAREN identifierList RPAREN)? AS LPAREN materializedViewQuery RPAREN
    ;

materializedViewQueryElement
    : LPAREN materializedViewQueryElement* RPAREN
    | TRUNCATE LPAREN materializedViewQueryElement* RPAREN
    | ~(SEMICOLON | LPAREN | RPAREN | CREATE | ALTER | DROP | TRUNCATE | RENAME | INSERT | UPDATE | DELETE
        | MERGE | CALL | EXEC | EXECUTE | GRANT | REVOKE | INTO | DECLARE | BEGIN)
    ;

createSequence
    : CREATE SEQUENCE ifNotExists? nonTableName nonTableTail
    ;

alterSequence
    : ALTER SEQUENCE nonTableName (RENAME TO nonTableName | nonTableTail)
    ;

dropSequence
    : DROP SEQUENCE ifExists? nonTableName nonTableTail
    ;

createSynonym
    : CREATE replacement? PUBLIC? SYNONYM ifNotExists? nonTableName nonTableTail
    ;

dropSynonym
    : DROP PUBLIC? SYNONYM ifExists? nonTableName nonTableTail
    ;

createRole
    : CREATE ROLE ifNotExists? nonTableIdentifier nonTableTail
    ;

dropRole
    : DROP ROLE ifExists? nonTableIdentifier nonTableTail
    ;

alterProgram
    : ALTER (PROCEDURE | FUNCTION | TRIGGER) nonTableName nonTableTail
    ;

dropProgram
    : DROP (PROCEDURE | FUNCTION | TRIGGER) ifExists? nonTableName nonTableTail
    ;

// Link endpoints and operator signatures do not define captured table columns.
// https://eco.dameng.com/document/dm/zh-cn/pm/external-link.html
linkStatement
    : CREATE replacement? PUBLIC? LINK ifNotExists? nonTableName nonTableTail
    | DROP PUBLIC? LINK ifExists? nonTableName nonTableTail
    ;

// https://eco.dameng.com/document/dm/zh-cn/pm/user-defined-operators.html
// Operator names can be symbolic (e.g. @^), rather than SQL identifiers.
operatorStatement
    : CREATE OPERATOR nonTableElement+
    | DROP OPERATOR ifExists? nonTableElement+
    ;

administrativeStatement
    : ALTER DATABASE (DROP LOGFILE | RENAME LOGFILE | nonTableElement)+
    | ALTER (SYSTEM | SESSION) nonTableElement+
    | ALTER JAVA CLASS nonTableName COMPILE DEBUG?
    ;

// DM SQL Language Manual 3.4.1-3.4.3, including the syntax diagrams.
// https://eco.dameng.com/document/dm/zh-cn/pm/definition-statement.html
// DROP TABLESPACE requires an unused tablespace; DROP DATAFILE requires all
// objects using that file to be removed first. Neither has a cascading form.
// https://eco.dameng.com/document/dm/zh-cn/pm/database-management.html
tablespaceStatement
    : CREATE TABLESPACE ifNotExists? nonTableIdentifier tablespaceDataFiles
        tablespaceCache? tablespaceEncryption? (STRIPING NUMBER)? (HIGH | NORMAL | EXTERNAL)?
        (COPY NUMBER)? MICRO? (WITH HUGE PATH STRING MICRO?)? tablespaceFileLimit? (STORAGE LPAREN ON nonTableIdentifier RPAREN)?
    | ALTER TABLESPACE nonTableIdentifier tablespaceAlterClause
    | DROP TABLESPACE ifExists? nonTableIdentifier
    ;

tablespaceDataFiles
    : DATAFILE tablespaceFile (COMMA tablespaceFile)*
    ;

tablespaceFile
    : STRING (MIRROR STRING)? SIZE NUMBER tablespaceAutoextend?
    ;

tablespaceAutoextend
    : AUTOEXTEND (OFF | ON (NEXT NUMBER)? (MAXSIZE (NUMBER | UNLIMITED) | UNLIMITED)?)
    ;

tablespaceCache
    : CACHE EQUALS_OP (NORMAL | KEEP | IDENTIFIER)
    ;

tablespaceEncryption
    : ENCRYPT WITH nonTableIdentifier (BY (WRAPPED STRING | regularIdentifier))?
    ;

tablespaceFileLimit
    : FILE LIMIT NUMBER
    ;

tablespaceFilePaths
    : STRING (COMMA STRING)*
    ;

tablespaceAlterClause
    : ONLINE | OFFLINE | CORRUPT
    | RENAME TO nonTableIdentifier
    | RENAME DATAFILE tablespaceFilePaths TO tablespaceFilePaths
    | ADD tablespaceDataFiles
    | RESIZE DATAFILE STRING TO NUMBER (ON nonTableIdentifier)?
    | DATAFILE tablespaceFilePaths tablespaceAutoextend?
    | tablespaceCache
    | OPTIMIZE signedNumber
    | ADD HUGE PATH STRING
    | DROP DATAFILE STRING
    | tablespaceFileLimit
    ;

setStatement
    : SET nonTableElement+
    ;

// Java members own semicolons just as SPL blocks do. Match braces without
// interpreting member types, expressions or method bodies as SQL statements.
// https://eco.dameng.com/document/dm/zh-cn/pm/class-type.html
javaClassDefinition
    : CREATE replacement? JAVA PUBLIC? ABSTRACT? FINAL? CLASS nonTableName javaClassHeaderElement* javaClassBody
    ;

javaClassHeaderElement
    : ~(LEFT_CURLY_PAREN | RIGHT_CURLY_PAREN | SEMICOLON | CREATE | ALTER | DROP | TRUNCATE | RENAME)
    ;

javaClassBody
    : LEFT_CURLY_PAREN (javaClassBody | ~(LEFT_CURLY_PAREN | RIGHT_CURLY_PAREN))* RIGHT_CURLY_PAREN
    ;

// Keep parentheses and real semicolons as boundaries, without interpreting
// query expressions, function names, index clauses or sequence options. A new
// DDL command cannot be swallowed by the preceding object's opaque tail.
nonTableTail
    : nonTableElement*
    ;

nonTableElement
    : LPAREN nonTableElement* RPAREN
    | TRUNCATE LPAREN nonTableElement* RPAREN
    | ~(SEMICOLON | LPAREN | RPAREN | CREATE | ALTER | DROP | TRUNCATE | RENAME)
    ;

// DM Security Management 3.3.1-3.3.4: database/role, object and schema privileges
// have different option clauses. Permission existence and applicability remain
// database checks; names containing CREATE/ALTER/DROP are not standalone DDL.
// https://eco.dameng.com/document/dm/zh-cn/pm/discretionary-access-control.html
grantStatement
    : GRANT systemPrivileges TO principals (WITH ADMIN OPTION)?
    | GRANT grantObjectPrivileges OFF? ON privilegeObject TO principals (WITH GRANT OPTION)?
    | GRANT schemaPrivileges ON SCHEMA nonTableIdentifier TO principals (WITH ADMIN OPTION)?
    ;

revokeStatement
    : REVOKE (ADMIN OPTION FOR)? systemPrivileges FROM principals
    | REVOKE (GRANT OPTION FOR)? revokeObjectPrivileges OFF? ON privilegeObject FROM principals (RESTRICT | CASCADE)?
    | REVOKE (GRANT ADMIN FOR)? schemaPrivileges
        ON SCHEMA nonTableIdentifier FROM principals (RESTRICT | CASCADE)?
    ;

systemPrivileges
    : allPrivileges
    | systemPrivilege (COMMA systemPrivilege)*
    ;

systemPrivilege
    : identifier
    | systemPrivilegeWord+
    ;

// Keep database privilege phrases extensible, but do not consume ON/TO/FROM,
// option clauses, column lists or qualified names as privilege text.
systemPrivilegeWord
    : regularIdentifier
    | ADMIN | ALTER | ANY | AUDIT | BACKUP | COMMENT | CONTEXT | CREATE | DATABASE
    | DELETE | DIRECTORY | DOMAIN | DROP | DUMP | EXECUTE | FLASHBACK | GRANT | GROUP
    | INDEX | INSERT | LIBRARY | LINK | LOG | MATERIALIZED | OPERATOR | PACKAGE
    | PARTITION | PROCEDURE | PROFILE | PUBLIC | REFERENCES | RESTORE | ROLE | SCHEMA
    | SELECT | SEQUENCE | SESSION | SYNONYM | TABLE | TABLESPACE | TRIGGER | UNLIMITED
    | UPDATE | USAGE | USER | VIEW
    ;

allPrivileges
    : ALL PRIVILEGES?
    ;

schemaPrivileges
    : allPrivileges
    | schemaPrivilege (COMMA schemaPrivilege)*
    ;

schemaPrivilege
    : (SELECT | INSERT | UPDATE | DELETE | REFERENCES | DUMP | FLASHBACK | CREATE | ALTER | DROP | EXECUTE | USAGE)
        ANY (TABLE | VIEW | MATERIALIZED VIEW | INDEX | CONTEXT INDEX | SEQUENCE | PROCEDURE | PACKAGE | DOMAIN | TRIGGER | SYNONYM)
    ;

grantObjectPrivileges
    : allPrivileges
    | grantObjectPrivilege (COMMA grantObjectPrivilege)*
    ;

grantObjectPrivilege
    : (SELECT | INSERT | UPDATE | REFERENCES) (LPAREN identifierList RPAREN)?
    | otherObjectPrivilege
    ;

revokeObjectPrivileges
    : allPrivileges
    | revokeObjectPrivilege (COMMA revokeObjectPrivilege)*
    ;

revokeObjectPrivilege
    : SELECT | INSERT | UPDATE | REFERENCES
    | otherObjectPrivilege
    ;

otherObjectPrivilege
    : DELETE | EXECUTE | READ | WRITE | USAGE | INDEX | ALTER | SELECT FOR DUMP | FLASHBACK
    ;

privilegeObject
    : (SCHEMA | DIRECTORY) nonTableIdentifier
    | privilegeObjectType? nonTableName
    ;

privilegeObjectType
    : TABLE | VIEW | PROCEDURE | FUNCTION | PACKAGE | CLASS | TYPE | SEQUENCE | DOMAIN | SYNONYM | LIBRARY
    ;

principals
    : identifierList
    ;

// DM SQL Language Manual 3.15: one/two-part object names and two/three-part
// column names. NULL retains historical parser compatibility; native DM 8.1.3.162
// clears comments with an empty string and rejects IS NULL.
commentStatement
    : COMMENT ON ((TABLE | VIEW | SEQUENCE) nonTableName
        | COLUMN nonTableName DOT nonTableIdentifier) IS (STRING | NULL)
    ;

// A missing object name must not consume the start of a following DDL command.
// Quoted keyword names remain available through regularIdentifier.
nonTableName
    : nonTableIdentifier (DOT nonTableIdentifier)?
    ;

nonTableIdentifier
    : regularIdentifier
    | nonCommandKeyword
    ;

// A stored definition owns its internal semicolons. The analyzer rejects any
// additional statement in the same LogMiner record after its closing END.
programDefinition
    : CREATE replacement? (procedureDefinition | functionDefinition | triggerDefinition
        | packageDefinition | classDefinition | typeBodyDefinition)
    ;

procedureDefinition
    : procedureSpecification routineBody
    ;

procedureSpecification
    : PROCEDURE ifNotExists? nonTableName encryptionClause? programParameters? authidClause?
    ;

functionDefinition
    : functionSpecification (routineBody | AGGREGATE USING nonTableName | externalFunctionClause)
    ;

functionSpecification
    : FUNCTION ifNotExists? nonTableName encryptionClause? (FOR CALCULATE)? programParameters?
        RETURN programReturnType (authidClause | DETERMINISTIC | RESULT_CACHE | PARALLEL_ENABLE)* PIPELINED?
    ;

programReturnType
    : SELF AS RESULT
    | programDataType
    ;

routineBody
    : (AS | IS) declarationSection? programBlock nonTableIdentifier?
    ;

// https://eco.dameng.com/document/dm/zh-cn/pm/package.html
// A package specification declares methods; the body may implement them and
// contain an initialization block. Its final END also closes that block.
packageDefinition
    : PACKAGE ifNotExists? nonTableName encryptionClause? authidClause?
        (AS | IS) programDeclaration+ END nonTableIdentifier?
    | PACKAGE BODY nonTableName encryptionClause? (AS | IS)
        programDeclaration* ((DECLARE programDeclaration+)? BEGIN programElement*)? END nonTableIdentifier?
    ;

// https://eco.dameng.com/document/dm/zh-cn/pm/class-type.html
// SQL CLASS definitions share SPL declarations. The Java-style DMSQL class
// syntax has brace-delimited bodies and uses javaClassDefinition instead.
classDefinition
    : CLASS ifNotExists? nonTableName encryptionClause? (UNDER nonTableName)?
        objectModifier* authidClause? (AS | IS) programDeclaration+ END nonTableIdentifier?
    | CLASS BODY nonTableName encryptionClause? (AS | IS)
        programDeclaration* ((DECLARE programDeclaration+)? BEGIN programElement*)? END nonTableIdentifier?
    ;

// https://eco.dameng.com/document/dm/zh-cn/pm/custom-type.html
// Each method owns its terminating semicolon; the surrounding END belongs to
// TYPE BODY and cannot be borrowed from a subsequent archived statement.
typeBodyDefinition
    : TYPE BODY nonTableName encryptionClause? (AS | IS)
        storedRoutineDeclaration+ END nonTableIdentifier?
    ;

objectModifier
    : NOT? (FINAL | INSTANTIABLE | PERSISTABLE)
    ;

methodModifier
    : objectModifier
    | NOT? OVERRIDING
    | MAP
    | STATIC
    | MEMBER
    | CONSTRUCTOR
    ;

// https://eco.dameng.com/document/dm/zh-cn/pm/external-function.html
// Creating an external-function binding does not execute its target. Keep the
// external path/name and USING/LIBRARY boundaries explicit instead of accepting
// an arbitrary suffix that could hide an additional DDL statement.
externalFunctionClause
    : EXTERNAL STRING AND? externalFunctionReference? USING (C | CS | JAVA)
    | AS LANGUAGE (C | CS) LIBRARY nonTableName NAME externalFunctionReference?
    ;

externalFunctionReference
    : (nonTableName | STRING) (LPAREN (programDataType (COMMA programDataType)*)? RPAREN)?
    ;

encryptionClause
    : WITH ENCRYPTION
    ;

authidClause
    : AUTHID (CURRENT_USER | DEFINER)
    ;

programParameters
    : LPAREN (programParameter (COMMA programParameter)*)? RPAREN
    ;

// The SQL Programming Manual accepts DEFAULT and := parameter defaults.
programParameter
    : identifier (IN OUT? | OUT IN? | INOUT)? programDataType programDefault?
    ;

programDefault
    : (DEFAULT | ASSIGN | programAssignmentOperator) expression
    ;

// https://eco.dameng.com/document/dm/zh-cn/pm/dm8_sql-data-types-operators.html
// These are SPL variable/parameter types, not supported captured-column types.
programDataType
    : (identifier (DOT identifier)* PERCENT (TYPE | ROWTYPE) | CURSOR | dataType) programArrayDimensions*
    ;

programArrayDimensions
    : LEFT_BRACKET (expression? (COMMA expression?)*) RIGHT_BRACKET
    ;

declarationSection
    : DECLARE? programDeclaration+
    ;

programDeclaration
    : identifier (COMMA identifier)* CONSTANT? programDataType (NOT NULL)? programDefault? SEMICOLON
    | identifier EXCEPTION (FOR signedNumber (COMMA STRING)?)? SEMICOLON
    | CURSOR identifier programParameters? ((AS | IS | FOR) selectStatement)? SEMICOLON
    | TYPE identifier (AS | IS) localTypeDefinition SEMICOLON
    | SUBTYPE identifier IS programDataType (NOT NULL)? SEMICOLON
    | storedRoutineDeclaration
    ;

localTypeDefinition
    : RECORD LPAREN programRecordField (COMMA programRecordField)* RPAREN
    | VARRAY LPAREN expression RPAREN OF programDataType (NOT NULL)?
    | TABLE OF programDataType (NOT NULL)? (INDEX BY programDataType)?
    | ARRAY dataType programArrayDimensions+
    ;

programRecordField
    : identifier programDataType (NOT NULL)? programDefault?
    ;

storedRoutineDeclaration
    : methodModifier* (procedureDefinition | functionDefinition | procedureSpecification | functionSpecification) SEMICOLON
    ;

programAssignmentOperator
    : COLON EQUALS_OP
    | EQUALS_OP
    ;

// Native table/view triggers, timers and DDL/system event triggers have
// distinct headers. Scheduling a stored body does not execute its contents.
triggerDefinition
    : TRIGGER qualifiedName encryptionClause? triggerHeader declarationSection? programBlock identifier?
    ;

triggerHeader
    : AFTER TIMER ON DATABASE timerScheduleElement*
    | ((BEFORE | AFTER) triggerDmlEvents | INSTEAD OF triggerDmlEvents) LOCAL? ON qualifiedName
        triggerReferencing? (FOR EACH (ROW | STATEMENT))? triggerOrdering?
        (WHEN LPAREN expression RPAREN)?
    | (BEFORE | AFTER) triggerEvents ON triggerScope
        (EXECUTE AT (identifier | NUMBER))? (WHEN expression)?
    ;

// The database validates calendar expressions and execution-node options.
// Preserve parentheses and the declaration/body boundary, rather than
// interpreting every scheduling clause as part of captured table DDL.
// https://eco.dameng.com/document/dm/zh-cn/pm/trigger.html (14.1.3)
timerScheduleElement
    : LPAREN timerScheduleElement* RPAREN
    | ~(LPAREN | RPAREN | SEMICOLON | BEGIN | END | DECLARE | CREATE | ALTER | DROP | TRUNCATE | RENAME)
    ;

triggerDmlEvents
    : triggerDmlEvent (OR triggerDmlEvent)*
    ;

triggerDmlEvent
    : INSERT
    | DELETE
    | UPDATE (OF identifierList)?
    ;

triggerReferencing
    : REFERENCING
        (OLD ROW? AS? identifier (NEW ROW? AS? identifier)?
        | NEW ROW? AS? identifier)
    ;

triggerOrdering
    : (FOLLOWS | PRECEDES) qualifiedName (COMMA qualifiedName)*
    ;

triggerScope
    : (identifier DOT)? SCHEMA
    | DATABASE
    ;

triggerEvents
    : triggerEvent (OR triggerEvent)*
    ;

triggerEvent
    : DDL
    | CREATE
    | ALTER
    | DROP
    | GRANT
    | REVOKE
    | TRUNCATE
    | COMMENT
    | LOGIN
    | LOGON
    | LOGOUT
    | LOGOFF
    | SERERR
    | BACKUP DATABASE
    | RESTORE DATABASE
    | TIMER
    | STARTUP
    | SHUTDOWN
    | CHECKPOINT
    | AUDIT
    | NOAUDIT
    ;

// The saved SPL body is balanced to locate the definition boundary; these rules
// do not claim to validate the complete executable SPL language. END IF/LOOP
// belongs to an inner control statement and cannot terminate the outer block.
programBlock
    : BEGIN programElement* END
    ;

programElement
    : programBlock
    | CASE programElement* END CASE?
    | END (IF | LOOP)
    | ~(BEGIN | CASE | END)
    ;

// https://eco.dameng.com/document/dm/zh-cn/pm/dm_sql-introduction.html
// Type syntax is independent of JDBC/history type mapping in the connector.
dataType
    : (CHAR | CHARACTER | NATIONAL CHAR | NATIONAL CHARACTER | NCHAR) VARYING? typePrecision?
    | (VARCHAR | VARCHAR2 | NVARCHAR | NVARCHAR2 | BINARY | VARBINARY | RAW) typePrecision?
    | (NUMERIC | DECIMAL | DEC | NUMBER_KEYWORD | FLOAT) typePrecision?
    | DOUBLE PRECISION? typePrecision?
    | (INT | INTEGER | BIGINT | SMALLINT | TINYINT | BYTE | REAL | BIT | BOOLEAN | DATE | BLOB | CLOB | TEXT | IMAGE | BFILE)
    | (TIME | TIMESTAMP | DATETIME) typePrecision? (WITH LOCAL? TIME ZONE | WITHOUT TIME ZONE)?
    | INTERVAL (YEAR | MONTH | DAY | HOUR | MINUTE | SECOND) typePrecision?
        (TO (YEAR | MONTH | DAY | HOUR | MINUTE | SECOND) typePrecision?)?
    | typeName typePrecision?
    ;

// Unknown/user-defined names stay available for a positioned semantic error,
// but column attributes such as NOT NULL cannot be mistaken for a type name.
typeName
    : (regularIdentifier | storedObjectKeyword | objectDdlKeyword) (DOT identifier)?
    ;

typePrecision
    : LPAREN NUMBER (COMMA signedNumber | CHAR | BYTE)? RPAREN
    ;

// https://eco.dameng.com/document/dm/zh-cn/pm/check-phrases.html
// Queries and expressions are a bounded SQL subset, rather than an opaque tail.
// Unsupported query forms fail closed; they are not evidence of harmless DDL.
selectStatement
    : (WITH commonTableExpression (COMMA commonTableExpression)*)? queryExpression orderByClause?
    ;

commonTableExpression
    : identifier (LPAREN identifierList RPAREN)? AS LPAREN selectStatement RPAREN
    ;

queryExpression
    : selectQuery ((UNION ALL? | INTERSECT | MINUS | EXCEPT) selectQuery)*
    ;

selectQuery
    : SELECT (ALL | DISTINCT)? selectItem (COMMA selectItem)*
        (FROM tableReference (COMMA tableReference)*)?
        (WHERE expression)? (GROUP BY expressionList)? (HAVING expression)?
    ;

selectItem
    : ASTERISK
    | expressionIdentifier DOT ASTERISK
    | expression (AS identifier | regularIdentifier)?
    ;

tableReference
    : tablePrimary joinClause*
    ;

tablePrimary
    : (qualifiedName | LPAREN selectStatement RPAREN) (AS? regularIdentifier)?
    ;

joinClause
    : (INNER | (LEFT | RIGHT | FULL) OUTER?)? JOIN tablePrimary
        (ON expression | USING LPAREN identifierList RPAREN)
    | CROSS JOIN tablePrimary
    | NATURAL (INNER | (LEFT | RIGHT | FULL) OUTER?)? JOIN tablePrimary
    ;

orderByClause
    : ORDER BY orderByItem (COMMA orderByItem)*
    ;

orderByItem
    : expression (ASC | DESC)? (NULLS (FIRST | LAST))?
    ;

expressionList
    : expression (COMMA expression)*
    ;

expression
    : orExpression
    ;

orExpression
    : andExpression (OR andExpression)*
    ;

andExpression
    : notExpression (AND notExpression)*
    ;

notExpression
    : NOT notExpression
    | predicate
    ;

predicate
    : concatenationExpression
        (comparisonOperator concatenationExpression
        | IS NOT? NULL
        | IS NOT? JSON jsonFormat? ((WITH | WITHOUT) UNIQUE KEYS)?
        | NOT? BETWEEN concatenationExpression AND concatenationExpression
        | NOT? IN LPAREN (selectStatement | expressionList) RPAREN
        | NOT? LIKE concatenationExpression (ESCAPE concatenationExpression)?)?
    ;

jsonFormat
    : STRICT | LAX
    | LPAREN (STRICT | LAX) RPAREN
    ;

comparisonOperator
    : EQUALS_OP
    | LESS_THAN_OP EQUALS_OP?
    | GREATER_THAN_OP EQUALS_OP?
    | LESS_THAN_OP GREATER_THAN_OP
    | (EXCLAMATION | CARET | TILDE) EQUALS_OP
    ;

concatenationExpression
    : additiveExpression (BAR BAR additiveExpression)*
    ;

additiveExpression
    : multiplicativeExpression ((PLUS_SIGN | MINUS_SIGN) multiplicativeExpression)*
    ;

multiplicativeExpression
    : unaryExpression ((ASTERISK | SOLIDUS | PERCENT) unaryExpression)*
    ;

unaryExpression
    : (PLUS_SIGN | MINUS_SIGN) unaryExpression
    | primaryExpression
    ;

primaryExpression
    : STRING
    | HEX_BINARY_LITERAL
    | NUMBER
    | DOT NUMBER
    | NULL
    | TRUE
    | FALSE
    | MAXVALUE
    | (DATE | TIME | TIMESTAMP | DATETIME) STRING
    | intervalLiteral
    | CAST LPAREN expression AS dataType RPAREN
    | CASE expression? (WHEN expression THEN expression)+ (ELSE expression)? END
    | EXISTS LPAREN selectStatement RPAREN
    // Trigger WHEN conditions allow OLD/NEW fields with or without a colon, including aliases.
    | COLON identifier DOT identifier
    | (OLD | NEW) DOT identifier
    | expressionName (LPAREN (ASTERISK | DISTINCT? functionArgumentList)? RPAREN)?
    | keywordFunction LPAREN functionArgumentList? RPAREN
    | LPAREN (selectStatement | expressionList) RPAREN
    ;

// Date-part keywords are valid function arguments, but must not become bare
// operands that could consume a following column or statement clause.
functionArgumentList
    : functionArgument (COMMA functionArgument)*
    ;

functionArgument
    : expression
    | YEAR | MONTH | DAY | HOUR | MINUTE | SECOND
    ;

intervalLiteral
    : INTERVAL (PLUS_SIGN | MINUS_SIGN)? STRING (YEAR | MONTH | DAY | HOUR | MINUTE | SECOND) typePrecision?
        (TO (YEAR | MONTH | DAY | HOUR | MINUTE | SECOND) typePrecision?)?
    ;

expressionName
    : expressionIdentifier (DOT identifier)*
    ;

// Structural keywords cannot become bare expression operands. This prevents a
// missing statement delimiter from becoming a query/default expression suffix.
expressionIdentifier
    : regularIdentifier
    | VALUE | USER | CURRENT_USER | LEVEL | ROWID | COLUMN | COMMENT | FORCE
    | INDEX | LOGIC | ROLE | SCHEMA | SEQUENCE | TABLE | VIEW
    | ADVANCED | JSON | KEYS | LAX | STRICT
    | storedObjectKeyword
    | objectDdlKeyword
    ;

keywordFunction
    : REPLACE | LEFT | RIGHT | LOG | CHAR | FIELD | TRUNCATE
    ;

signedNumber
    : (PLUS_SIGN | MINUS_SIGN)? NUMBER
    ;

replacement: OR REPLACE;
ifNotExists: IF NOT EXISTS;
ifExists: IF EXISTS;

qualifiedName
    : identifier (DOT identifier)?
    ;

identifierList
    : identifier (COMMA identifier)*
    ;

identifier
    : regularIdentifier
    | contextualKeyword
    ;

regularIdentifier
    : WORD
    | IDENTIFIER
    ;

// The connector historically accepts keyword tokens as contextual names. This
// compatibility rule is not a declaration that every DM keyword is non-reserved.

contextualKeyword
    : CREATE | ALTER | DROP | TRUNCATE | RENAME
    | nonCommandKeyword
    ;

nonCommandKeyword
    : ACTION | ADD | ADMIN | ADVANCED | AFTER | ALL | ALWAYS | AND
    | ANY | ARE | AS | ASC | ASSIGN | ASYNCHRONOUS | AT | AUDIT
    | AUTHID | AUTO_INCREMENT | BACKUP | BADFILE | BEFORE | BEGIN | BETWEEN | BFILE
    | BIGINT | BINARY | BIT | BITMAP | BLOB | BOOLEAN | BRANCH | BY
    | BYTE | CACHE | CALCULATE | CASCADE | CASCADED | CASE | CAST | CHAR
    | CHARACTER | CHARACTER_CODE | CHECK | CHECKPOINT | CLASS | CLOB | CLUSTER | CLUSTERBTR
    | COLUMN | COMMENT | COMMIT | COMPILE | COMPRESS | CONNECT | CONSTANT | CONSTRAINT
    | CONSTRAINTS | CONTEXT | COUNTER | CREATION | CROSS | CURRENT | CURRENT_USER
    | CURSOR | CYCLE | DATABASE | DATAFILE | DATE | DATETIME | DAY | DDL
    | DEBUG | DEC | DECIMAL | DECLARE | DEFAULT | DEFERRABLE | DEFERRED | DEFINER
    | DELETE | DELIMITED | DELTA | DESC | DETERMINISTIC | DIRECTORY | DISABLE | DISKSPACE
    | DISTINCT | DOMAIN | DOUBLE | DUMP | EACH | ELSE | ENABLE
    | ENCRYPTION | END | EQU | ERRORS | ESCAPE | EXCEPT | EXCEPTION | EXCLUSIVE
    | EXECUTE | EXISTS | EXTERNAL | FALSE | FIELD | FIELDS | FILESIZE | FILLFACTOR
    | FIRST | FLASHBACK | FLOAT | FOLLOWS | FOR | FORCE | FOREIGN | FROM
    | FULL | FUNCTION | GENERATED | GLOBAL | GRANT | GROUP | HASH | HAVING
    | HOUR | HUGE | IDENTITY | IF | IGNORE | IMAGE | IMMEDIATE | IN
    | INCREMENT | INDEX | INDEXES | INITIAL | INITIALLY | INNER | INOUT | INSERT
    | INSTEAD | INT | INTEGER | INTERSECT | INTERVAL | INTO | INVISIBLE | IS
    | JOIN | JSON | KEY | KEYS | LAST | LAX | LEFT | LESS | LEVEL | LIBRARY | LIKE
    | LIMIT | LIST | LOCAL | LOCATION | LOG | LOGIC | LOGIN | LOGOFF
    | LOGON | LOGOUT | LONG | LOOP | MATCH | MATERIALIZED | MAXVALUE | MINEXTENTS
    | MINUS | MINUTE | MINVALUE | MISSING | MODIFY | MONITORING | MONTH | MOVE
    | NATIONAL | NATURAL | NCHAR | NEW | NEXT | NO | NOAUDIT | NOBRANCH
    | NOCACHE | NOCOMPRESS | NOCYCLE | NOMAXVALUE | NOMINVALUE | NOMONITORING | NONCLUSTERED | NONE
    | NOORDER | NOPARALLEL | NOSORT | NOT | NOVALIDATE | NULL | NULLS | NULL_STR
    | NUMBER_KEYWORD | NUMERIC | NVARCHAR | NVARCHAR2 | OF | OFF | OLD | ON
    | ONLINE | ONLY | OPEN | OPTION | OR | ORDER | OUT | OUTER
    | PACKAGE | PARALLEL | PARMS | PARTIAL | PARTITION | PARTITIONS | PCTFREE | PENDANT
    | PIPELINED | PRECEDES | PRECISION | PRESERVE | PRIMARY | PRIOR | PRIVILEGES | PROCEDURE
    | PREBUILT | PUBLIC | PURGE | RANGE | RAW | READ | REAL | REBUILD | RECORDS | REFRESH
    | REFERENCES | REFERENCING | REPLACE | RESTORE | RESTRICT | RETURN | REUSE
    | REVERSE | REVOKE | RIGHT | ROLE | ROLLBACK | ROW | ROWID | ROWS
    | SCHEMA | SECOND | SECTION | SEGMENT | SELECT | SEQUENCE | SERERR | SESSION
    | SET | SHARE | SHUTDOWN | SIMPLE | SKIP_KEYWORD | SMALLINT | START | STARTUP
    | STAT | STATEMENT | STORAGE | STORE | STRICT | SUBPARTITION | SYNCHRONOUS | SYNONYM | TABLE
    | TABLESPACE | TEMPORARY | TEXT | THAN | THEN | TIME | TIMER | TIMESTAMP
    | TINYINT | TO | TRIGGER | TRIGGERS | TRUE | TYPE | UNION
    | UNIQUE | UNLIMITED | UNUSABLE | UPDATE | USAGE | USER | USING | VALIDATE
    | VALUE | VALUES | VARBINARY | VARCHAR | VARCHAR2 | VARYING | VIEW | VIRTUAL
    | VISIBLE | WHEN | WHERE | WITH | WITHOUT | WORK | WRITE | YEAR
    | ZONE
    | storedObjectKeyword
    | objectDdlKeyword
    ;

// Object headers introduce contextual words without reserving existing column,
// type or expression names that were previously ordinary identifier tokens.
objectDdlKeyword
    : ALLOW_IP | AUTHORIZATION | BODY | EXTERNALLY | IDENTIFIED | NOT_ALLOW_IP
    | PROFILE | SIZE | THROUGH | ABSTRACT | LINK | LOGFILE | OPERATOR | SYSTEM
    | AUTOEXTEND | COPY | CORRUPT | ENCRYPT | FILE | HIGH | KEEP | MAXSIZE | MICRO
    | MIRROR | NORMAL | OFFLINE | OPTIMIZE | PATH | RESIZE | STRIPING | WRAPPED
    | BUILD | CALL | COMPLETE | DEMAND | EXEC | FAST | MERGE | NEVER | QUERY | REDUCED | REWRITE
    | EXCHANGE | HASHPARTMAP | MOVEMENT | SPLIT | SUBPARTITIONS | TEMPLATE
    ;

// Preserve the contextual identifier behavior of words that now also identify
// stored-object headers. Adding a program keyword must not reserve table names
// or break existing default/CTAS expressions that use those names.
storedObjectKeyword
    : AGGREGATE | ARRAY | C | CONSTRUCTOR | CS | FINAL | INSTANTIABLE | JAVA
    | LANGUAGE | MAP | MEMBER | NAME | OVERRIDING | PARALLEL_ENABLE | PERSISTABLE
    | RECORD | RESULT | RESULT_CACHE | ROWTYPE | SELF | STATIC | SUBTYPE | UNDER | VARRAY
    ;
