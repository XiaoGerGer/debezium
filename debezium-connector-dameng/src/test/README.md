# Dameng connector tests

Run tests with the Maven wrapper from the repository root, using JDK 21 or newer:

```sh
./mvnw -pl debezium-connector-dameng -am test
./mvnw -pl debezium-connector-dameng -am verify -Pdameng-tests
./mvnw -pl debezium-connector-dameng -am verify -Pdameng-tests \
  -Dit.test=DamengPostgresCompatibilityIT -Dfailsafe.failIfNoSpecifiedTests=false \
  -Ddameng.compatible.mode=7
```

The first command runs unit tests; the second also runs routine database and embedded-engine integration tests. The third runs all 16 PostgreSQL-mode FLOAT_MODE/NUMBER_MODE/ORA_DATE_FMT/BACKSLASH_ESCAPE combinations, covering snapshot, historical DDL, CRUD and recovery. `-am` includes reactor dependencies. Use `JAVA_HOME=/path/to/jdk` before the command if the shell defaults to an older JDK.

ORA_DATE_FMT is effective only in Oracle mode. Other profiles do not read, validate or include it in compatibility signatures;
their matrices retain its variation as a control. Unit tests verify missing and arbitrary reported values are ignored.
LENGTH_IN_CHAR is not read, validated or tracked in any mode; VARCHAR values remain strings regardless of the declared length unit.

Database tests require Docker and the documented DM image. They create and clean up their own containers unless an external test database is explicitly configured. Integration tests are skipped by default.

| Scope | Maven profile | Selection |
| --- | --- | --- |
| Routine database/engine regression | `dameng-tests` | Excludes probes, performance experiments and independent Connect runtime tests |
| Independent Kafka Connect runtime | `dameng-connect-tests,assembly` | `DamengKafkaConnectIT`; packages the current connector |
| Manual capability/dependency diagnostics | `dameng-probes` | Only `*ProbeIT` |
| Performance experiments | `dameng-performance-tests,assembly` | Only `*BenchmarkIT` and `*PerformanceIT`; check each experiment's opt-in properties |
| All Failsafe tests except independent Connect runtime | `dameng-all-tests` | Includes diagnostics and performance; opt-in conditions still apply |

Run one diagnostic explicitly, for example:

```sh
./mvnw -pl debezium-connector-dameng -am verify -Pdameng-probes \
  -Dit.test=DamengDdlSyntaxProbeIT -Dfailsafe.failIfNoSpecifiedTests=false
```

Shared configuration, SQL samples and datatype inputs live in `DamengTestConfiguration`, `DamengDdlTestCases` and `DamengDatatypeCases`. Executable test classes should not provide fixtures to other test classes. Large suites use JUnit `@Nested` groups by behavior while sharing their enclosing fixture.

Select a complete nested suite with `-Dit.test=DamengConnectorIT`. Run method-filtered selections separately: mixing a nested suite name with another class's `#method` filter in one `it.test` value can omit the nested suite. Check the executed class names and counts in the XML reports.

Surefire writes unit reports under `target/surefire-reports`; Failsafe writes integration reports under `target/failsafe-reports`. Use a clean run when collecting coverage with `dameng-coverage` so previous JaCoCo data is not included. A targeted run does not establish complete regression coverage.

See the [Dameng manual](../../../documentation/modules/ROOT/pages/connectors/dameng.adoc#dameng-testing) for images, offline preparation and coverage checks. [testing.adoc](testing.adoc) contains the applicability matrix and performance properties; [regression-history.adoc](regression-history.adoc) preserves dated execution records.

Native compatibility regression uses `-Pdameng-tests -Dit.test=DamengNativeCompatibilityIT,DamengLexicalSettingsIT -Ddameng.compatible.mode=0`.
`DamengNativeCompatibilityIT` owns disposable databases and covers all 16 FLOAT_MODE/NUMBER_MODE/ORA_DATE_FMT/BACKSLASH_ESCAPE combinations.
It checks binary/decimal FLOAT aliases, pure DATE, compiled string defaults, snapshot, archived CREATE/ALTER and CRUD,
NULL transitions, deleted rows and two restarts against independently read JDBC values and metadata.
On the current DM 8.1.3.162 fixture, NUMBER_MODE=1 selects decimal FLOAT and BACKSLASH_ESCAPE=1 enables escape decoding;
FLOAT_MODE and ORA_DATE_FMT do not change the sampled values. Native BACKSLASH_ESCAPE=1 also observes the session's actual
raw/escaped behavior and records it as derived NATIVE_BACKSLASH_SEMANTICS, preserving the raw behavior of DM 8.1.2.128.
The native signature now includes NUMBER_MODE;
older native offsets without this parameter require a new capture baseline.
With NUMBER_MODE=1, derived NATIVE_REAL_SEMANTICS records the actual CAST metadata: decimal REAL on 8.1.3.162,
binary REAL on 8.1.2.128. Both derived settings are checked at mining boundaries without an extra query per boundary.
The lexical test also checks DISQL, bound control characters and nested comments.
The same lexical test can be selected on the older DM 8.1.2.128 image to verify its raw behavior.
One-off parameter probes remain outside the repository.

Oracle compatibility regression uses `-Pdameng-tests -Dit.test=DamengOracleCompatibilityIT,DamengCaseSensitivityIT -Ddameng.compatible.mode=2`.
`DamengOracleCompatibilityIT` owns disposable databases and covers all 16 FLOAT_MODE/NUMBER_MODE/ORA_DATE_FMT/BACKSLASH_ESCAPE combinations.
Sqlstring/adaptive time modes and both nested-comment settings are distributed across the matrix; each NUMBER_MODE/BACKSLASH_ESCAPE combination covers both time modes.
Add `DamengOracleNumberModeIT` with `-Ddameng.test.ini.NUMBER_MODE=1` to verify decimal FLOAT values and defaults, database precision rounding, offline DDL/CRUD and recovery in precise, string and double decimal modes.
The dedicated engine test checks timestamp DATE values and defaults, numeric aliases, archived CREATE/ALTER and CRUD, NULL transitions and restart recovery in sqlstring/adaptive modes.
String images and schema defaults are checked independently against JDBC, including plain/N/E/Q literals,
single/multi-column ADD, ALTER DEFAULT and bound literal backslashes. Inline comments in archived CREATE statements
are also checked against JDBC; current snapshot metadata does not capture column comments. One-off diagnostic scripts belong outside the repository.

MySQL compatibility regression uses `-Pdameng-tests -Dit.test=DamengMySqlCompatibilityIT -Ddameng.compatible.mode=4`.
The test owns disposable databases and covers all 16 FLOAT_MODE/NUMBER_MODE/ORA_DATE_FMT/BACKSLASH_ESCAPE combinations.
CASE_SENSITIVE, nested comments and MY_STRICT_TABLES each take both values across the matrix.
Optionally fix strict mode with `-Ddameng.test.ini.MY_STRICT_TABLES=0` or `1`.
It checks backticks, escaped names, auto-increment keys, DATETIME, high-precision decimals, TEXT/BLOB aliases, defaults,
archived CREATE/ALTER and CRUD, decimal FLOAT, escaped defaults, literal backslashes in bound parameters,
NULL transitions, deleted rows and two restarts against independently read JDBC values.

PostgreSQL compatibility regression uses the third command above and owns its disposable databases.
It checks both CASE_SENSITIVE and nested-comment settings across all 16 parameter combinations, quoted identifiers,
TIME/TIMESTAMP WITHOUT TIME ZONE, decimal FLOAT and precision changes, plain/Q/N/E defaults, ALTER DEFAULT,
bound literal backslashes, archived CREATE/ALTER and CRUD, NULL transitions, deleted rows and two restarts.
Expected images are read independently through JDBC before deletion. One-off probes stay outside the repository.
