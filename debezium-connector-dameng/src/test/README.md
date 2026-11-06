# Dameng connector tests

Run tests with the Maven wrapper from the repository root, using JDK 21 or newer:

```sh
./mvnw -pl debezium-connector-dameng -am test
./mvnw -pl debezium-connector-dameng -am verify -Pdameng-tests
./mvnw -pl debezium-connector-dameng -am verify -Pdameng-tests \
  -Dit.test=DamengPostgresCompatibilityIT -Dfailsafe.failIfNoSpecifiedTests=false \
  -Ddameng.compatible.mode=7
```

The first command runs unit tests; the second also runs routine database and embedded-engine integration tests. The third runs the PostgreSQL-mode snapshot, historical DDL, CRUD and recovery regression. `-am` includes reactor dependencies. Use `JAVA_HOME=/path/to/jdk` before the command if the shell defaults to an older JDK.

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

Oracle compatibility regression uses `-Pdameng-tests -Dit.test=DamengOracleCompatibilityIT,DamengCaseSensitivityIT -Ddameng.compatible.mode=2`.
The fixture accepts `-Ddameng.test.ini.FLOAT_MODE=0|1`, `-Ddameng.test.ini.ORA_DATE_FMT=0|1` and `-Ddameng.test.ini.NUMBER_MODE=0|1` (choose one value for each).
Add `DamengOracleNumberModeIT` with `-Ddameng.test.ini.NUMBER_MODE=1` to verify decimal FLOAT values and defaults, database precision rounding, offline DDL/CRUD and recovery in precise, string and double decimal modes.
The dedicated engine test checks timestamp DATE values and defaults, numeric aliases, archived CREATE/ALTER and CRUD, NULL transitions and restart recovery in sqlstring/adaptive modes.
Run the four parameter combinations separately and preserve their reports. One-off diagnostic scripts belong outside the repository.

MySQL compatibility regression uses `-Pdameng-tests -Dit.test=DamengMySqlCompatibilityIT -Ddameng.compatible.mode=4`.
The test owns disposable databases and covers all 16 FLOAT_MODE/NUMBER_MODE/ORA_DATE_FMT/BACKSLASH_ESCAPE combinations.
CASE_SENSITIVE, nested comments and MY_STRICT_TABLES each take both values across the matrix.
Optionally fix strict mode with `-Ddameng.test.ini.MY_STRICT_TABLES=0` or `1`.
It checks backticks, escaped names, auto-increment keys, DATETIME, high-precision decimals, TEXT/BLOB aliases, defaults,
archived CREATE/ALTER and CRUD, decimal FLOAT, escaped defaults, literal backslashes in bound parameters,
NULL transitions, deleted rows and two restarts against independently read JDBC values.
