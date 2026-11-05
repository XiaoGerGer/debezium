# Dameng connector tests

Run tests with the Maven wrapper from the repository root, using JDK 21 or newer:

```sh
./mvnw -pl debezium-connector-dameng -am test
./mvnw -pl debezium-connector-dameng -am verify -Pdameng-tests
```

The first command runs unit tests; the second also runs routine database and embedded-engine integration tests. `-am` includes reactor dependencies. Use `JAVA_HOME=/path/to/jdk` before the command if the shell defaults to an older JDK.

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
