# Dameng connector tests

Run the functional regression from the repository root:

```sh
python3 debezium-connector-dameng/src/test/run-tests.py
```

Requires Linux x86_64, local Docker, Python 3.9+, and JDK 21+. The runner builds reactor dependencies, starts isolated databases/Kafka/workers, runs the integrated BSEG generator, and checks fresh line coverage >= 95%. No external datagen checkout or existing Kafka service is required.

Database capability probes and load benchmarks are kept out of the routine regression profile. Run one explicitly with `./mvnw -pl debezium-connector-dameng verify -Pdameng-probes -Dit.test=DamengDdlSyntaxProbeIT` (replace the class name with the probe you need). The probe profile selects only `*ProbeIT`, `*BenchmarkIT`, and `*PerformanceIT`. Use `-Pdameng-all-tests` only when you intentionally need the entire Failsafe suite, including diagnostics.

See the [unified Dameng documentation](../../../documentation/modules/ROOT/pages/connectors/dameng.adoc#dameng-testing) for prerequisites, images, offline transfer, suite selection, data generator settings, and diagnostics that are separate from functional regression.

See [testing.adoc](testing.adoc) for the Oracle test applicability matrix, implemented coverage, known gaps and real database execution instructions.
