# How this was measured

The rule the guide is built on: **measure, then claim.** Plausible reasoning about Camel's
error handling turns out to be wrong about as often as it is right, so a statement in
[guide.md](guide.md) is either reproduced by a probe here, cited to a method in the
`camel-4.18.0` sources, or marked *not measured*. There is no fourth category.

## Running them

```bash
mvn clean test                      # every probe, about 40s
mvn test -Dtest=TransactionProbe    # one class
python3 tools/check-links.py        # every probe link resolves to a real test
```

Check the exit code, not the output. A compile failure leaves the previous
`target/surefire-reports/*.txt` in place, so grepping the output for failures reads stale
green results from the last successful build — which once hid a broken `@Override` for
several turns. `echo $?`, or look for `BUILD SUCCESS`.

Use `clean`. Adding an anonymous inner class and running without it can leave a stale class
file behind; if that lands inside a transaction policy the transaction stays associated with
the thread and the *next* test fails, so one stale file produces failures in tests that
passed minutes earlier.

## Versions

Everything is pinned. Findings that do not transfer are worse than no findings.

| | |
|---|---|
| Apache Camel | **4.18.0** — every source citation is at the `camel-4.18.0` tag |
| JDK | 25 (`maven.compiler.release` 25) |
| Transaction managers | Spring `DataSourceTransactionManager` via `camel-spring`, **and** `camel-jta` with Narayana 7.3.4.Final |
| Database | H2 2.3.232, in memory |
| Connection pool | HikariCP 6.3.0, used only where a probe needs a bounded pool |
| Broker | ActiveMQ 6.3.1 broker and client, in-JVM `vm://`. `camel-activemq`'s `activemq-client-jakarta` 5.19.1 is excluded in favour of it |
| HTTP | `camel-platform-http-vertx`, binding a real port in the test JVM |
| Circuit breaker | `camel-resilience4j` |
| Test framework | JUnit 5 via `camel-test-junit5`, AssertJ 3.27.3 |

**Version bumps are manual, at LTS moves.** The pin means the probes will pass, so CI is not
regression-testing Camel — it guards the evidence layer, which is a different job (below).
When the version does move, the probes assert *values* — 10/20/1000 for the default pool, 25
bulkhead permits, 1000 for `seda:` — so a bump surfaces as failing assertions naming exactly
which claims changed. That is a work list rather than a research project.

Last full run against the pin: **2026-08-31**, 168 tests, green.

## No containers

A probe is a JUnit test that runs in about a second. The broker, the HTTP port and the
database are all in-JVM. This is deliberate and it is the main reason the evidence layer
exists at all: the work that produced this guide began in a codebase where every experiment
needed Testcontainers, which was slow enough and broke often enough that reasoning routinely
outran measurement. Several confident claims from that period turned out to be wrong.

Two consequences worth knowing when reading a probe:

- `pollTimeout=100` on every `seda:` consumer URI. `SedaConsumer` polls with a one-second
  timeout and shutdown waits for that poll on every endpoint, which cost about four seconds
  per test method before it was set. Teardown only — it never affects a finding.
- `NarayanaRequiredPolicy` is the single JTA policy, a faithful copy of the Quarkus
  `TransactionalJtaTransactionPolicy`, and is deliberately shared rather than copied into
  each probe. Shared helpers are usually the wrong instinct here — a probe should read as a
  self-contained claim — and this is the exception, because it is subtle lifecycle code whose
  correctness is not the subject of any probe.

## How a probe is written

The test name states the finding, the assertion message says why it matters, and the failure
output is therefore the documentation:

```java
@Test
void handledAlone_returnsAResponse_butCommitsTheWrites() throws Exception {
    var out = template.request("direct:in", ex -> ex.getIn().setBody("x"));

    assertThat(out.getMessage().getBody(String.class)).isEqualTo("MAPPED");
    assertThat(rows())
            .describedAs("the clause cleared the exception before the boundary looked, "
                    + "so gate 2 saw a clean exchange and committed the row the failure "
                    + "was supposed to undo")
            .isEqualTo(1);
}
```

`ProbeSupport` supplies the database, a real `.transacted()` boundary, `rows()`, `notes()`
and `stamps(exchange)` — which prints claimed / handled / rollback-mark in the guide's
vocabulary rather than Camel's internal names.

[How to write one that catches the commit-on-failure](guide.md#how-to-write-a-probe) is in
the guide, because the trap it describes is the reason the suite exists.

## What CI protects

Two checks, and the second is the one that matters over time.

1. **The probes run on every push**, against the pinned version. This is a guard against a
   probe being quietly edited out from under a claim, not against Camel changing.
2. **The links are checked.** Every `[probe](…)` in the documents must resolve to a file
   containing the test method it names, and every in-document anchor must resolve to a real
   heading. This is what stops the evidence layer from rotting invisibly — which has already
   happened twice in this project's short life, once as a stale footer contradicting the
   document above it and once as prose references to numbered questions after a question was
   inserted.

## Known gaps

- **The transaction manager.** Most probes run on Spring's manager; the application this work
  came from runs `camel-jta`. Where the two differ the guide says which one a finding is
  stated for, and two claims are marked as not transferring: the nested `REQUIRES_NEW` row,
  and what a clause sees on a retry that was given a fresh transaction.
- **Outbox and relay** is a pattern rather than a construct — there is no single thing to
  point a probe at, and most of its answers depend on how the relay is built.
- **Splitters and aggregators**, **stream caching**, **graceful shutdown** and the
  **streaming-transfer** section are reasoned, not measured, and say so where they appear.
