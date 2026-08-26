# camel-sandbox

A scratch project for settling Apache Camel semantics **by experiment**, and the working space for
an application design guide built on what those experiments show.

Nothing here ships. Break it freely.

## The job

Produce a **design guide for Camel applications, aimed at both humans and agents**. Not a summary of
Camel's manual — a guide to the decisions you actually face when laying out routes: what a stage
should own, where a boundary belongs, what each construct commits you to.

The guide lives as an artifact, not in this repo — *Where to Cut a Camel Application*:

**https://claude.ai/code/artifact/60ab4b26-fd17-4659-b11c-8b2eb9f12ec5**

To update it, pass that URL as `url` when publishing. Publishing without it forks a new artifact and
loses the thread. Read it first — it is the current state of the work.

**The guide is organised around the cut**, not around topics. This was chosen deliberately so that
new topics cost a *column*, not a *part*:

1. **The cut** — the spine, eight numbered questions: should it be a route, who is waiting, should
   it own its errors, does a caller need to recover, what is the transaction boundary, does
   splitting split the commit, where does the thread change, what happens when it is too slow.
   **This is where the advice lives** — mechanism is cited from here, never restated.
2. **What each cut costs you** — the matrix. Rows are cuts (`direct:`, `.transacted()`, `seda:`,
   broker consumer, HTTP edge, `.circuitBreaker()`, outbox+relay); columns are topics (error
   ownership, transaction, retry, threading, backpressure). **Add a topic by adding a column
   here.** A construct earns a *row* only if work passes through it and it has an answer in several
   columns — a bulkhead is a cell of the `.circuitBreaker()` row, and a connection pool is not in
   the matrix at all because the route cannot see it.
3. **Mechanism** — reference, consulted not read. Gates, stamps, the construct table, claiming,
   retry scoping, thread-boundary numbers, the HTTP body. New topics add sibling sections, and it
   is fine for this part to be exhaustive because nobody reads it top to bottom.
4. **Work no cut covers** — ordering an outside effect against the commit, what changes when that
   effect is itself the downstream trigger, where compensation can hang, sync versus deferred, when
   a handoff has to be durable, and large streaming transfers (explicitly marked *not measured*).

**Every matrix cell is marked `probe` / `source` / `not measured`.** Never fill a cell by
reasoning — mark it `not measured` and add a probe to the queue. Two rows (broker consumer, and
most of HTTP edge) are nearly empty on purpose.

We started with **error handling** because it turns out to drive route layout more than anything
else: the error constructs and the stage boundaries are the same decision viewed twice. Part three
follows from the same place — once a stage's failure behaviour is settled, what is left is the work
the boundary never covered. Other topics (routing patterns, threading and SEDA, idempotency,
testing) come later.

The guide is aimed at 2026 application shapes — services, serverless, model calls — and is
deliberately **not** exhaustive. Prefer the decision a reader actually faces over coverage.

## Why this project exists

The work started inside a Quarkus/Camel application where `.transacted()` was found to be committing the
writes it was supposed to roll back. Diagnosing it took far too long, for two reasons worth avoiding
here: every experiment needed Testcontainers (slow, and it repeatedly broke), and the reasoning
outran the evidence. Several confident claims turned out to be wrong under measurement.

So this project is deliberately **container-free** — a probe is a JUnit test against H2 in memory
and runs in about a second.

```bash
mvn clean test              # all probes, ~30s — CHECK THE EXIT CODE, not the output
mvn test -Dtest=OwnershipProbe
```

**Never judge a run by grepping for "FAIL".** A compile error leaves the previous
`target/surefire-reports/*.txt` in place, so a grep reads stale green results from the last
successful build. This happened once and hid a broken `@Override` for several turns. Check
`echo $?` after `mvn clean test`, or look for `BUILD SUCCESS`.

Camel is pinned to **4.18.0** in `pom.xml`, matching the application. Keep it that way; findings that
don't transfer are worse than no findings.

## How to work here

**Measure, then claim.** The single biggest lesson from the prior session: plausible reasoning about
Camel's error handling is wrong about as often as it's right. If a statement is going into the
guide, there should be either a passing probe or a source citation behind it. When neither exists,
say so in the guide.

**Keep `pollTimeout=100` on every `seda:` consumer URI in a probe.** `SedaConsumer` polls with a
1s timeout, and shutdown waits for that poll on every endpoint — which cost ~4s per test method
before it was set, and dominated the whole suite. It affects teardown only, never a finding.

**`ProbeSupport.createDataSource()` is overridable** so a probe can supply a bounded pool
(`TransactedBackpressureProbe` uses HikariCP with `maximumPoolSize=2`). The default stays an
unpooled H2 `JdbcDataSource`.

**Probes are named `*Probe`** (surefire is configured for it) and read as a claim: the test name
states the finding, the assertion message says why it matters. `ProbeSupport` gives you an H2
database, a real `.transacted()` boundary, `rows()`, and `stamps(exchange)` — which prints the four
pieces of state in the guide's vocabulary rather than Camel's internal names.

**Cite source at the `camel-4.18.0` tag.** Sources jars are in `~/.m2`; unzip and read them rather
than guessing. Verify any URL before putting it in the guide — a first pass of doc links from memory
was mostly 404s.

## What is already established

Verified by probe here, by source reading at 4.18.0, or by tests against a real deployment. Do not
re-derive these; do challenge them if something contradicts them.

| Finding | How it was established |
|---|---|
| Two independent gates: routing continues on `isFailed \|\| isRollbackOnly \|\| isRollbackOnlyLast \|\| errorHandlerHandled`; the transaction rolls back on `exception != null \|\| isRollbackOnly`. `errorHandlerHandled` is in the first and not the second — that asymmetry is the whole bug. | `PipelineHelper.continueProcessing`, `TransactionErrorHandler` |
| `handled(true)` alone returns a mapped response **and commits** the writes made before the failure. Adding `.markRollbackOnly()` last keeps the response and rolls back. | `TransactionProbe` here, plus the application's own regression test |
| A callee that owns the error decides the outcome; the caller's clause never fires and the caller does **not** resume after the call. | `OwnershipProbe` here |
| The "claimed" stamp is **not** caused by a clause. `setFailureHandled` is the first statement in `prepareExchangeAfterFailure`, before any handled/continue branching, so **a route declaring nothing at all claims its failures too** — claimed and unhandled at once, exception still live, caller's clause skipped. Clauses only decide what the claim produces. `noErrorHandler()` is genuinely the only way to decline: it is a pass-through that never reaches that code. | `DefaultHandlerProbe`; `RedeliveryErrorHandler.prepareExchangeAfterFailure`; `ExchangeHelper.setFailureHandled` |
| The default error handler also owns **redelivery** with no clauses registered: a callee under `defaultErrorHandler().maximumRedeliveries(2)` retries twice before the caller learns anything. Out of the box the policy is 0 redeliveries, so it looks inert until someone configures it. | `DefaultHandlerProbe` |
| **A transacted route cannot decline.** `.transacted()` installs the transaction error handler and it wins over `errorHandler(noErrorHandler())` on the same route — the failure is claimed and the caller's clause never fires. Declining is only available to a stage that is not a transaction boundary. | `TransactedDeclineProbe` |
| Redelivery is scopeable at four levels, which **override** rather than stack: builder handler, route handler (`from(…).errorHandler(…)`), exception clause (`onException(X).maximumRedeliveries(n)`), and predicate (`onException(X).onWhen(pred)` — two clauses for the same type, chosen per occurrence). | `RedeliveryScopeProbe` |
| **A clause with steps in it silently resets `maximumRedeliveries` to 0.** `ExceptionPolicy.createRedeliveryPolicy` copies the parent policy and zeroes it when `hasOutputs`; only a clause with no outputs inherits. So `onException(X).handled(true)` keeps the handler's retries and `onException(X).handled(true).setBody(…)` does not — adding one step turns retry off without mentioning retry. | `RedeliveryScopeProbe`; `ExceptionPolicy.createRedeliveryPolicy` |
| **A redelivery re-invokes only the step that threw**, not the route. A route whose second processor fails three times runs its first processor once. Work before the failing step is neither repeated nor undone by a retry. | `RedeliveryUnitProbe` |
| `doTry`/`doCatch` **does** reach a failure from a transacted callee — the caller resumes after `end()`, and the callee's transaction still rolls back. This is the only recovery available against a stage that cannot decline. | `TransactedCatchProbe` |
| `onCompletion` scoping, three findings: a route-scoped hook fires for **every route the exchange visited** (innermost first); all hooks run after the **whole** routing, not per route, because they hang off the unit of work; and a route-scoped hook **replaces** the builder-scoped one for that route — two routes with their own hooks means a builder-wide hook never fires. | `OnCompletionScopeProbe` |
| For HTTP there is **no setting that returns your own body on a failed exchange**. `muteException` defaults to **true** (empty body); setting it false sends the **stack trace**. Both force `text/plain`, and the message body is only read when no exception is present. | `VertxPlatformHttpSupport.handleExceptions`; `PlatformHttpEndpoint.muteException` |
| **`shouldWrapInErrorHandler` is the unifying rule**: no error handler is installed inside `doTry`/`doCatch`/`doFinally`, inside `onException` clause bodies, inside `.circuitBreaker()`, or on `multicast()` children — parent chain included. This single method explains three findings measured separately here: redelivery cannot re-enter a breaker, a throwing compensation inside a clause is unhandled and replaces the original failure, and a throw inside `doCatch` reaches no clause. | `ProcessorDefinitionHelper.shouldWrapInErrorHandler`; `CircuitBreakerRetryProbe`, `ClauseProbe`, `CatchRethrowProbe` |
| On a **transacted** route only the **per-clause** redelivery policy survives: builder-scoped and route-scoped handlers are replaced by the transaction handler, whose own policy defaults to 0. Measured under camel-jta: 1 / 1 / 3 attempts for builder / route / clause scope. | `JtaTransactedRetryProbe` |
| Under **camel-jta each retry is its own transaction** (3 attempts → 3 rollbacks); under Spring all attempts share one. Both halves now measured rather than one being a javadoc claim. | `JtaTransactedRetryProbe`, `TransactedRetryProbe` |
| **Two independent facts govern a throw from inside a `doCatch`, and conflating them is easy.** The *wrapping* rule decides whether **this route's** clauses fire — never, inside a catch. The *claim* decides whether **anyone else's** do. So a rethrow after an inline failure escapes unclaimed and **the caller's clause maps it normally**; a rethrow after a *called route* failed does not, because that route claimed and every later handler treats the exchange as finished. Same code, opposite outcome, decided by whether the thing inside `doTry` was inline or a route. | `CatchEscapeProbe`, `CatchRethrowProbe` |
| A `direct:` call stays on the caller's thread; `.threads()`, `wireTap` and a queue endpoint each move to another one. | `ThreadingProbe` |
| **`.threads()` is a silent no-op inside a transacted route.** `ThreadsProcessor.process` returns immediately when `exchange.isTransacted()` — "the transaction manager doesn't support using different threads in the same transaction". Measured: the step after the hop runs on the same thread and both writes stay in one transaction. Atomicity is safe; the concurrency you asked for is simply absent. **`seda:` is not suppressed this way** — it is a real endpoint and does leave both the thread and the transaction. | `ThreadingProbe`; `ThreadsProcessor` javadoc + `process` |
| Camel's default thread pool profile: poolSize **10**, maxPoolSize **20**, maxQueueSize **1000**, keepAlive 60s, `allowCoreThreadTimeOut(true)`, rejectedPolicy **CallerRuns**. | `BaseExecutorServiceManager` ctor |
| `seda:` defaults: `size=1000`, `concurrentConsumers=1`, `blockWhenFull=false`, `discardWhenFull=false`. | `SedaEndpoint`, `SedaConstants.QUEUE_SIZE` |
| `camel-platform-http-vertx` runs route processing inside `vertx.executeBlocking(…, false)`, so a route gets a **Vert.x worker thread** — measured as `vert.x-worker-thread-0`, never an event loop. It is the *shared* worker pool, so it bounds you without isolating you. | `HttpConsumerProbe`; `VertxPlatformHttpConsumer:327` |
| At an HTTP edge the status survives a failed exchange and the body does not — measured end to end: `409` arrives, body is empty, `Content-Type` forced to `text/plain`. With `handled(true)` the same body arrives intact. With `muteException=false` the body is the **stack trace**, never yours. | `HttpConsumerProbe` |
| Backpressure at a full `seda:` queue, all three measured: default **throws** `IllegalStateException` at the producer; `blockWhenFull` **parks** the producer until space appears (no timeout); `discardWhenFull` **drops silently**. | `BackpressureProbe` |
| A task rejected by a `.threads()` pool runs on the **caller's thread** — Camel's default `rejectedPolicy` is `CallerRuns`, so the hop silently does not happen. | `BackpressureProbe` |
| `.circuitBreaker()` with `onFallback` **claims** the failure (a builder clause never sees it) but **routing resumes after `end()`** — it owns like a callee and returns like a `doCatch`. | `CircuitBreakerProbe` |
| The breaker is a **semaphore, not a pool**: by default the call runs on the caller's thread, no hop. Enabling `timeoutEnabled` brings Resilience4j's TimeLimiter, which **does** hop — a config flag that reads like a timeout is a thread boundary. | `CircuitBreakerProbe` |
| **`timeoutEnabled` inside `.transacted()` silently splits atomicity.** Camel suppresses `.threads()` for a transacted exchange; nothing suppresses Resilience4j's hop. Measured: the write inside the breaker committed on another thread while the write before it rolled back. | `CircuitBreakerProbe` |
| Camel's Resilience4j defaults: `bulkheadEnabled=false`, `bulkheadMaxConcurrentCalls=25`, **`bulkheadMaxWaitDuration=0`**, `timeoutEnabled=false`. A full bulkhead therefore sheds immediately into the fallback rather than queueing — **the source doc's claim that the default queues is wrong at 4.18.0**. | `CircuitBreakerProbe`; `Resilience4jConfigurationCommon` |
| **On a plain (non-transacted) JMS consumer an unhandled failure is acknowledged and LOST** — one delivery, no redelivery, and it never reaches the DLQ. Redelivery is a property of the **session**, not of how the failure was treated. This corrected the guide, which had claimed leaving a failure unhandled produces redelivery. | `BrokerProbe` |
| With `transacted=true`: a live exception → redelivered to the policy limit → DLQ. `handled(true)` → acknowledged and dropped after one delivery. `handled(true)` + `markRollbackOnly()` → redelivered → DLQ, so the rollback and the retry really are one event. | `BrokerProbe` |
| `concurrentConsumers` **is** the concurrency bound: with 3 and six messages queued, exactly 3 were in flight, on 3 distinct Camel threads. A route has no concurrency of its own. | `BrokerThreadingProbe` |
| **A slow broker consumer backs up in the broker, not in the sender.** 200 sends completed without blocking or throwing while the consumer had processed one — where `seda:` refuses at its in-process limit. Nothing pushes back until the broker runs out of room. | `BrokerThreadingProbe` |
| With **producer flow control** on a destination, the broker's backpressure arrives as a **parked send** — the same shape as `blockWhenFull`, and the same risk of holding a thread that cannot afford it. | `BrokerThreadingProbe` |
| Whether a `seda:` consumer's failure reaches the sender is a **producer option**: default fire-and-forget hides it, `waitForTaskToComplete=Always` brings the exception back. Even then the consumer's handler has **claimed** it, so the sender is stopped rather than given a decision — unless the consumer uses `noErrorHandler()`. | `SedaSemanticsProbe` |
| A failed `seda:` message is **not redelivered** — no session, nothing to un-acknowledge. One delivery and it is gone, with no transacted option to switch redelivery on and no DLQ unless one is built from `errorHandler(deadLetterChannel(...))`. | `SedaSemanticsProbe` |
| **The HTTP edge has no backpressure of its own.** 40 concurrent requests against a slow route gave exactly **20** concurrent executions — the Vert.x worker pool default — and the excess 20 were neither refused nor errored. They queued invisibly and all eventually returned 200. The pool size is the concurrency of the whole edge and nothing in the route declares it. | `HttpBackpressureProbe` |
| **A `.transacted()` boundary takes a connection when it opens, not at the first statement.** A transacted route issuing *no SQL at all* still ran only `poolSize` copies concurrently; the excess failed on the pool's connection timeout. So a slow HTTP or model call inside a transacted route holds a database connection for its whole duration. Backpressure at a transaction boundary is the pool, and it surfaces as the pool's exception. | `TransactedBackpressureProbe` |
| **Camel redelivery does not re-enter a `.circuitBreaker()` block.** Identical failure, identical handler: retried 3× in a control route, attempted **once** inside the breaker. `ResilienceProcessor` sets the exception on the exchange and returns instead of throwing through the channel. `maximumRedeliveries` therefore applies everywhere in a route except inside a breaker, silently. | `CircuitBreakerRetryProbe`; `ResilienceProcessor` |
| **A caller's retry DOES re-enter a breaker in a callee** — 3 attempts became 3 breaker calls, each a separate recorded failure. Redelivery cannot re-enter the block; it can re-enter the route around it. So where the retry sits decides whether it is invisible to the breaker or triples the failure rate it opens on. | `CircuitBreakerRetryProbe` |
| Adding `onFallback` turns retry off as a side effect — the fallback clears the exception, so the error handler sees a success. Same shape as giving an `onException` clause a body. Once the breaker is open the body is not called at all and the fallback answers every caller. | `CircuitBreakerRetryProbe` |
| `waitForTaskToComplete` decides whether a `seda:` send blocks: `IfReplyExpected` (the default) waits **only for an out-capable exchange**, so the same DSL is a dispatch in one route and a blocking call in another. When it waits it enqueues a *copy*, blocks on a latch (30s default → `ExchangeTimedOutException`), then `copyResults` puts body/headers/exception back on the caller's exchange. | `SedaProducer.process`; `SedaSemanticsProbe` |
| `transacted=true` on a JMS endpoint makes Camel create a **`JmsTransactionManager`** over the session (`JmsConfiguration:1790`) — a *local JMS transaction covering acknowledgement only*. Not the route-level `.transacted()`, not the database, not XA. Say "when the message is acknowledged", never "the session". | `JmsConfiguration` |
| `doTry`/`doCatch` is reachable on a failed exchange because `TryProcessor.continueRouting` checks only `isRouteStop()` and `hasNext()` — so a callee using `handled(false)` **can** be caught by a caller. A callee using `handled(true)` cannot: `CatchProcessor` exits when there is no exception. | `TryProcessor`, `CatchProcessor` |
| `continued(true)` resumes inside the failing route and **clears any rollback mark** (`prepareExchangeForContinue`). It is route-scopeable, not builder-only — `ProcessorDefinition.onException` exists. | source |
| `.markRollbackOnly()` is exactly `exchange.setRollbackOnly(true)` and an immediate return. | `RollbackProcessor.process` |
| For HTTP: the status survives a failed exchange because it is a header; the body does not, because `getBody` returns `""` and forces `text/plain` whenever an exception is present. | `VertxPlatformHttpSupport.getBody` |
| `handled(true)` clears the exception, so the unit of work sees a **success**: `onCompletion().onCompleteOnly()` fires and `onFailureOnly()` does **not** — even with a rollback mark set and the transaction rolling back. The hooks read `isFailed()` only; the mark is never consulted. | `CompensationProbe`; `OnCompletionProcessor.shouldSkip` |
| `onCompletion` runs **after** the commit — a hook on a successful transacted route reads its row through a fresh connection. | `CompensationProbe` |
| A queued handoff (`seda:`, and by the same mechanism any broker send) is **not in the transaction**: it is delivered even when the sender rolls back, and can be read by the receiver *before* the sender commits. | `AsyncHandoffProbe` |
| A row written to an outbox table in the same transaction as the work disappears with it on rollback. One resource manager, so the trigger and the work cannot disagree. | `AsyncHandoffProbe` |
| A transacted caller **absorbs** a transacted callee under the default propagation — one transaction, so breaking a stage into its own route buys no isolation. | `NestedTransactionProbe` |
| `markRollbackOnly()` sets the mark on the **exchange**, which is shared across `direct:` boundaries. So it rolls back an enclosing transaction too, *even with* `PROPAGATION_REQUIRES_NEW`. Isolating a callee takes `REQUIRES_NEW` **and** `markRollbackOnlyLast()`. | `NestedTransactionProbe`; `RollbackProcessor.process` |
| A **route-scoped** `onException` beats a builder-scoped one for the same exception type; a route without its own clause still falls back to the builder's. | `ClauseProbe` |
| Exchange **properties survive** into the clause, so a route can record what it did (`uploadedKey`) and the clause can undo exactly that much — absence of the property is how it tells "failed before the upload" from "failed after". | `ClauseProbe` |
| If the compensation inside a clause **throws**, the clause pipeline halts there: `markRollbackOnly()` and the response body never run. The transaction **still rolls back** (the new exception satisfies gate 2 alone), but the mapped response is lost and the exchange now carries the *compensation's* exception — the original failure is overwritten. | `ClauseProbe` |

## Known fidelity gaps

Fix or flag these before leaning on them.

1. **Transaction manager differs — now diffed AND probed, and the difference is bigger than the
   rollback condition.** See `JtaFidelityProbe` and the CONTRADICTION note below before trusting
   anything on this page about `handled(true)` under JTA. This sandbox uses
   Spring's `DataSourceTransactionManager` via `camel-spring`; the application uses `camel-jta` with
   Narayana. Two differences that matter:

   - **Redelivery nests the other way round.** Spring's `TransactionErrorHandler` *is* a
     `RedeliveryErrorHandler`, so retries happen **inside** one transaction. The application's
     `JtaTransactionErrorHandler` is a `RedeliveryErrorHandler` that *wraps* an inner
     `TransactionErrorHandler`, so **each retry gets a fresh transaction**. Camel's own javadoc
     gives the reason: "every error breaks the current transaction". Any finding about retry and
     transaction interaction must be stated for the application's nesting, not this one's.
   - **`isRollbackOnlyLast()` is in Spring's rollback condition and absent from JTA's.** Spring
     rolls back on `exception != null || isRollbackOnly() || isRollbackOnlyLast()`; JTA on
     `exception != null || isRollbackOnly()`. So `markRollbackOnlyLast()` after a `handled(true)`
     (which cleared the exception) is expected to **commit** under Narayana while it rolls back
     here. `NestedTransactionProbe#requiresNewPlusRollbackOnlyLast_isolatesTheCalleeProperly`
     carries this caveat and should not be quoted for the application without a test there.

   Gate 2 as stated in the table above — `exception != null || isRollbackOnly` — is the **JTA**
   condition, so it is right for the application; this sandbox is merely more eager to roll back.
   Note also that the class the application actually instantiates is `JtaTransactionErrorHandler`, not
   the `TransactionErrorHandler` sitting beside it in the same package.
2. ~~**No HTTP consumer is wired.**~~ **Closed.** `camel-platform-http-vertx` is now a test
   dependency and `HttpConsumerProbe` binds a real port in-JVM (no container). The status/body
   behaviour, `muteException`, and the worker-thread question are all measured.
3. ~~**No broker.**~~ **Closed.** `camel-activemq` + `activemq-broker` 6.3.1 (Jakarta) are test
   dependencies and `BrokerProbe` runs an in-JVM `vm://` broker with a short redelivery policy, so
   redelivery and the DLQ are measured. Note the version pin: `camel-activemq` 4.18.0 brings
   `activemq-client-jakarta` 5.19.1, which is excluded in favour of ActiveMQ 6.3.1 client+broker.
   `BrokerThreadingProbe` builds a `BrokerService` explicitly so destination policy (memory
   limit, producer flow control) can be set; `BrokerProbe` uses the simpler auto-created `vm://`.

## Incoming source material

`~/.claude/uploads/…/b1d2f394-camelthreadsandbulkheads.md` — "Threads, Pools and Bulkheads in
Camel", written before this project and **partially validated** (2026-08-24). Everything checkable
in it held up: the 10/20/1000 defaults, CallerRuns, the seda defaults, and platform-http-vertx's
`executeBlocking`. Its one real gap is that `.threads()` is suppressed inside a transaction, which
changes its "When to hop" advice. Since probed: `timeoutEnabled` does reintroduce a thread hop (**confirmed**), and
`bulkheadMaxWaitDuration` defaults to **0**, not to queueing (**the doc is wrong there**). Still
unverified in it: the blocking/non-blocking component lists and the CAMEL issue number. Its sizing formulas are arithmetic, not Camel behaviour — mark them as reasoning, not
measurement, if they go in the guide.

## The commit-on-failure bug, fully characterised (2026-08-25)

Two independent variables: **which transaction manager**, and **which route threw**. All measured.

| Thrown | Spring | camel-jta |
|---|---|---|
| inside the transacted route | clause runs first → `handled(true)` **commits** (the original bug) | transaction completes first → **rolls back** |
| in a non-transacted callee reached from inside | **commits** | **commits** |
| either, plus `markRollbackOnly()` | **rolls back** | **rolls back** |

Row 1 differs because the handlers are wired opposite ways. Spring's `TransactionErrorHandler`
*extends* `RedeliveryErrorHandler`, so clause dispatch is inside the transaction template
(`SpringOrderingProbe`: `["clause-ran","COMMITTED"]`). `JtaTransactionErrorHandler` *wraps* an inner
`TransactionErrorHandler`, so the transaction completes first (`JtaFidelityProbe`:
`["ROLLED_BACK","clause-ran"]`).

**The founding incident was row 2, not row 1.** The application runs camel-jta, where row 1 rolls
back — so the original bug could only ever have been the boundary-crossing case, and an independent reviewer
confirmed exactly that. Note the consequence for the sandbox: `TransactionProbe`, the first probe
written here, reproduces row 1 on **Spring**. It demonstrates the same symptom by a different
route, and is not a reproduction of the incident. That is fine for illustrating the mechanism, but
do not cite it as the application's bug.

Row 2 is the same on both and has nothing to do with transactions: the callee's own error handler
claims and clears the failure before control returns. **This is the row a refactor creates** —
factoring a stage into its own untransacted route moves its failures into a different error
handler. Confirmed against the application by an independent reviewer, where the S3 upload sits in a separate
non-transacted route reached by `.choice()`.

The mark works everywhere because it writes to the **exchange**, which survives all of these paths.

**It was never about `handled(true)`.** Anything that clears the exception before the boundary
looks has the same effect, and three constructs do — measured inside a transacted route, each
commits the work written before the failure: `handled(true)`, a circuit breaker's `onFallback`, and
`doCatch` (`CircuitBreakerProbe`). Two of those are reached for to make a route *more* robust.

Also measured: the bug is only *observable* when the failure is non-resource — a SQL error has
already poisoned the transaction. Of the application's three test cases, two fail via CHECK constraints
and passed even unfixed.

## Open questions worth a probe

- Every component row in the matrix is now measured in every column. The only `not measured` cells
  left are the four on `outbox + relay`, which is a pattern rather than a construct — there is no
  single thing to point a probe at, and the answers depend on how the relay is built.
- The remaining structural gap is **JTA/Narayana**, not coverage: two findings still carry explicit
  "does not transfer" caveats and cannot be settled in this sandbox.


- **Known-and-left**, so nobody re-derives them: a bulkhead's permits against a caller's retry is
  stated in the guide as an inference from the measured route-versus-block rule, not probed —
  each retry re-enters the route and therefore takes another permit. And nothing measures a
  breaker's effect on a transaction beyond `timeoutEnabled` and the `onFallback` commit above.
  Both look like diminishing returns rather than gaps.
- ~~camel-jta's per-attempt transaction claim.~~ **Measured** (`JtaTransactedRetryProbe`): three
  attempts, three rollbacks. Spring shares one transaction across all attempts.
- ~~`continued(true)` and rollback marks.~~ **Measured** (`ContinuedProbe`): it does erase a mark,
  but only when the mark and the failure come from the **same step** — a mark set earlier halts
  routing at the next step, so nothing throws and no clause fires.
- Does the application's per-retry-a-fresh-transaction nesting change what a clause sees on the second
  attempt? Not reproducible here; needs a probe against `camel-jta`.
- Is there a hook that fires on **failure including a mapped one**? `onFailureOnly` is out (it reads
  `isFailed()`), and `onCompleteOnly` fires on rollbacks. Today the only reliable answer is to do
  compensation inside the clause itself, before `markRollbackOnly()`.

## Guide conventions already chosen

Keep these unless there is a reason not to; they were argued over.

- **Plain names, not Camel's internals.** The guide says *claimed* (`failureHandled`), *handled*
  (`errorHandlerHandled`), and *rollback mark* (`rollbackOnly`). Internal names appear once, in an
  appendix. Nobody remembers `errorHandlerHandled` a week later.
- **Two words to keep honest.** Never write "session" for JMS acknowledgement — say *when the
  message is acknowledged*, and say *local JMS transaction* when that is what it is, so it is never
  confused with `.transacted()`. And never write "consumer" bare: it means either the `from(...)`
  end of a route or the broker's subscription, so write *the consuming route* or *the broker*.
- **The table is keyed by what you type**, not by flag. A reader arrives knowing they wrote
  `handled(true)` and wants to know what it cost them.
- **Structure encodes something true.** Part one is what the machine does; part two is how to
  decide. The numbered questions in part two are a real sequence — the answer to each changes the
  next.
- An error handler on a reusable stage is framed as a **declaration**: it claims the right to
  interrupt the consumer, and callers must respect it or defend against it.
- **Do not oversell decline-by-default.** The guide argues that helper routes should decline so a
  refactor cannot silently change who owns a failure — but a transacted route *cannot* decline, so
  the convention excludes exactly the routes where the commit-on-failure bug lives. The honest
  claim is that it shrinks the set you must think hard about down to the transaction boundaries,
  which you then reason about one at a time. Stated at the `A transacted stage cannot decline` flag.
- **The guide is product-neutral.** It names no internal class, test, repo path or product. Findings
  established against a real application go in as the finding, stated generically — a base
  `RouteBuilder` that registers clauses in `configure()`, not the class that happens to do it here.
  The sandbox's own probes may be cited by name in this file, but not on the page.
- **Beware generalising from probes that share a precondition.** The `doCatch` rule was first
  stated as "a rethrow can never be mapped" from eight tests that all threw *after a callee had
  claimed*. The unclaimed case behaves oppositely. Before generalising, ask what every test in the
  set happens to have in common that was never the variable.
- **A correction to a probe is a correction to the guide.** Probe javadoc and guide prose are
  written from the same reasoning, so when a probe's explanation is found wrong, grep the guide for
  the same wording before considering it fixed. This was missed once: the disproved
  "camel-jta is expected to commit" prediction was retired from `NestedTransactionProbe` and left
  standing in the guide.
- **Every claim carries its provenance**: reproduced by a probe, cited to a 4.18.0 source method, or
  marked *not measured*. There is no fourth category, and reasoning is not one of them.
