# camel-design-guide

A design guide for Apache Camel applications, and the probe harness that settles its claims **by
experiment**. The guide is the deliverable; the probes are the evidence for it.

**This repository is public.** The probes are still a scratch space — break them freely, they exist
to be rewritten — but `guide.md`, `README.md` and `METHOD.md` are published work that people read
and link to. Treat edits to those as edits to something shipped.

## The job

Produce a **design guide for Camel applications, aimed at both humans and agents**. Not a summary of
Camel's manual — a guide to the decisions you actually face when laying out routes: what a stage
should own, where a boundary belongs, what each construct commits you to.

**The guide lives in this repo**, as markdown, and is the source of truth:

- `guide.md` — the document.
- `README.md` — the navigator, the five per-property tables and the review card. The README is
  *not* the guide; the card exists in one place only, here, because a drifted card is worse than
  no card.
- `METHOD.md` — versions, how to run the probes, what CI protects, known gaps.
- `tools/check-links.py` — every `[probe](…)` must resolve to a file containing the test method it
  names, and every in-document anchor to a real heading. Run it after any edit; CI runs it too.

Probe links are `[probe](src/test/java/sandbox/XProbe.java "theTestMethodName")`. Link the file and
name the method — never `#L42`, which rots silently on any edit above it.

An earlier HTML artifact at
`https://claude.ai/code/artifact/60ab4b26-fd17-4659-b11c-8b2eb9f12ec5` is the pre-port version and
is **frozen** — it is still shared with people, so leave it live, but do not treat it as current and
do not sync edits into it. Decide its fate when this repo goes public.

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

**A stale class file lies about tests you did not touch.** Adding an anonymous inner class and
running `mvn test` without `clean` can leave the old class file in place — `NoClassDefFoundError`
on `Outer$1` at runtime. If that lands inside a transaction policy, the transaction is left
associated with the thread and the *next* test fails with Narayana's ARJUNA016051, so one stale
file produces failures in tests that passed minutes earlier. `mvn clean test` fixes it with no code
change. (Reported independently; the same class of trap as the stale reports below.)

**Never judge a run by grepping for "FAIL".** A compile error leaves the previous
`target/surefire-reports/*.txt` in place, so a grep reads stale green results from the last
successful build. This happened once and hid a broken `@Override` for several turns. Check
`echo $?` after `mvn clean test`, or look for `BUILD SUCCESS`.

Camel is pinned to **4.18.0** in `pom.xml`, matching the application this came from. Keep it that
way; findings that
don't transfer are worse than no findings.

## How to work here

**Measure, then claim.** The single biggest lesson from the prior session: plausible reasoning about
Camel's error handling is wrong about as often as it's right. If a statement is going into the
guide, there should be either a passing probe or a source citation behind it. When neither exists,
say so in the guide.

**Keep `pollTimeout=100` on every `seda:` consumer URI in a probe.** `SedaConsumer` polls with a
1s timeout, and shutdown waits for that poll on every endpoint — which cost ~4s per test method
before it was set, and dominated the whole suite. It affects teardown only, never a finding.

**`NarayanaRequiredPolicy` is the single JTA policy — do not copy it into a probe.** It is a
faithful copy of Quarkus's `TransactionalJtaTransactionPolicy`, which is what makes the JTA probes
say anything about a real JTA deployment, and it takes optional sinks for the completion outcome
and the
error-handler frames. Three probes previously kept private copies; when one was found to leak a
transaction on a setup failure, the fault had already been copied twice — the fidelity everyone
wanted came bundled with scaffolding nobody re-read. Shared helpers are usually the wrong instinct
in this repo (a probe should read as a self-contained claim), and this is the exception: subtle
lifecycle code whose correctness is not the subject of any probe.

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

Verified by probe here, by source reading at 4.18.0, or against a real deployment. Do not
re-derive these; do challenge them if something contradicts them.

| Finding | How it was established |
|---|---|
| Two independent gates: routing continues on `isFailed \|\| isRollbackOnly \|\| isRollbackOnlyLast \|\| errorHandlerHandled`; the transaction rolls back on `exception != null \|\| isRollbackOnly`. `errorHandlerHandled` is in the first and not the second — that asymmetry is the whole bug. | `PipelineHelper.continueProcessing`, `TransactionErrorHandler` |
| `handled(true)` alone returns a mapped response **and commits** the writes made before the failure. Adding `.markRollbackOnly()` last keeps the response and rolls back. | `TransactionProbe` here, plus a regression test against a real deployment |
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
| On a **transacted** route what survives is **being a clause**, not where it is declared. Measured under camel-jta with the same `maximumRedeliveries(2)` in four places: builder handler **1**, route handler **1**, builder clause **3**, route clause **3**. Both handler scopes are replaced by the transaction handler (own policy defaults to 0) and fail silently; both clause scopes are still consulted. A route-scoped clause must be declared before `.transacted()`. | `JtaTransactedRetryProbe` |
| Under **camel-jta each retry is its own transaction** (3 attempts → 3 rollbacks); under Spring all attempts share one. Both halves now measured rather than one being a javadoc claim. | `JtaTransactedRetryProbe`, `TransactedRetryProbe` |
| **Swallowing a failure to keep the exchange clean is a TRAP, not a technique.** A stage that catches its own failure internally does come back unclaimed — because nothing failed as far as the boundary is concerned, so a transacted stage doing this **commits** the work it meant to undo. Commit-on-failure in a different outfit. Kept as a probe because it is an attractive-looking wrong answer. | `ContainedFailureProbe` |
| **`doCatch` is already SimpleTask-shaped.** `CatchProcessor:151-153` sets `EXCEPTION_HANDLED`, records `EXCEPTION_CAUGHT` and clears the exception — the same bookkeeping `SimpleTask.prepareExchangeForContinue` does. So a catch needs only the *flags* cleared, not the bookkeeping redone. But SimpleTask is still the wrong donor: it never clears `failureHandled` (so it would not restore mappability at all) and it sets `errorHandlerHandled(true)`, which `PipelineHelper.continueProcessing` treats as **stop**. | `CatchProcessor`, `PipelineHelper.continueProcessing` |
| **There are TWO `prepareExchangeForContinue` methods in `RedeliveryErrorHandler` and they disagree.** `SimpleTask`'s removes `EXCEPTION_CAUGHT` and sets `errorHandlerHandled(true)` while never clearing the claim; `RedeliveryTask`'s clears the claim and leaves the verdict alone. Only the latter can apply to `continued(true)`: `simpleTask` is chosen only when `exceptionPolicies` is empty, and a continued clause is itself a policy. Cite the `RedeliveryTask` one. | `RedeliveryErrorHandler:754` vs `:1234`, selection at `:1983` |
| **A catch SNAPSHOTS `routeStop`, `rollbackOnly` and `rollbackOnlyLast` on entry and RESTORES them in its callback** (`CatchProcessor:134-139`, `:173-181`). Three measured consequences: the rollback mark reads `false` inside the body and is true again after `end()`; clearing it from inside is discarded on the way out; and a marked exchange **stops routing at the first step after `end()`**, so a repair placed there is never reached. Plus a sharper case — on a marked exchange a caught `RollbackExchangeException` is re-set on the exchange after the body. `failureHandled` and `errorHandlerHandled` are in neither snapshot, which is why they are the two a repair can clear. | `CatchRestoreProbe`; `CatchProcessor` |
| **A mark set inside a `doTry` without a throw leaves only `doFinally` running.** The try body halts at the step after the mark, so nothing throws; each `doCatch` is entered and exits at `CatchProcessor:124` on `caught == null`; the finally runs; routing then stops after `end()`. So a thrown abort is compensated in a clause and a marked abort in `doFinally` — two paths, neither covering the other. | `CatchRestoreProbe` |
| **`shouldWrapInErrorHandler` excludes the `doTry`/`doCatch`/`doFinally` DEFINITIONS themselves, not only their children** (`definition instanceof TryDefinition` is the first branch). So a failure escaping a `doTry` block never reaches **its own route's** clauses — only a calling route can map it. This is why `CatchEscapeProbe`'s finding is phrased as *the caller's* clause. | `ProcessorDefinitionHelper.shouldWrapInErrorHandler`; `ContinuePlacementProbe` |
| **A clause's continue clearing is SPLIT around its own body.** Before the body (`:1454-1462`, the `handleOrContinue` block): rollback mark, redelivery headers, `redeliveryExhausted`. After it, in the failure processor's done callback (`afp.process(exchange, sync -> prepareExchangeAfterFailure(...))` at `:1530-1534`): the claim is *set* at `:1615` and then cleared at `:1249`, and the exception cleared. So a clause body **always runs unclaimed** — not because continue cleared anything, but because nothing has claimed yet. That is also why `continued(true)` stacks. | `ContinuedProbe`; `RedeliveryErrorHandler` |
| **A throw inside a `continued(true)` clause body cancels the continue entirely.** `FatalFallbackErrorHandler:133,143` shadows `EXCEPTION_CAUGHT` with the new exception and pins `errorHandlerHandled(false)`; `prepareExchangeAfterFailure`'s `alreadySet` branch at `:1617-1630` then restores it and **returns before `shouldContinue` is consulted**. The route does not resume and the compensation's exception replaces the original — identical to `handled(true)`, no special protection. This is a *reachable* instance of the `alreadySet` short-circuit, but not the candidate ticket's reproducer: here the pin comes from the same failure's own clause failing, which is defensible. | `ContinuedProbe`; `FatalFallbackErrorHandler` |
| **A `doTry` nested inside a `doCatch` needs `.endDoTry()` then `.end()`, and the outer block then needs TWO `.end()` calls.** Getting it wrong is silent — no build or run error. One `.end()` after the nested `doCatch` closes only that catch, leaving you inside the nested try; the next step joins that block and the following `.end()` closes the nested try instead of the outer one, reparenting the rest into the outer catch body. It cannot be caught by asserting the failure path, because everything in a catch body runs there under either parenting; only a **success**-path assertion distinguishes them. Reported independently (nine failing tests, none near the cause); reproduced here, where it had silently affected this probe's own route. | `ContinuePlacementProbe` |
| **Placement within the catch body is a developer choice and decides coverage.** Measured: with the reset first, a compensation route called from the body has its own clause fire, and a throw from the body escapes unclaimed for a *calling* route to map; with it last, that clause never fires, the compensation's failure escapes unmapped, and the reset itself never runs. Four shapes, all measured and now in the guide: ignore (alone or first); compensate-and-give-up (last); compensate-and-handle-by-hand (last + nested `doTry`/`doCatch`, and routing does carry on to a reset placed after the nested block); compensate-via-a-route-whose-handlers-must-fire (first + nested guard). Only the fourth is impossible in the other position. | `ContinuePlacementProbe` |
| **The two flags gate different machinery.** *Claimed* (`failureHandled`) gates the **error handler** — `isDone` at `:309-311` treats a claimed exchange as finished, so no clause is dispatched. *Handled* (`errorHandlerHandled`) is a term in the **pipeline's** between-steps gate (`PipelineHelper:41`), so it stops the route advancing. That is why they poison differently: claimed suppresses mapping, handled suppresses routing. | `RedeliveryErrorHandler`, `PipelineHelper` |
| **A compensation route that `handled(true)` ends the caller from the point of the call.** With the reset first its clause does fire — but `handled(true)` sets *handled*, which the pipeline gate reads, so the step after the call **inside the catch body** does not run either, nor the step after `end()`. The exchange ends clean carrying that clause's body, so at an edge the compensation's error body ships as the response. Consequence: a catch body may only call compensation routes that do **not** handle (no clauses, `noErrorHandler()`, `handled(false)`, a rethrow, or `continued(true)` are all fine) — and that is a property of the callee the caller cannot defend against. | `ContinuePlacementProbe` |
| **Route state written INSIDE a catch body survives only when the failure arrived clean.** The restore block is guarded on the snapshot having carried something, and then writes all three fields back from it — so `markRollbackOnly()` or `.stop()` in a catch body stands in the ordinary case and is silently discarded when the failure itself carried route state. A rethrow is the dependable way to make a catch terminal. | `CatchRestoreProbe` |
| **`MarkRecovered` is exactly two lines**, `setFailureHandled(false)` + `setErrorHandlerHandled(null)`, placed **inside** the `doCatch`. `setRouteStop(false)` and `setRedeliveryExhausted(false)` were dropped as dead: the catch clears both before the body runs and clears exhausted again afterwards. Message hygiene from `prepareExchangeForContinue` (stream cache, redelivery headers) is kept. Measured in the shape with no other answer — transacted stage throws and rolls back, caller catches, later failure maps normally on a clean exchange. Reaches into `ExchangeExtension`; intended to be replaced by an upstream lever on `doCatch`. | `MarkRecoveredProbe`, `CatchRestoreProbe` |
| **`continued(true)` can fire twice** on one exchange — it clears the claim every time. CAMEL-5139's complaint is fixed. This is why the clause system copes with repeated failures and `doCatch` does not: the reset was attached to clauses. | `ContainedFailureProbe` |
| **The cleanse works and is repeatable.** Setting exception/rollbackOnly/rollbackOnlyLast/routeStop clear, plus `setFailureHandled(false)` and `setErrorHandlerHandled(null)` and `setRedeliveryExhausted(false)`, restores a claimed exchange completely: the next stage's clause fires, body sets, exchange leaves clean — and it works between every stage, not once. Caveats: `ExchangeExtension` is not application-facing API, and it destroys the record that an earlier stage failed. | `CleanseProbe` |
| **No construct performs a full reset.** `doCatch` clears the exception only (claim kept, handled pinned `false`). `continued(true)` clears exception, rollback mark and claim — but it is a *clause*, and a claimed exchange never reaches a clause, so it can never repair inherited state. A copy sheds the claim but keeps `handled` pinned. Only clearing both flags by hand is complete. | `ErrorStateResetProbe`, `ClaimResetProbe` |
| **Two sticky flags, not one.** Clearing `failureHandled` alone lets the clause run and set the body, but `errorHandlerHandled` is *also* sticky (left `false` by the first failure), so `prepareExchangeAfterFailure` sees it already set and restores the exception — the mapped body ships with a failed exchange. Clearing **both** (`setFailureHandled(false)` + `setErrorHandlerHandled(null)`) is a complete repair: clause fires, body set, exchange clean. This is why the single-line workaround in CAMEL-5139 only half-works. | `ClaimResetProbe` |
| **An exchange copy does NOT carry the claim.** Mechanism: `failureHandled` is a plain field on `ExtendedExchangeExtension`, and `Exchange.copy()` → `newCopy()` → `new DefaultExchange(this)` builds a **fresh extension**, so the field starts `false`. Measured for `wireTap` (via `createCorrelatedCopy`) and `enrich`: both hand the downstream route an unclaimed exchange while the origin keeps its own. There is no separate "claim copier" to configure — it is shed by construction. | `ClaimResetProbe`; `AbstractExchange.copy`, `ExtendedExchangeExtension` |
| **A claim costs exactly one capability, and nothing else.** On a claimed exchange: routing still reaches the end of the route, a later `doTry`/`doCatch` still catches, `setHeader`/`setBody` still land, and the exchange ends clean if nothing is left thrown. Only *being mapped by a clause* is lost, for the rest of the exchange's life — and via `direct:` that includes the caller's later steps. The failure mode is a 500 where you meant a 409, on a route where everything else works. | `CatchRethrowProbe` |
| **`doCatch` clears the exception, not the claim.** Measured: inside the catch `exception=false, claimed=true`, and still `claimed=true` after `end()` while the route runs on. Routing resumes because the exception is gone — *claimed* is not one of gate 1's terms (`isFailed \|\| isRollbackOnly \|\| isRollbackOnlyLast \|\| errorHandlerHandled`). The claim is a permanent statement of ownership, which is why a *later* failure on the same exchange also finds every clause closed. | `CatchEscapeProbe` |
| **Two independent facts govern a throw from inside a `doCatch`, and conflating them is easy.** The *wrapping* rule decides whether **this route's** clauses fire — never, inside a catch. The *claim* decides whether **anyone else's** do. So a rethrow after an inline failure escapes unclaimed and **the caller's clause maps it normally**; a rethrow after a *called route* failed does not, because that route claimed and every later handler treats the exchange as finished. Same code, opposite outcome, decided by whether the thing inside `doTry` was inline or a route. | `CatchEscapeProbe`, `CatchRethrowProbe` |
| A `direct:` call stays on the caller's thread; `.threads()`, `wireTap` and a queue endpoint each move to another one. | `ThreadingProbe` |
| **`.threads()` is a silent no-op inside a transacted route.** `ThreadsProcessor.process` returns immediately when `exchange.isTransacted()` — "the transaction manager doesn't support using different threads in the same transaction". Measured: the step after the hop runs on the same thread and both writes stay in one transaction. Atomicity is safe; the concurrency you asked for is simply absent. **`seda:` is not suppressed this way** — it is a real endpoint and does leave both the thread and the transaction. | `ThreadingProbe`; `ThreadsProcessor` javadoc + `process` |
| Camel's default thread pool profile: poolSize **10**, maxPoolSize **20**, maxQueueSize **1000**, keepAlive 60s, `allowCoreThreadTimeOut(true)`, rejectedPolicy **CallerRuns**. | `BaseExecutorServiceManager` ctor |
| `seda:` defaults: `size=1000`, `concurrentConsumers=1`, `blockWhenFull=false`, `discardWhenFull=false`. | `SedaEndpoint`, `SedaConstants.QUEUE_SIZE` |
| `camel-platform-http-vertx` runs route processing inside `vertx.executeBlocking(…, false)`, so a route gets a **Vert.x worker thread** — measured as `vert.x-worker-thread-0`, never an event loop. It is the *shared* worker pool, so it bounds you without isolating you. | `HttpConsumerProbe`; `VertxPlatformHttpConsumer:327` |
| At an HTTP edge the status survives a failed exchange and the body does not — measured end to end: `409` arrives, body is empty, `Content-Type` forced to `text/plain`. With `handled(true)` the same body arrives intact. With `muteException=false` the body is the **stack trace**, never yours. | `HttpConsumerProbe` |
| Backpressure at a full `seda:` queue, all three measured: default **throws** `IllegalStateException` at the producer; `blockWhenFull` **parks** the producer until space appears (no timeout); `discardWhenFull` **drops silently**. | `BackpressureProbe` |
| A task rejected by a `.threads()` pool runs on the **caller's thread** — Camel's default `rejectedPolicy` is `CallerRuns`, so the hop silently does not happen. | `BackpressureProbe` |
| **There is no blocking rejection policy.** `ThreadPoolRejectedPolicy` has exactly two values at 4.18.0, `Abort` and `CallerRuns` — parking the producer until a thread frees up is available at a `seda:` queue (`blockWhenFull`) and nowhere at a thread pool. Advice to configure a `Block` policy has nothing to configure. | `RejectionPolicyProbe`; `ThreadPoolRejectedPolicy` |
| **An `Abort` pool rejection is a mappable failure, not a throw at the sender.** Camel's Abort handler checks for `Rejectable`, and `ThreadsProcessor.ProcessCall.reject` *sets* a `RejectedExecutionException` on the exchange and completes the callback — so the route's own clause maps it and an edge can answer 429. Also measured: naming a profile on `.threads().executorService("x")` carries that profile's policy across, because `ThreadsReifier.resolveRejectedPolicy` looks the ref up as a **profile**. A ref naming a bean instead falls back to `callerRunsWhenRejected`, default true. | `RejectionPolicyProbe`; `ThreadsProcessor`, `ThreadPoolRejectedPolicy.asRejectedExecutionHandler`, `ThreadsReifier:108-118` |
| **Each `.threads()` without an `executorService` builds its own pool** from the default profile — `ThreadsReifier` constructs a fresh `ThreadPoolProfile` and calls `manager.newThreadPool` per definition. Five of them is five pools nobody sized. | `ThreadsReifier:60-74` |
| **A non-blocking producer does release the caller's thread**: an `AsyncProcessor` returning `false` leaves the step after it running on the thread that completed the call, not the one that made it. This is the premise the "don't hop before an async producer" rule rests on, and it holds. | `AsyncContinuationProbe` |
| **Inside `.circuitBreaker()` it does not.** `ResilienceProcessor.processTask` runs the block through the synchronous `processor.process(copy)` and awaits it, so the caller's thread is parked for the whole call and resumes the route itself after `end()`. A permit and a thread are therefore held over the same interval, however non-blocking the component inside the block is — which kills the idea of sizing a bulkhead by dependency latency independently of thread count. | `AsyncContinuationProbe`; `ResilienceProcessor:564-593` |
| **Producer class hierarchy does not tell you whether a component blocks.** `camel-http`, `sql` and `aws2-s3` producers extend `DefaultProducer` (synchronous by construction), but `file` extends `DefaultAsyncProducer` and still blocks: it does the work and `return true`. The test is whether `process(exchange, callback)` returns **false**, not what it extends. | `HttpProducer`, `SqlProducer`, `AWS2S3Producer`, `GenericFileProducer:64-73` |
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
   Spring's `DataSourceTransactionManager` via `camel-spring`; the target application uses
   `camel-jta` with
   Narayana. Two differences that matter:

   - **Redelivery nests the other way round.** Spring's `TransactionErrorHandler` *is* a
     `RedeliveryErrorHandler`, so retries happen **inside** one transaction. The target's
     `JtaTransactionErrorHandler` is a `RedeliveryErrorHandler` that *wraps* an inner
     `TransactionErrorHandler`, so **each retry gets a fresh transaction**. Camel's own javadoc
     gives the reason: "every error breaks the current transaction". Any finding about retry and
     transaction interaction must be stated for that nesting, not this one's.
   - **`isRollbackOnlyLast()` is in Spring's rollback condition and absent from JTA's.** Spring
     rolls back on `exception != null || isRollbackOnly() || isRollbackOnlyLast()`; JTA on
     `exception != null || isRollbackOnly()`. So `markRollbackOnlyLast()` after a `handled(true)`
     (which cleared the exception) is expected to **commit** under Narayana while it rolls back
     here. `NestedTransactionProbe#requiresNewPlusRollbackOnlyLast_isolatesTheCalleeProperly`
     carries this caveat and should not be quoted for a JTA deployment without a test there.

   Gate 2 as stated in the table above — `exception != null || isRollbackOnly` — is the **JTA**
   condition, so it is the right one for a JTA deployment; this sandbox is merely more eager to roll
   back.
   Note also that the class actually instantiated there is `JtaTransactionErrorHandler`, not
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
Camel", written before this project and now **fully reviewed** (2026-08-31; the copy at
`/tmp/camel-threads-and-bulkheads.md` is byte-identical, so there is only one version of it).

Confirmed: the 10/20/1000 defaults, CallerRuns, the seda defaults, platform-http-vertx's
`executeBlocking`, the pool-per-`.threads()` claim, `poolSize = maxPoolSize` (Camel builds a
bounded `LinkedBlockingQueue`, so a JDK pool really does fill the queue before growing), the
blocking-component list (`http`, `sql`, `aws2-s3` and `file` all block), and `timeoutEnabled`
reintroducing a hop.

Four things in it are **wrong or misleading** and must not be carried into the guide:

- **`Block` is not a rejection policy.** Four passages configure or discuss one. The enum has only
  `Abort` and `CallerRuns`; the parked-producer shape it describes belongs to `seda:blockWhenFull`.
- **Bulkhead permits are not decoupled from threads.** "Fifty permits and roughly zero threads is
  normal" is false inside Camel's `.circuitBreaker()`, which runs its block synchronously — so the
  whole "bulkheads for non-blocking calls" premise inverts. Wrapping a non-blocking call in a
  breaker is itself the thing that makes it hold a thread.
- **`bulkheadMaxWaitDuration` defaults to 0, not to queueing.**
- **CAMEL-20480 is not the ticket for `executeBlocking`.** It is *Won't Fix*, filed 2024, about a
  blocked **worker** thread under jbang debug — it presupposes the offload it is cited for. Cite
  `VertxPlatformHttpConsumer` instead.

Its remaining gaps: `.threads()` is suppressed inside a transaction (which changes its "When to
hop" advice), and the streaming section's claim that a body handed to `seda:` closes underneath the
source route is **not measured**. Its sizing formulas are arithmetic, not Camel behaviour — mark
them as reasoning, not measurement, if they go in the guide.

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

**The founding incident was row 2, not row 1.** That application runs camel-jta, where row 1 rolls
back — so the original bug could only ever have been the boundary-crossing case, and an independent
investigation confirmed exactly that. Note the consequence for the sandbox: `TransactionProbe`, the first probe
written here, reproduces row 1 on **Spring**. It demonstrates the same symptom by a different
route, and is not a reproduction of the incident. That is fine for illustrating the mechanism, but
do not cite it as a reproduction of the original bug.

Row 2 is the same on both and has nothing to do with transactions: the callee's own error handler
claims and clears the failure before control returns. **This is the row a refactor creates** —
factoring a stage into its own untransacted route moves its failures into a different error
handler. Confirmed against a real deployment, where an object-store upload sat in a separate
non-transacted route reached by `.choice()`.

The mark works everywhere because it writes to the **exchange**, which survives all of these paths.

**It was never about `handled(true)`.** Anything that clears the exception before the boundary
looks has the same effect, and three constructs do — measured inside a transacted route, each
commits the work written before the failure: `handled(true)`, a circuit breaker's `onFallback`, and
`doCatch` (`CircuitBreakerProbe`). Two of those are reached for to make a route *more* robust.

Also measured: the bug is only *observable* when the failure is non-resource — a SQL error has
already poisoned the transaction. Of three real test cases, two failed via CHECK constraints
and passed even unfixed.

## Out of scope by decision, not oversight

- **General exception propagation.** `MarkRecovered` makes Mule-4-style propagation
  (catch, translate, rethrow, catch higher) technically possible. **We will not build it and the
  guide warns against it** — it produces error flow invisible to `onException`, behaviour depending
  on internal flags no reviewer checks, and failure paths unreadable from the DSL. The reset is a
  repair at one boundary you own, never a foundation.
- The opinion that `doTry`/`doCatch` is *broken* stays out of the guide. The guide states the
  limitation and its difficulties; it does not editorialise about the framework.

## Candidate upstream issue — NOT filed, may be pursued later

`CandidateIssueProbe` holds the reproducer and the reason it is not a ticket yet.

**The claim worth filing:** an `onException` clause that fires and sets `handled(true)` has its
decision overridden and the exception restored, because `errorHandlerHandled` was pinned `false` by
an earlier, unrelated, already-resolved failure. Narrower and harder to defend than "claims
persist" (CAMEL-19441, closed as by design).

**Blocker:** no route shape found that reaches it without the app first clearing `failureHandled`
by hand. Without that the claim suppresses the clause outright. Tried and failed: called-route
failure then a later throw (clause never runs); `enrich` (resource failure propagates rather than
aggregating). Upstream would fairly say poking internal state is not a defect.

**Best lead if pursued:** `AbstractExchange(AbstractExchange parent)` propagates
`errorHandlerHandled`, `rollbackOnly`, `rollbackOnlyLast`, `routeStop` and `redeliveryExhausted` —
and NOT `failureHandled`. Two flags that jointly decide mappability, copied inconsistently. A route
where a copy is routed onward and then fails would make the case without touching internals.

## Second candidate ticket — a continue lever on `doCatch` (worked out, not filed)

Distinct from the one above and much easier to defend, because it asks for an addition rather than
claiming a defect.

**Shape still open:** an attribute on `doCatch` (`doCatch(X).continued(true)`) or a DSL *step* in the
same family as `markRollbackOnly()` — a `markRecovered()` the developer places where they want it.
The step form fits better now that placement is understood to be a real choice rather than a rule;
the attribute form cannot express "cover only what follows this point". To be reasoned through
before a PR.

**Ask (attribute form):** `doCatch(X).continued(true)`, clearing `failureHandled` and `errorHandlerHandled` inside
`CatchProcessor.process` **before** `processor.process(...)` at `:165` — joining the
clear-and-leave-cleared group at `:151-155`, never the snapshot group at `:134-139`. Before the body,
because the body will often call another route and that route's clauses can only map its own
failures once the claim is gone. `null`, not `false`: `false` is the sticky value that restores the
exception at `RedeliveryErrorHandler:1622`.

**Why that name.** The whole delta between what a catch does and what `prepareExchangeForContinue`
does is those two flags — exception, `EXCEPTION_CAUGHT`, `EXCEPTION_HANDLED` and
`redeliveryExhausted` are already identical. Two benign narrowings: resumption point is already
fixed by `end()`, and it cannot clear the rollback mark (the snapshot restores it) — which is the
half the guide calls a hazard. Rejected: `handled(true)` (taken, adjacent enough to confuse),
`clearErrorState()`/`resetErrorHandling()` (names the mechanism, invites use as a general cleanser).

**It SHOULD clear the rollback mark** — reversed after measurement. Two earlier drafts of this note
argued against it; both rested on claims that turned out to be false, recorded here so they are not
re-derived:

- *Retracted:* "a clause sees only its own failure's mark, a catch sees anything." Wrong — a mark
  inside a `doTry` halts the try body like anywhere else (`TryProcessor.next()` returns three
  *parts*, and its routeStop-only gate governs advancing between those; the try body is an ordinary
  Pipeline gating on the mark). Both constructs can only ever meet a mark set by the same step that
  threw. No asymmetry.
- *Retracted:* "clearing it would be one route revoking another route's abort, unlike a clause."
  Wrong — measured in `ClauseMarkErasureProbe`: a declining callee marks and throws, and the
  **caller's** clause revokes it. Clauses are cross-route too.
- *Retracted:* "don't smuggle a transaction decision into an error-handling boolean." That is what
  `continued(true)` does today, so the objection could only ever have been *don't widen it* — and
  per the finding below there is nothing to widen.

The finding that settles it: `RedeliveryErrorHandler:1453-1462` clears `rollbackOnly` under
`isDeadLetterChannel || shouldHandle || shouldContinue`, **before** any branch-specific work. So
`handled(true)` erases the mark identically to `continued(true)`, and `prepareExchangeForContinue`'s
own `setRollbackOnly(false)` at `:1241` is a redundant second clear. Erasure belongs to *resolving a
failure in a clause*, not to one keyword. A `doCatch` lever that cleared the mark is therefore
consistent with the clause system rather than an extension of it.

Supporting: the per-branch case is real and measured — one clause continues, its sibling maps and
stops. And the risk is low where it matters: in the shape the lever exists for (transacted callee
throws, caller catches) there is **no mark at all**, because gate 2's `exception != null` did the
rollback. The mark is absent exactly where the users are.

Residual nuance worth stating in the ticket, not a blocker: the mark conflates *abort* with *stop*
(the stop is emergent, via gate 1 — `RollbackProcessor` sets one flag and does not touch
`routeStop`). Continue has a legitimate claim only on the stop half, and clearing the mark is the
only way to get it. Note also `continued(true)` clears `rollbackOnly` and leaves `rollbackOnlyLast`,
while `CatchProcessor` snapshots both.

**Smaller fallback ask:** `ExchangeHelper.setFailureHandled(Exchange)` is public at `:654` but
one-way. A public counterpart clearing both flags makes the workaround supported with no DSL change
and no behaviour change to anything existing.

**Why `continued(true)` omits the verdict, for the ticket's background section.** Not because it is
only ever `false` — `:1639` sets `true`, `:1679` sets `false`. The three branches at `:1633/:1637/
:1672` are exclusive and continue is the one that never writes it, so there is nothing from that
pass to undo. An inherited verdict cannot reach it either: `alreadySet` returns at `:1630` *before*
`shouldContinue` is consulted, and separately the verdict's only writers sit below the unconditional
`setFailureHandled` at `:1615`, so **verdict set implies claimed** and a clause never runs on a
claimed exchange. The invariant holds everywhere except an exchange copy — which is exactly the hole
the other candidate ticket rests on.

## Upstream tickets on the sticky-claim family

Known and repeatedly reported; treated as by design in the general case.

- **CAMEL-19441** (2023) — routing does not continue after `doTry..doCatch..end` when a `direct:`
  callee used `handled(true)`. Resolved **Not A Bug**.
- **CAMEL-5139** — `continued(Predicate)` does not fire the second time; the reporter identifies
  `Exchange.FAILURE_HANDLED` persisting and names `removeProperty` as the workaround. Resolved
  **Incomplete** (fix versions 2.9.2 / 2.10.0), referencing **CAMEL-4057** for the analogous
  `continued(true)` case.
- That CAMEL-4057 fix is still in the code: `prepareExchangeForContinue` calls
  `setFailureHandled(false)`, which is why `continued(true)` is the one construct that clears a
  claim. Individual instances were patched; the general behaviour was not.

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
- Does camel-jta's per-retry-a-fresh-transaction nesting change what a clause sees on the second
  attempt? Not reproducible here; needs a probe against `camel-jta`.
- **Does a streaming body handed to `seda:` really close underneath the source route?** The source
  doc's streaming section rests on it, and it is reachable in-JVM: a `platform-http` consumer, a
  `seda:` handoff, and a read of the body after the source route has completed. Not measured.
- Is there a hook that fires on **failure including a mapped one**? `onFailureOnly` is out (it reads
  `isFailed()`), and `onCompleteOnly` fires on rollbacks. Today the only reliable answer is to do
  compensation inside the clause itself, before `markRollbackOnly()`.

## Guide conventions already chosen

Keep these unless there is a reason not to; they were argued over.

- **Plain names, not Camel's internals.** The guide says *claimed* (`failureHandled`), *handled*
  (`errorHandlerHandled`), and *rollback mark* (`rollbackOnly`). Internal names appear once, in an
  appendix. Nobody remembers `errorHandlerHandled` a week later.
- **The term is *claimed by an error handler*; "claimed" is the short form.** Write it in full at
  first use in anything that does not carry the vocabulary table — probe javadoc, code comments,
  commit messages, PR text, a reply in a thread. Bare *claimed* is for repeat use and for the
  guide's own prose. This is not fussiness: the word alone reads as a private coinage and does not
  answer the question a cold reader has, which is *claimed by what*. The same applies to anything
  copied out of the guide, since a snippet's comments travel into other people's codebases —
  which is why the reset snippet spells both stamps out.
- **Do not borrow Camel's own phrasing for these two.** Camel's javadoc calls `errorHandlerHandled`
  "handled by the error handler" and the manual calls `failureHandled` "failure handled", so both
  of Camel's names point at the opposite flag from the one a reader would guess. Cite them in the
  vocabulary table as a bridge for someone arriving from the manual; never adopt them.
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
- **The claim's blast radius depends on app shape, and the guide says so.** A broker-driven route
  gets a fresh exchange per delivery, so a claim lives one message and is invisible. A
  request/reply app threads one exchange through the whole request, so the first claim anywhere
  disables clause mapping for everything after it. Same mechanism, different consequence — and the
  reason a messaging-shaped error model carries this comfortably for years.
- **Do not oversell decline-by-default.** The guide argues that helper routes should decline so a
  refactor cannot silently change who owns a failure — but a transacted route *cannot* decline, so
  the convention excludes exactly the routes where the commit-on-failure bug lives. The honest
  claim is that it shrinks the set you must think hard about down to the transaction boundaries,
  which you then reason about one at a time. Stated at the `A transacted stage cannot decline` flag.
- **This repo is public, so this file is written for a reader outside it.** Findings established
  against a real deployment go in as the finding, stated generically — no application name, no
  internal reporter, no customer detail. That was scrubbed once; keep it that way when adding notes.
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
