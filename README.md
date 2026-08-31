# Where to Cut a Camel Application

**An evidence-backed guide to execution boundaries in Apache Camel: error ownership,
transactions, retry, threading and backpressure.**

Camel's manual describes the constructs. This describes what happens *between* them — which
is where `.transacted()` turns out to commit the writes it was supposed to undo, where a
refactor silently changes who owns a failure, and where a thread hop you configured does not
happen at all.

Nothing here is asserted. Every claim is reproduced by a [probe you can run in about a
second](src/test/java/sandbox), cited to a method in the `camel-4.18.0` sources, or marked *not
measured*.

**→ [Read the guide](guide.md)** · [How it was measured](METHOD.md)

---

## Start here

**Designing a route?** Work down the [review card](#the-review-card), then the question
sequence in [part one](guide.md#part-one--the-cut) and
[part two](guide.md#part-two--capacity).

**Debugging something?** Use the property tables below — they index every boundary construct
against every property — or go straight to the symptom:

| Symptom | Start at |
|---|---|
| Partial work committed after an error response | [Two gates](guide.md#two-gates-and-they-do-not-ask-the-same-question) |
| A 500 where you meant a 409 | [What a claim costs](guide.md#5-what-can-still-be-translated-after-the-first-failure) |
| Your `onException` never fires | [The first clause to fire is the only clause](guide.md#the-first-clause-to-fire-is-the-only-clause) |
| The error body arrives empty, as `text/plain` | [Why the status survives but the body does not](guide.md#why-the-status-survives-but-the-body-does-not) |
| Retries happen, or do not, and you did not ask | [Where retry is configured](guide.md#where-retry-is-configured) |
| A message vanished without reaching the DLQ | [Who is waiting for the answer](guide.md#2-who-is-waiting-for-the-answer) |
| The service stalls under load instead of refusing | [What happens when it is too slow](guide.md#what-happens-when-it-is-too-slow) |
| A thread hop that does not happen | [Where the thread changes](guide.md#where-the-thread-changes) |

---

## What each cut costs you

Seven boundary constructs, five properties. Each cell links to the passage that measures it.

### Error ownership

| The cut | What happens to a failure |
|---|---|
| `direct:` | The callee claims it; the caller stops and never resumes. [→](guide.md#the-first-clause-to-fire-is-the-only-clause) |
| `.transacted()` | Cannot decline — the transaction handler claims it regardless. [→](guide.md#3-should-it-own-its-errors) |
| `seda:` | The consuming route claims it. The sender learns only with `waitForTaskToComplete=Always`, and can map it only if that route declines. [→](guide.md#2-who-is-waiting-for-the-answer) |
| broker consumer | `handled(true)` acknowledges and drops. A live exception only survives if the route acknowledges at the *end* — that is, `transacted=true`. [→](guide.md#2-who-is-waiting-for-the-answer) |
| HTTP edge | Status survives as a header; the body is dropped while an exception is present. [→](guide.md#why-the-status-survives-but-the-body-does-not) |
| `.circuitBreaker()` | `onFallback` claims it — no clause fires — yet routing resumes after `end()`. [→](guide.md#retry-and-why-it-does-not-compose-with-a-breaker) |
| outbox + relay | *not measured* |

### Transaction

| The cut | What it does to the commit |
|---|---|
| `direct:` | Joins the caller's — no boundary of its own. [→](guide.md#7-does-splitting-the-stage-split-the-commit) |
| `.transacted()` | The whole route body, wherever written; absorbs a transacted callee. [→](guide.md#6-what-is-the-transaction-boundary) |
| `seda:` | Outside it — delivered even when the sender rolls back, and readable before the sender commits. [→](guide.md#what-guarantees-the-deferred-work-happens) |
| broker consumer | A local JMS transaction over acknowledgement, not your database. Aborting it un-acknowledges. [→](guide.md#2-who-is-waiting-for-the-answer) |
| HTTP edge | — |
| `.circuitBreaker()` | None of its own, but `timeoutEnabled` moves work *out* of an enclosing one. [→](guide.md#where-the-thread-changes) |
| outbox + relay | The trigger is inside it — a rollback takes the work and the trigger together. [→](guide.md#what-guarantees-the-deferred-work-happens) |

### Retry

| The cut | What retries, and how often |
|---|---|
| `direct:` | None of its own; whichever handler applies. [→](guide.md#where-retry-is-configured) |
| `.transacted()` | Only clauses survive the boundary — handler scopes are replaced and fail silently. Spring shares one transaction across attempts; camel-jta gives each its own. [→](guide.md#where-retry-is-configured) |
| `seda:` | None. Nothing acknowledges an in-memory message, so a failure loses it unless an error handler catches it. [→](guide.md#2-who-is-waiting-for-the-answer) |
| broker consumer | The broker's policy, then the dead-letter queue — plain sessions get neither. [→](guide.md#2-who-is-waiting-for-the-answer) |
| HTTP edge | The route's handler can retry a step; the request is never re-delivered, so the retry that matters is the client's. [→](guide.md#what-happens-when-it-is-too-slow) |
| `.circuitBreaker()` | Redelivery cannot re-enter the block — one attempt. A caller's retry re-enters the *route*, and each attempt is another recorded failure. [→](guide.md#retry-and-why-it-does-not-compose-with-a-breaker) |
| outbox + relay | *not measured* |

### Threading

| The cut | Whose thread |
|---|---|
| `direct:` | No change — the caller's throughout. [→](guide.md#where-the-thread-changes) |
| `.transacted()` | Pins the route to one thread; `.threads()` inside is suppressed outright. [→](guide.md#where-the-thread-changes) |
| `seda:` | A consumer thread; the sender does not wait, unless it is out-capable. [→](guide.md#where-the-thread-changes) |
| broker consumer | One thread per `concurrentConsumers`, and that count is the bound. [→](guide.md#8-where-does-the-thread-change-and-what-happens-when-it-is-too-slow) |
| HTTP edge | A shared Vert.x *worker* thread, never the event loop. [→](guide.md#where-the-thread-changes) |
| `.circuitBreaker()` | A semaphore, not a pool — no hop unless `timeoutEnabled`. The block runs synchronously, so the caller's thread is held even around a non-blocking call. [→](guide.md#when-there-is-no-pool-to-bound) |
| outbox + relay | *not measured* |

### Backpressure

| The cut | What the producer is told when it is full |
|---|---|
| `direct:` | Nothing: the caller blocks, which is the bound. [→](guide.md#what-happens-when-it-is-too-slow) |
| `.transacted()` | The connection pool. A boundary takes a connection when it opens and holds it for the whole body, so concurrency caps at the pool and the excess fails on its timeout. [→](guide.md#what-happens-when-it-is-too-slow) |
| `seda:` | Bounded at 1000. Full: throws by default, parks with `blockWhenFull`, drops silently with `discardWhenFull`. And a restart loses whatever is queued. [→](guide.md#what-a-restart-loses) |
| broker consumer | The backlog sits in the broker, not in you; only producer flow control pushes back, and then by parking the send. [→](guide.md#what-happens-when-it-is-too-slow) |
| HTTP edge | Caps at the worker pool — 20 by default — then queues invisibly and never refuses. A bulkhead in front of the work turns that into a 503. [→](guide.md#what-happens-when-it-is-too-slow) |
| `.circuitBreaker()` | Bulkhead: off by default; on, 25 permits and no wait, so it sheds into the fallback. [→](guide.md#when-there-is-no-pool-to-bound) |
| outbox + relay | *not measured* |

### One line per construct

Reading a single construct across all five properties is the debug path the split above
loses:

| The cut | In one line |
|---|---|
| `direct:` | A method call. Same thread, same transaction, same exchange — and therefore the callee's claim is the caller's problem. |
| `.transacted()` | Owns the whole route body and cannot decline a failure. Its concurrency is the connection pool; its retry survives only as clauses. |
| `seda:` | A thread boundary that is not a durability boundary. Outside the transaction, no redelivery, bounded at 1000, empty after a restart. |
| broker consumer | The only cut where rollback and retry are one event — but only with `transacted=true`. Otherwise a failure is acknowledged and lost. |
| HTTP edge | Refuses nothing and returns your body only if the exception is gone. Concurrency is the Vert.x worker pool, which nothing in the route declares. |
| `.circuitBreaker()` | Owns like a callee, returns like a `doCatch`. No handler inside it, so no redelivery; the block runs synchronously on your thread. |
| outbox + relay | One resource manager, so the trigger cannot disagree with the work. Mostly *not measured* — it is a pattern, not a construct. |

---

## The review card

One pass over a route before it merges. Every line is a link to the passage that measures it
— if a line here and the guide disagree, the guide is right and this is a bug.

**Structure**

- [ ] **Does this need to be its own route?** Reuse, its own transaction, or different error
  ownership — otherwise inline it. A route boundary is naming and reuse; it is *not* a
  boundary for failure, transaction or retry. [→](guide.md#1-should-it-be-its-own-route)
- [ ] **Is anyone waiting for the answer?** Request/reply and fire-and-forget give
  `handled(true)` opposite meanings — *this is the answer* versus *acknowledge and drop it*.
  [→](guide.md#2-who-is-waiting-for-the-answer)

**Failure**

- [ ] **Does this stage own its errors, deliberately?** Clauses are a declaration that every
  caller lives with. Writing none is not neutral — the stage still claims. Decline with
  `errorHandler(noErrorHandler())`, and note that a transacted stage *cannot*, which is
  exactly why `doTry`/`doCatch` is the remaining recovery against one.
  [→](guide.md#3-should-it-own-its-errors)
- [ ] **If a caller must recover, does the callee leave the exception intact?**
  `handled(true)` in the callee makes recovery impossible, not merely awkward — there is
  nothing left to catch. [→](guide.md#4-does-a-caller-need-to-recover-from-it)
- [ ] **Has anything already claimed this exchange?** One claim disables clause mapping for
  the rest of its life, which in a request/reply app is the rest of the request.
  [→](guide.md#5-what-can-still-be-translated-after-the-first-failure)

**Commit**

- [ ] **Where is the transaction boundary, and does every mapping clause end with
  `markRollbackOnly()`?** Anything that clears the exception before the boundary looks —
  `handled(true)`, an `onFallback`, a `doCatch`, a swallowed failure — commits the work it
  meant to undo. [→](guide.md#two-gates-and-they-do-not-ask-the-same-question)
- [ ] **Did splitting the stage actually split the commit?** Usually not: a transacted caller
  absorbs a transacted callee, and `REQUIRES_NEW` alone does not isolate it because the mark
  travels on the exchange. [→](guide.md#7-does-splitting-the-stage-split-the-commit)

**Capacity**

- [ ] **Is a hop here avoiding a block on a thread I do not own?** That is the only reason to
  hop. Inside `.transacted()` it will not happen at all.
  [→](guide.md#where-the-thread-changes)
- [ ] **Which pool does this land in, and did I choose it?** Every `.threads()` without an
  `executorService` builds its own unsized pool. One pool per resource class, named.
  [→](guide.md#how-many-pools-and-what-goes-in-them)
- [ ] **What is this boundary's queue, and what does the producer hear when it fills?** Every
  boundary has one. The HTTP edge's is unbounded and refuses nothing until you put a ceiling
  in front of it. [→](guide.md#what-happens-when-it-is-too-slow)
- [ ] **Does the retry sit inside or outside the breaker?** Inside, it does not happen;
  outside, it multiplies the recorded failures and spends bulkhead permits.
  [→](guide.md#retry-and-why-it-does-not-compose-with-a-breaker)

**Outside effects**

- [ ] **What in here survives a rollback?** Find the irreversible step first and place it
  deliberately; order the window to leave inert garbage rather than a dangling reference.
  [→](guide.md#which-way-should-the-window-fail)
- [ ] **If it compensates, is the compensation in the clause and before the mark?**
  `onCompletion` reads the exception, never the rollback mark, so `onCompleteOnly` fires for
  transactions that rolled back. [→](guide.md#where-does-the-compensation-actually-hang)
- [ ] **Can this operation safely happen twice?** Almost every path here ends in
  at-least-once. Key from the sender's facts, persistent repository, and in the same
  transaction as the work. [→](guide.md#can-this-operation-safely-happen-twice)
- [ ] **If work is deferred, what guarantees it happens?** If committed state cannot rebuild
  the trigger, the queue is not a guarantee and the message has to be written in the same
  transaction as the work. [→](guide.md#what-guarantees-the-deferred-work-happens)

---

## This repository

```
README.md    this — the navigator, the property tables and the review card
guide.md     the document
METHOD.md    versions, configuration, how to run the probes
src/test/    the probes; every `probe` link in the guide points here
tools/       the link checker that keeps those links honest
```

```bash
mvn clean test                        # every probe, about 40s
mvn test -Dtest=TransactionProbe      # one
python3 tools/check-links.py          # evidence and cross-reference links
```

A probe is a JUnit test against in-memory H2, an in-JVM ActiveMQ and an in-JVM
`platform-http` port. There are no containers and nothing to install beyond a JDK. The test
name states the finding and the assertion message says why it matters, so a failure reads as
a retracted claim rather than a stack trace.
