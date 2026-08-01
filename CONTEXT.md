# Kuu

Kuu coordinates background work and makes its execution observable.

## Language

**Task**:
A registered unit of work that can be invoked. A Task is a definition, not a particular execution.
_Avoid_: Job, Run

**Run**:
One logical invocation of a Task from submission to its final outcome. A Run may contain multiple Attempts.
_Avoid_: Task, Attempt

**Attempt**:
One distinct execution within a Run. Retries always create a new Attempt; their logs, timing, errors, and outcome remain explicitly separated.
_Avoid_: Run, Retry

**Observation Gap**:
A known loss of observable records from an Attempt. It is always reported explicitly with the number of missing records; it is never a silent drop.
_Avoid_: Complete logs

**Remote Failure**:
A safe, inert description of an exception raised by an Attempt. It preserves the exception label, message, causal chain, and stack frames without reconstructing executable exception objects.
_Avoid_: Serialized exception

**Launch Request**:
A request to create a Run. Invalid parameters reject the Launch Request before any Run or Attempt exists.
_Avoid_: Run

**Retry**:
A subsequent Attempt within the same active Run. A Retry preserves the Run's identity and history.
_Avoid_: Replay, New Run

**Replay**:
A new Run created from a previous terminal Run's invocation. It has its own identity and retry budget and keeps an explicit link to the source Run.
_Avoid_: Retry, Reopened Run

**Dead-letter Recovery**:
A Replay created from an invocation held in a broker's dead-letter store. The original terminal Run remains unchanged.
_Avoid_: Retry

**Progress**:
A structured, non-terminal update emitted by an active Attempt. Progress belongs only to that Attempt and resets when a Retry creates the next Attempt.
_Avoid_: Log, Run outcome

**Unknown Attempt**:
An Attempt whose current state cannot be established after its worker disappears without a terminal event. Unknown is an observability state, not a failure outcome.
_Avoid_: Failed Attempt, Running Attempt

**Lost Attempt**:
An Attempt that became Unknown and was later superseded by redelivery. It is an operational outcome distinct from an exception raised by task code.
_Avoid_: Failed Attempt, Retry

**Failed Run**:
A terminal Run whose Attempts were exhausted without success. Placement of its invocation in a dead-letter store is recorded separately and is not the Run's status.
_Avoid_: Dead Run

**Preset**:
A named operational configuration under which Tasks are registered and Runs are executed. Operator commands target a Preset rather than a particular process.
_Avoid_: Instance, Worker

**Instance**:
A live process serving a Preset. Instances are replaceable runtime participants, not stable operator targets.
_Avoid_: Preset, Worker

**Configuration Drift**:
A disagreement between Instances of the same Preset about a Task's contract. Manual Launch is blocked until the Preset becomes consistent.
_Avoid_: Task version

**Live-only Mode**:
A dashboard mode without durable Run history. It shows current observations but cannot provide catch-up, history, or Replay.
_Avoid_: Persistence failure

**Cancel Requested**:
A non-terminal Run state indicating that cancellation was requested but not yet acknowledged. A Run becomes Cancelled only after acknowledgement.
_Avoid_: Cancelled
