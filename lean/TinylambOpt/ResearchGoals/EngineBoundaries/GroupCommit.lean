import Std

namespace TinylambOpt.ResearchGoals.EngineBoundaries.GroupCommit

/-! LSNs are exclusive byte ends. One successful sync may have been issued
before the writer advanced again. A waiter supplies the end of its record. -/
structure State where
  appended : Nat := 0
  written : Nat := 0
  durable : Nat := 0
  flight : Option Nat := none
  deriving BEq, DecidableEq, Inhabited

inductive Event where
  | append (bytes : Nat) | write (bytes : Nat) | submit | finish | fail
  deriving BEq

def step (s : State) : Event → State
  | .append n => { s with appended := s.appended + n }
  | .write n => { s with written := min s.appended (s.written + n) }
  | .submit => { s with flight := s.flight.orElse (fun _ => some s.written) }
  | .finish => { s with durable := max s.durable (s.flight.getD 0), flight := none }
  | .fail => { s with flight := none }

def run (s : State) (es : List Event) := es.foldl step s
def ordered (s : State) := s.durable ≤ s.written ∧ s.written ≤ s.appended ∧
  ∀ x ∈ s.flight, x ≤ s.written
def ready (s : State) (waits : List Nat) := waits.filter (· ≤ s.durable)

/-- An arbitrary interleaving keeps each acknowledged byte in the written prefix. -/
def FrontierOrder.Goal : Prop := ∀ s es, ordered s → ordered (run s es)

/-- Coalescing adjacent partial writes preserves the next sync's coverage. -/
def CoalesceWrites.Goal : Prop := ∀ s a b rest, ordered s →
  run s (.write a :: .write b :: rest) = run s (.write (a+b) :: rest)

/-- Wait for just the greatest target in a group if all waiters need synchronous durability. -/
def GreatestWaiter.Goal : Prop := ∀ s ws, ready s ws = ws ↔ ws.max?.getD 0 ≤ s.durable

/-- Once a waiter can reply it stays eligible across retries and concurrent appends. -/
def StickyAck.Goal : Prop := ∀ s es ws w, w ∈ ready s ws → w ∈ ready (run s es) ws

/-- Aggressive: one completion also covers a write issued after sync submission. -/
def LateWritePiggyback.Goal : Prop := ∀ s n, ordered s → s.flight = none →
  let t := run s [.submit, .write n, .finish]
  t.durable = t.written

/-- Retrying after failure captures the newer written frontier. -/
def RetryLatest.Goal : Prop := ∀ s n, ordered s →
  let t := run s [.fail, .write n, .submit, .finish]
  t.durable = t.written

/-- Two requests to start the same outstanding sync need only one completion. -/
def DuplicateSubmit.Goal : Prop := ∀ s trace,
  run s (.submit :: .submit :: trace) = run s (.submit :: trace)

/-- Attaching waiters to an in-flight sync cannot certify more than its captured end. -/
def FlightCoverage.Goal : Prop := ∀ s endLSN ws, s.flight = some endLSN →
  ready (step s .finish) ws = ws.filter (· ≤ max s.durable endLSN)

/-- Testing a record's starting LSN instead of its exclusive end admits partial records. -/
def StartIsEnough.Goal : Prop := ∀ s start bytes, ordered s → 0 < bytes →
  start + bytes ≤ s.appended → start ≤ s.durable → start + bytes ≤ s.durable

/-- Replies newly eligible after a sync occupy an exact half-open/closed interval. -/
def NewlyDurable.Goal : Prop := ∀ s ws, let t := step s .finish
  (ready t ws).filter (fun w => s.durable < w) =
    ws.filter (fun w => s.durable < w && w ≤ t.durable)

#guard (run {} [.append 12, .write 7, .submit, .write 5, .finish]).durable == 7
#guard (run {} [.append 12, .write 7, .submit, .write 5, .finish]).written == 12

end TinylambOpt.ResearchGoals.EngineBoundaries.GroupCommit
