import Std

/-! Reader/writer admission for one contended resource. A FIFO turn admits one
writer or a contiguous reader cohort. Tickets name a generation so a delayed
cancel cannot abort the next transaction on a reused connection. -/
namespace TinylambOpt.ResearchGoals.ResourcePolicies.WaitQueue

structure Ticket where
  owner : Nat
  generation : Nat
  writer : Bool
  deriving DecidableEq, Repr
def turn : List Ticket → List Ticket
  | [] => []
  | t::ts => if t.writer then [t] else t::(ts.takeWhile (fun x => !x.writer))
def after (q : List Ticket) := q.drop (turn q).length
def waitTurns (q : List Ticket) (t : Ticket) : Nat :=
  let ahead := q.takeWhile (· != t)
  let flags := ahead.map Ticket.writer
  let prev := true :: flags
  (flags.filter id).length + (flags.zip prev).countP (fun p => !p.1 && p.2) -
    (if !t.writer && flags.getLast? == some false then 1 else 0)

/-- Count writer turns and reader runs separately to bound head-of-line delay in scheduler turns. -/
def CohortWaitCountdown.Goal : Prop := ∀ q t, q.Nodup → t ∈ after q →
  waitTurns q t = waitTurns (after q) t + 1

/-- Future arrivals cannot delay an existing ticket when admission and cancellation preserve FIFO order. -/
def ArrivalIndependentCohortWait.Goal : Prop := ∀ q arrivals t, t ∈ q →
  waitTurns (q++arrivals) t = waitTurns q t

/-- Admit the largest prefix compatible with one shared read lease or one exclusive write lease. -/
def MaximalCompatiblePrefix.Goal : Prop := ∀ q k, 0 < k → k ≤ q.length →
  ((q.take k).all (fun t => !t.writer) = true ∨ k = 1) → k ≤ (turn q).length

/-- A newly queued reader may bypass a waiting writer with no increase in the writer's number of scheduler turns. -/
def ReaderBypassLatency.Goal : Prop := ∀ active writer newcomer tail, !active.writer → writer.writer →
  !newcomer.writer → active ≠ writer → newcomer ≠ writer →
  waitTurns (active::newcomer::writer::tail) writer = waitTurns (active::writer::tail) writer

/-- Equal turn counts also bound elapsed time if a read cohort holds the lease until its slowest member completes. -/
def BypassElapsedTimeBound.Goal : Prop := ∀ (old : List Nat) newcomer,
  old ≠ [] → ((old++[newcomer]).max?).getD 0 ≤ (old.max?).getD 0

def cancel (owner gen : Nat) (q : List Ticket) :=
  q.filter (fun t => !(t.owner == owner && t.generation == gen))

/-- Canceling older transactions can combine reader cohorts but cannot increase any surviving ticket's turn count. -/
def CancelMonotoneWait.Goal : Prop := ∀ q owner gen t, t ∈ cancel owner gen q →
  waitTurns (cancel owner gen q) t ≤ waitTurns q t

/-- Test reusing an already selected cohort after head cancellation instead of scheduling the newly exposed queue head. -/
def CanceledHeadNeedsRebatch.Goal : Prop := ∀ t ts, t.writer → ts ≠ [] →
  turn (cancel t.owner t.generation (t::ts)) = cancel t.owner t.generation (turn (t::ts))

/-- If the oldest reader remains pinned, the retirement horizon is unchanged even when younger readers complete. -/
def OldestReaderRetirementHorizon.Goal : Prop := ∀ q owner gen oldest,
  ((q.filter (fun t => !t.writer)).map Ticket.generation).min? = some oldest →
  (∃ t ∈ cancel owner gen q, t.writer = false ∧ t.generation = oldest) →
  (((cancel owner gen q).filter (fun t => !t.writer)).map Ticket.generation).min? = some oldest

#guard waitTurns [⟨1,1,false⟩,⟨2,2,false⟩,⟨3,3,true⟩] ⟨3,3,true⟩ == 1

end TinylambOpt.ResearchGoals.ResourcePolicies.WaitQueue
