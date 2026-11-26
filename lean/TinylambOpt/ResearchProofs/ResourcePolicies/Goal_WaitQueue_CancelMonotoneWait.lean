import TinylambOpt.ResearchProofs.Batch5.Proof_ResourcePolicies_WaitQueue_CancelMonotoneWait

/-!
Registers the existing checked result for `WaitQueue.CancelMonotoneWait.Goal`.

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

The theorem is contributed by `TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.WaitQueue.Goal_CancelMonotoneWait` and re-exported here so that
`WaitQueue_All.lean` registers the goal. SQL execution may rely on the
modeled result only under the assumptions stated above.
-/

namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_WaitQueue_CancelMonotoneWait

theorem sound : TinylambOpt.ResearchGoals.ResourcePolicies.WaitQueue.CancelMonotoneWait.Goal :=
  TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.WaitQueue.Goal_CancelMonotoneWait.Scratch.CancelMonotoneWait.sound

end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_WaitQueue_CancelMonotoneWait
