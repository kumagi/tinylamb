import TinylambOpt.ResearchProofs.Batch5.Proof_ResourcePolicies_WaitQueue_OldestReaderRetirementHorizon

/-!
Registers the existing checked result for `WaitQueue.OldestReaderRetirementHorizon.Goal`.

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

The theorem is contributed by `TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.WaitQueue.Goal_OldestReaderRetirementHorizon` and re-exported here so that
`WaitQueue_All.lean` registers the goal. SQL execution may rely on the
modeled result only under the assumptions stated above.
-/

namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_WaitQueue_OldestReaderRetirementHorizon

theorem sound : TinylambOpt.ResearchGoals.ResourcePolicies.WaitQueue.OldestReaderRetirementHorizon.Goal :=
  TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.WaitQueue.Goal_OldestReaderRetirementHorizon.Scratch.OldestReaderRetirementHorizon.sound

end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_WaitQueue_OldestReaderRetirementHorizon
