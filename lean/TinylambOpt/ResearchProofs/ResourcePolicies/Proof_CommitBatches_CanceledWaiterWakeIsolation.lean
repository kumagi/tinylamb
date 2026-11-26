import TinylambOpt.ResearchGoals.ResourcePolicies.CommitBatches

/-!
Proof contribution for ResourcePolicies_CommitBatches_CanceledWaiterWakeIsolation. The theorem below
records a planning or execution decision that preserves the modeled
SQL result, or an explicit counterexample when the proposed rewrite is
unsound.
-/


namespace TinylambOpt.ResearchProofs.ResourcePolicies.CommitBatches.Goal_CanceledWaiterWakeIsolation

open TinylambOpt.ResearchGoals.ResourcePolicies.CommitBatches


open TinylambOpt.ResearchGoals.ResourcePolicies.CommitBatches

/-- Cancellation is a filter on the waiter list; filtering commutes with the
    durability filter, so removing a client before or after the fsync callback
    acknowledges exactly the same remaining clients in the same order. -/
theorem sound : CanceledWaiterWakeIsolation.Goal := by
  intro ws d c
  simp [wake, List.filter_filter, Bool.and_comm]
end TinylambOpt.ResearchProofs.ResourcePolicies.CommitBatches.Goal_CanceledWaiterWakeIsolation
