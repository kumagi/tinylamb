import TinylambOpt.ResearchGoals.ResourcePolicies.CommitBatches

/-!
Proof contribution for ResourcePolicies_CommitBatches_StartOnlyCompletionPrune. The theorem below
records a planning or execution decision that preserves the modeled
SQL result, or an explicit counterexample when the proposed rewrite is
unsound.
-/


namespace TinylambOpt.ResearchProofs.ResourcePolicies.CommitBatches.Goal_StartOnlyCompletionPrune

open TinylambOpt.ResearchGoals.ResourcePolicies.CommitBatches


open TinylambOpt.ResearchGoals.ResourcePolicies.CommitBatches

/-- Counterexample: a write starting before the resume point can cover bytes at
    and after it; pruning by start alone therefore shortens the durable cut. -/
theorem refute : ¬ StartOnlyCompletionPrune.Goal := by
  intro h
  have hbad := h [⟨0, 5⟩] 2 10
  have hnot : ¬ prefixEnd ([] : List Write) 2 10 = prefixEnd [⟨0, 5⟩] 2 10 := by
    decide
  exact hnot hbad
end TinylambOpt.ResearchProofs.ResourcePolicies.CommitBatches.Goal_StartOnlyCompletionPrune
