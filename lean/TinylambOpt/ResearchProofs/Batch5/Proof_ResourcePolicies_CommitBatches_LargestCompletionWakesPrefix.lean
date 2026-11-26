import TinylambOpt.ResearchGoals.ResourcePolicies.CommitBatches

/-!
Proof contribution for ResourcePolicies_CommitBatches_LargestCompletionWakesPrefix. The theorem below
records a planning or execution decision that preserves the modeled
SQL result, or an explicit counterexample when the proposed rewrite is
unsound.
-/


namespace TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.CommitBatches.Goal_LargestCompletionWakesPrefix

open TinylambOpt.ResearchGoals.ResourcePolicies.CommitBatches


open TinylambOpt.ResearchGoals.ResourcePolicies.CommitBatches

/-- Counterexample: the largest write end need not be reached contiguously; a gap
    before it stops the fsync cut early even though the maximum end is large. -/
theorem refute : ¬ LargestCompletionWakesPrefix.Goal := by
  intro h
  have hbad := h [⟨0, 2⟩, ⟨5, 3⟩] 8 (by decide) (by decide)
  have hnot : ¬ prefixEnd [⟨0, 2⟩, ⟨5, 3⟩] 0 8 = 8 := by native_decide
  exact hnot hbad
end TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.CommitBatches.Goal_LargestCompletionWakesPrefix
