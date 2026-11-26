import TinylambOpt.ResearchGoals.ResourcePolicies.CommitBatches

/-!
Proof contribution for ResourcePolicies_CommitBatches_WALBytesBoundWaitingClients. The theorem below
records a planning or execution decision that preserves the modeled
SQL result, or an explicit counterexample when the proposed rewrite is
unsound.
-/


namespace TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.CommitBatches.Goal_WALBytesBoundWaitingClients

open TinylambOpt.ResearchGoals.ResourcePolicies.CommitBatches


open TinylambOpt.ResearchGoals.ResourcePolicies.CommitBatches

/-- Counterexample: the model allows two waiters with the same exclusive end,
    so a byte bound does not by itself bound the number of acknowledgements. -/
theorem refute : ¬ WALBytesBoundWaitingClients.Goal := by
  intro h
  have hbad := h [⟨0, 1⟩, ⟨0, 1⟩] 0 1 (by decide)
  have hnot : ¬ (2 : Nat) ≤ 1 := by decide
  exact hnot hbad
end TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.CommitBatches.Goal_WALBytesBoundWaitingClients
