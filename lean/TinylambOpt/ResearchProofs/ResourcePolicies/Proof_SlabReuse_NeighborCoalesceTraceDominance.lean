import Std
import TinylambOpt.ResearchGoals.ResourcePolicies.SlabReuse

/-!
Proof contribution for ResourcePolicies_SlabReuse_NeighborCoalesceTraceDominance. The theorem below
records a planning or execution decision that preserves the modeled
SQL result, or an explicit counterexample when the proposed rewrite is
unsound.
-/


namespace TinylambOpt.ResearchProofs.ResourcePolicies.SlabReuse.Goal_NeighborCoalesceTraceDominance

open TinylambOpt.ResearchGoals.ResourcePolicies.SlabReuse


/-- Coalescing neighbors loses a feasible trace: [3,2,2] fits [2,2,3] but not [4,3]. -/
theorem refute : ¬ TinylambOpt.ResearchGoals.ResourcePolicies.SlabReuse.NeighborCoalesceTraceDominance.Goal := by
  intro h
  have hgood :
      (TinylambOpt.ResearchGoals.ResourcePolicies.SlabReuse.place (([] : List Nat) ++ [2, 2] ++ [3]) [3, 2, 2]).isSome = true := by
    decide
  have hbad :
      (TinylambOpt.ResearchGoals.ResourcePolicies.SlabReuse.place (([] : List Nat) ++ [2 + 2] ++ [3]) [3, 2, 2]).isSome = false := by
    decide
  have hc := h ([] : List Nat) 2 2 [3] [3, 2, 2] hgood
  rw [hbad] at hc
  contradiction
end TinylambOpt.ResearchProofs.ResourcePolicies.SlabReuse.Goal_NeighborCoalesceTraceDominance
