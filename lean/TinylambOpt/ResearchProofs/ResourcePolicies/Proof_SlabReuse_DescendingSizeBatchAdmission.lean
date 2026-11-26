import Std
import TinylambOpt.ResearchGoals.ResourcePolicies.SlabReuse

/-!
Proof contribution for ResourcePolicies_SlabReuse_DescendingSizeBatchAdmission. The theorem below
records a planning or execution decision that preserves the modeled
SQL result, or an explicit counterexample when the proposed rewrite is
unsound.
-/


namespace TinylambOpt.ResearchProofs.ResourcePolicies.SlabReuse.Goal_DescendingSizeBatchAdmission

open TinylambOpt.ResearchGoals.ResourcePolicies.SlabReuse


/-- Descending order is not a complete admission test: [2,2,3] fits holes [4,3], but [3,2,2] does not. -/
theorem refute : ¬ TinylambOpt.ResearchGoals.ResourcePolicies.SlabReuse.DescendingSizeBatchAdmission.Goal := by
  intro h
  have hperm : List.Perm [2, 2, 3] [2, 2, 3] := by simp
  have hgood : (TinylambOpt.ResearchGoals.ResourcePolicies.SlabReuse.place [4, 3] [2, 2, 3]).isSome = true := by
    decide
  have hsort : ([2, 2, 3].mergeSort (fun a b => decide (b ≤ a))) = [3, 2, 2] := by
    simp [List.mergeSort]
  have hbad :
      (TinylambOpt.ResearchGoals.ResourcePolicies.SlabReuse.place [4, 3]
        ([2, 2, 3].mergeSort (fun a b => decide (b ≤ a)))).isSome = false := by
    rw [hsort]
    decide
  have hc := h [4, 3] [2, 2, 3] [2, 2, 3] hperm hgood
  rw [hbad] at hc
  contradiction
end TinylambOpt.ResearchProofs.ResourcePolicies.SlabReuse.Goal_DescendingSizeBatchAdmission
