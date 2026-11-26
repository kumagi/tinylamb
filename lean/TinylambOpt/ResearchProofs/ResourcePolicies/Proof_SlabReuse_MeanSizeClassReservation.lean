import Std
import TinylambOpt.ResearchGoals.ResourcePolicies.SlabReuse

/-!
Proof contribution for ResourcePolicies_SlabReuse_MeanSizeClassReservation. The theorem below
records a planning or execution decision that preserves the modeled
SQL result, or an explicit counterexample when the proposed rewrite is
unsound.
-/


namespace TinylambOpt.ResearchProofs.ResourcePolicies.SlabReuse.Goal_MeanSizeClassReservation

open TinylambOpt.ResearchGoals.ResourcePolicies.SlabReuse


/-- Rounding the mean does not bound skewed totals: classes [8,16,32] charge [1,9] as 24 > 2*8. -/
theorem refute : ¬ TinylambOpt.ResearchGoals.ResourcePolicies.SlabReuse.MeanSizeClassReservation.Goal := by
  intro h
  have hne : ([1, 9] : List Nat) ≠ [] := by decide
  have hcharge :
      TinylambOpt.ResearchGoals.ResourcePolicies.SlabReuse.charge [8, 16, 32] [1, 9] = some 24 := by
    decide
  have hround :
      TinylambOpt.ResearchGoals.ResourcePolicies.SlabReuse.rounded [8, 16, 32]
        (([1, 9].sum + [1, 9].length - 1) / [1, 9].length) = some 8 := by
    decide
  have hc := h [8, 16, 32] [1, 9] 24 8 hne hcharge hround
  have hlen : ([1, 9] : List Nat).length = 2 := by decide
  rw [hlen] at hc
  omega
end TinylambOpt.ResearchProofs.ResourcePolicies.SlabReuse.Goal_MeanSizeClassReservation
