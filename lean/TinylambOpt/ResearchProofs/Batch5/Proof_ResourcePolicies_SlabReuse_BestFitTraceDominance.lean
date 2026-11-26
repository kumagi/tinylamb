import Std
import TinylambOpt.ResearchGoals.ResourcePolicies.SlabReuse

/-!
Proof contribution for ResourcePolicies_SlabReuse_BestFitTraceDominance. The theorem below
records a planning or execution decision that preserves the modeled
SQL result, or an explicit counterexample when the proposed rewrite is
unsound.
-/


namespace TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.SlabReuse.Goal_BestFitTraceDominance

open TinylambOpt.ResearchGoals.ResourcePolicies.SlabReuse


/-- Best-fit is not dominant: first-fit places [1,2,2] into holes [3,2], best-fit cannot. -/
theorem refute : ¬ TinylambOpt.ResearchGoals.ResourcePolicies.SlabReuse.BestFitTraceDominance.Goal := by
  intro h
  have hfirst : (TinylambOpt.ResearchGoals.ResourcePolicies.SlabReuse.place [3, 2] [1, 2, 2]).isSome = true := by
    decide
  have hbest : (TinylambOpt.ResearchGoals.ResourcePolicies.SlabReuse.bestFit [3, 2] [1, 2, 2]).isSome = false := by
    native_decide
  have hc := h [3, 2] [1, 2, 2] hfirst
  rw [hbest] at hc
  contradiction
end TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.SlabReuse.Goal_BestFitTraceDominance
