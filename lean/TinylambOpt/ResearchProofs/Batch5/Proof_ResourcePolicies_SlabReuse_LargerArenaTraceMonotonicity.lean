import Std
import TinylambOpt.ResearchGoals.ResourcePolicies.SlabReuse

/-!
Proof contribution for ResourcePolicies_SlabReuse_LargerArenaTraceMonotonicity. The theorem below
records a planning or execution decision that preserves the modeled
SQL result, or an explicit counterexample when the proposed rewrite is
unsound.
-/


namespace TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.SlabReuse.Goal_LargerArenaTraceMonotonicity

open TinylambOpt.ResearchGoals.ResourcePolicies.SlabReuse


/-- Growing a single hole can break first-fit: [3,7,5] admits [4,5,3,3], [4,7,5] does not. -/
theorem refute : ¬ TinylambOpt.ResearchGoals.ResourcePolicies.SlabReuse.LargerArenaTraceMonotonicity.Goal := by
  intro h
  have hlen : ([3, 7, 5] : List Nat).length = ([4, 7, 5] : List Nat).length := by decide
  have hle : ∀ p ∈ ([3, 7, 5] : List Nat).zip [4, 7, 5], p.1 ≤ p.2 := by decide
  have hgood :
      (TinylambOpt.ResearchGoals.ResourcePolicies.SlabReuse.place [3, 7, 5] [4, 5, 3, 3]).isSome = true := by
    decide
  have hbad :
      (TinylambOpt.ResearchGoals.ResourcePolicies.SlabReuse.place [4, 7, 5] [4, 5, 3, 3]).isSome = false := by
    decide
  have hc := h [3, 7, 5] [4, 7, 5] [4, 5, 3, 3] hlen hle hgood
  rw [hbad] at hc
  contradiction
end TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.SlabReuse.Goal_LargerArenaTraceMonotonicity
