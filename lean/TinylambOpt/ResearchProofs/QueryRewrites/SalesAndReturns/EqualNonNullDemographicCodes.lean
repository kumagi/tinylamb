import TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns

/-!
# EqualNonNullDemographicCodes

Equality of canonical non-NULL demographic codes represents equality of the
corresponding attributes under the model's assumptions.

The join predicate can compare shared codes rather than comparing both attribute strings
on every pair.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false


namespace TinylambOpt.ResearchProofs.QueryRewrites.SalesAndReturns.EqualNonNullDemographicCodes
open TinylambOpt.ResearchGoals.QueryRewrites
open TinylambOpt.ResearchGoals.QueryRewrites
open TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns.EqualNonNullDemographicCodes.Goal := by
  intro a b code hinj
  cases a with
  | none => cases b <;> simp [hit]
  | some pa =>
    cases b with
    | none => simp [hit]
    | some pb =>
      simp only [hit, Option.map_some, Option.isSome_some, Bool.true_and]
      by_cases h : pa = pb
      · subst pb
        simp
      · have hne : code pa ≠ code pb := by
          intro heq
          exact h (hinj pa pb heq)
        simp [beq_false_of_ne hne, beq_false_of_ne h]
end TinylambOpt.ResearchProofs.QueryRewrites.SalesAndReturns.EqualNonNullDemographicCodes
