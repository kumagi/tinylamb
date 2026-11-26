import TinylambOpt.ResearchGoals.QueryRewrites.EncodedScanAggregates

set_option linter.unusedSimpArgs false
set_option linter.deprecated false


/-!
# ConstantTitlePagePopulation

A certified constant nonempty title contributes the population of the survivor
mask without any per-row title lookup. When every title in the group is the same
nonempty constant, filtering by survivor mask yields that constant replicated by
the survivor count.
-/

namespace TinylambOpt.ResearchProofs.QueryRewrites.EncodedScanAggregates.Goal_ConstantTitlePagePopulation

open TinylambOpt.ResearchGoals.QueryRewrites

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.EncodedScanAggregates.ConstantTitlePagePopulation.Goal := by
  intro titles label hne hall
  induction titles with
  | nil => simp [countWhere]
  | cons r rs ih =>
      obtain ⟨s, b⟩ := r
      have hs : s = label := hall (s, b) (by simp)
      subst hs
      have ih' := ih (fun r hr => hall r (by simp [hr]))
      cases b <;> simp only [countWhere, List.filterMap_cons, List.filter_cons, Prod.snd,
        Bool.true_and, Bool.false_and, bne_iff_ne, ne_eq, ite_true, ite_false,
        List.replicate_succ, List.length_cons, List.nil_append, ↓reduceIte] at * <;>
        simp_all


end TinylambOpt.ResearchProofs.QueryRewrites.EncodedScanAggregates.Goal_ConstantTitlePagePopulation
