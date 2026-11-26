import TinylambOpt.ResearchGoals.QueryRewrites.EncodedScanAggregates

set_option linter.unusedSimpArgs false
set_option linter.deprecated false


/-!
# DefaultLaneSumWithPatches

A constant base and signed corrections at exceptional lanes suffice for a
non-null integer sum.
-/

namespace TinylambOpt.ResearchProofs.QueryRewrites.EncodedScanAggregates.Goal_DefaultLaneSumWithPatches

open TinylambOpt.ResearchGoals.QueryRewrites

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.EncodedScanAggregates.DefaultLaneSumWithPatches.Goal := by
  intro ids base
  induction ids with
  | nil => simp [total]
  | cons a as ih =>
      simp only [List.sum_cons, List.length_cons, List.filter_cons, List.map_cons, List.sum_cons, total]
      have ih' : as.sum = (as.length : Int) * base + (List.map (fun x => x - base) (List.filter (fun x => x != base) as)).sum := by
        simpa [total] using ih
      have hlen : ((as.length + 1 : Nat) : Int) = (as.length : Int) + 1 := by omega
      by_cases h : a = base
      · subst h
        simp only [bne_self_eq_false, Bool.false_eq_true, ↓reduceIte, ih']
        rw [hlen, Int.add_mul, Int.one_mul]
        omega
      · have hb : (a != base) = true := by simp [bne_iff_ne, h]
        simp only [hb, ↓reduceIte, List.map_cons, List.sum_cons, ih']
        rw [hlen, Int.add_mul, Int.one_mul]
        omega


end TinylambOpt.ResearchProofs.QueryRewrites.EncodedScanAggregates.Goal_DefaultLaneSumWithPatches
