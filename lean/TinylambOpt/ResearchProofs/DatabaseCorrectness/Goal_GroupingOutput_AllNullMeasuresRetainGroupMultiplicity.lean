import TinylambOpt.ResearchGoals.DatabaseCorrectness.GroupingOutput

/-! All-NULL measures keep the exact COUNT(*) multiplicity while SUM stays
    NULL; an empty partition is never counted as a row. -/
set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.DatabaseCorrectness.GroupingOutput.Goal_AllNullMeasuresRetainGroupMultiplicity

open TinylambOpt.ResearchGoals.DatabaseCorrectness.GroupingOutput
open TinylambOpt.ResearchGoals.QueryRewrites (nsum forKey)

private theorem nsum_all_none (xs : List (Option Int)) (h : xs.all Option.isNone = true) :
    nsum xs = none := by
  induction xs with
  | nil => simp [nsum]
  | cons x xs ih =>
      have hx : x = none := by
        have := (List.all_eq_true.mp h) x (by simp)
        simpa using this
      have hxs : xs.all Option.isNone = true := by
        apply List.all_eq_true.mpr
        intro y hy
        have hy' := (List.all_eq_true.mp h) y (by simp [hy])
        simpa using hy'
      rw [show nsum (x :: xs) = nsum xs by simp [nsum, hx]]
      exact ih hxs

private theorem forKey_length_eq_count (rs : Input) (k : Key) :
    (forKey rs k).length = (rs.map Prod.fst).count k := by
  rw [List.count_eq_length_filter]
  induction rs with
  | nil => rfl
  | cons r rs ih =>
      simp [forKey] at ih
      by_cases h : r.fst == k <;> simp [forKey, h, ih]

theorem sound : AllNullMeasuresRetainGroupMultiplicity.Goal := by
  intro rs k h
  constructor
  · simpa [detail] using nsum_all_none (forKey rs k) h
  · simp [detail, forKey_length_eq_count]

end TinylambOpt.ResearchProofs.DatabaseCorrectness.GroupingOutput.Goal_AllNullMeasuresRetainGroupMultiplicity
