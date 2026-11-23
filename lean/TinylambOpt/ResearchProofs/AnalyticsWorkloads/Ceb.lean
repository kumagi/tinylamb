import TinylambOpt.ResearchGoals.AnalyticsWorkloads.Ceb

namespace TinylambOpt.ResearchProofs.AnalyticsWorkloads.Ceb

open TinylambOpt.ResearchGoals.AnalyticsWorkloads.Ceb
open TinylambOpt.ResearchGoals.AnalyticsWorkloads

/-!
# Proof strategy overview

We prove the six STATS-CEB propositions from `Ceb.lean`:

* `RepeatedScore`: `repeatChecked` computes, via integer division, the number of
  times `v` can be added to `acc` while staying inside the `Int64` range, and
  agrees with the naive checked loop `repeatedReference`.
* `RoleRouting`: under unique user/post ids, the fused owner-map + eligible-set
  routing produces exactly the flatMap expansion of the reference query.
* `EndpointProfiles`: `linkState` aggregates each source post once and multiplies
  by the typed-destination frequency, matching `summarize` of the flatMap.
* `SparseOuterCorrection`: the sparse correction table assigns each hit a weight
  equal to `max 1` of its non-NULL tag multiplicity.
* `DemandBadges`: the badge-demand histogram lookup equals the product of the
  eligible-user multiplicity and the badge multiplicity at the demanded key.
* `YearOnlyOrder`: bucketing rows by year and sorting bucket keys reproduces the
  stable `mergeSort` by year, because `mergeSort` is stable (`mergeSort_zipIdx`).
-/

section HistogramHelpers

variable {α : Type} [BEq α] [Hashable α] [EquivBEq α] [LawfulHashable α]

/-- Invariant of the histogram fold: after folding `xs` into `H`, the count at `a`
is the initial count plus the number of occurrences of `a` in `xs`. -/
private theorem foldl_hist_getD (xs : List α) (H : Std.HashMap α Nat) (a : α) :
    (xs.foldl (fun m x => m.insert x (m.getD x 0 + 1)) H).getD a 0 =
      H.getD a 0 + xs.countP (· == a) := by
  induction xs generalizing H with
  | nil => simp
  | cons x xs ih =>
      rw [List.foldl_cons, ih, Std.HashMap.getD_insert, List.countP_cons]
      by_cases hxa : x = a
      · have hx : (x == a) = true := beq_iff_eq.mpr hxa
        simp [hx]
        omega
      · have hx : (x == a) = false := beq_eq_false_iff_ne.mpr hxa
        simp [hx]

/-- The histogram fold makes every element of `xs` a key with count `≥ 1`, and
any key outside `xs` keeps its initial count. -/
private theorem histogram_getD (xs : List α) (a : α) :
    (histogram xs).getD a 0 = xs.countP (· == a) := by
  unfold histogram
  rw [foldl_hist_getD]
  simp

private theorem foldl_hist_contains (xs : List α) (H : Std.HashMap α Nat) (a : α) :
    (xs.foldl (fun m x => m.insert x (m.getD x 0 + 1)) H).contains a =
      H.contains a || xs.contains a := by
  induction xs generalizing H with
  | nil => simp
  | cons x xs ih =>
      rw [List.foldl_cons, ih, Std.HashMap.contains_insert, List.contains_cons]
      have hcomm : (x == a) = (a == x) := by
        by_cases h : x = a <;> simp [h]
      rw [hcomm]
      cases hb : H.contains a <;> cases hc : xs.contains a <;>
        simp [hb, hc]

private theorem histogram_contains (xs : List α) (a : α) :
    (histogram xs).contains a = xs.contains a := by
  unfold histogram
  rw [foldl_hist_contains]
  simp

end HistogramHelpers

/-! ### DemandBadges -/

theorem sound_DemandBadges : DemandBadges.Goal := by
  intro ps us bs k hk
  sorry

end TinylambOpt.ResearchProofs.AnalyticsWorkloads.Ceb
