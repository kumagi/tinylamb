import TinylambOpt.ResearchGoals.QueryRewrites.PartitionedAggregateStates

namespace TinylambOpt.ResearchProofs.QueryRewrites.PartitionedAggregateStates.Goal_CumulativeDelayCountersToExclusiveBins

open TinylambOpt.ResearchGoals.QueryRewrites.PartitionedAggregateStates


/-! Four cumulative delay thresholds are equivalent to disjoint histogram bins:
bin `i` is selected exactly when the delay crosses boundary `i` but not the
next one. Nullable inputs are already removed by `filterMap id`. The proof
converts each cumulative difference into a filtered count over the matching
half-open interval, then identifies that interval with the bucket predicate. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

open TinylambOpt.ResearchGoals.QueryRewrites

local notation "BS" => [30, 60, 90, 120]

/-- The cumulative count at a smaller threshold never exceeds the larger one. -/
theorem countWhere_le_of_le (ds : List Int) {s t : Int} (hst : s ≤ t) :
    countWhere ds (· ≤ s) ≤ countWhere ds (· ≤ t) := by
  induction ds with
  | nil => simp [countWhere]
  | cons d ds ih =>
      dsimp [countWhere] at ih ⊢
      simp only [List.filter_cons]
      by_cases hds : d ≤ s <;> by_cases hdt : d ≤ t <;> simp [hds, hdt] at ih ⊢ <;> omega

/-- The cumulative count at any threshold is bounded by the input length. -/
theorem countWhere_le_length (ds : List Int) (t : Int) : countWhere ds (· ≤ t) ≤ ds.length := by
  simpa [countWhere] using
    (List.Sublist.length_le (List.filter_sublist (p := fun d : Int => d ≤ t) (l := ds)))

/-- Bucket zero is exactly the delays at or below thirty. -/
theorem bucket_eq0 (d : Int) : bucket BS d = 0 ↔ d ≤ 30 := by
  by_cases h30 : 30 < d <;> by_cases h60 : 60 < d <;>
    by_cases h90 : 90 < d <;> by_cases h120 : 120 < d <;>
    simp [bucket, h30, h60, h90, h120] <;> omega

/-- Bucket one is the strictly positive range up to sixty. -/
theorem bucket_eq1 (d : Int) : bucket BS d = 1 ↔ 30 < d ∧ d ≤ 60 := by
  by_cases h30 : 30 < d <;> by_cases h60 : 60 < d <;>
    by_cases h90 : 90 < d <;> by_cases h120 : 120 < d <;>
    simp [bucket, h30, h60, h90, h120] <;> omega

/-- Bucket two is the strictly positive range from sixty to ninety. -/
theorem bucket_eq2 (d : Int) : bucket BS d = 2 ↔ 60 < d ∧ d ≤ 90 := by
  by_cases h30 : 30 < d <;> by_cases h60 : 60 < d <;>
    by_cases h90 : 90 < d <;> by_cases h120 : 120 < d <;>
    simp [bucket, h30, h60, h90, h120] <;> omega

/-- Bucket three is the strictly positive range from ninety to one hundred twenty. -/
theorem bucket_eq3 (d : Int) : bucket BS d = 3 ↔ 90 < d ∧ d ≤ 120 := by
  by_cases h30 : 30 < d <;> by_cases h60 : 60 < d <;>
    by_cases h90 : 90 < d <;> by_cases h120 : 120 < d <;>
    simp [bucket, h30, h60, h90, h120] <;> omega

/-- Bucket four collects all delays beyond the final threshold. -/
theorem bucket_eq4 (d : Int) : bucket BS d = 4 ↔ 120 < d := by
  by_cases h30 : 30 < d <;> by_cases h60 : 60 < d <;>
    by_cases h90 : 90 < d <;> by_cases h120 : 120 < d <;>
    simp [bucket, h30, h60, h90, h120] <;> omega

/-- Equal Boolean predicates give equal filtered counts. -/
theorem countWhere_congr {α : Type} (xs : List α) (p q : α → Bool)
    (h : ∀ x ∈ xs, p x = q x) : countWhere xs p = countWhere xs q := by
  unfold countWhere
  exact congrArg List.length (List.filter_congr h)

/-- Counts of a nested predicate differ by the count of the ring between them. -/
theorem count_diff (ds : List Int) {lo hi : Int} (hlohi : lo ≤ hi) :
    countWhere ds (· ≤ hi) - countWhere ds (· ≤ lo) =
      countWhere ds (fun d => lo < d ∧ d ≤ hi) := by
  induction ds with
  | nil => simp [countWhere]
  | cons d ds ih =>
      have hmono := countWhere_le_of_le ds hlohi
      dsimp [countWhere] at ih hmono ⊢
      by_cases hlo : lo < d
      · have hdlo : ¬ d ≤ lo := by omega
        by_cases hdh : d ≤ hi
        · simp [List.filter_cons, hlo, hdlo, hdh] at ih hmono ⊢
          omega
        · simp [List.filter_cons, hlo, hdh] at ih hmono ⊢
          omega
      · have hdlo : d ≤ lo := by omega
        have hdh : d ≤ hi := by omega
        have hlohi' : ¬ lo < d := by omega
        simp [List.filter_cons, hdlo, hdh, hlohi'] at ih hmono ⊢
        omega

/-- The tail bin is the delay count outside the final threshold. -/
theorem count_tail (ds : List Int) :
    ds.length - countWhere ds (· ≤ 120) = countWhere ds (fun d => 120 < d) := by
  induction ds with
  | nil => simp [countWhere]
  | cons d ds ih =>
      have hlen := countWhere_le_length ds 120
      dsimp [countWhere] at ih hlen ⊢
      by_cases hgt : 120 < d
      · have hdle : ¬ d ≤ 120 := by omega
        simp [List.filter_cons, hgt, hdle] at ih hlen ⊢
        omega
      · have hdle : d ≤ 120 := by omega
        simp [List.filter_cons, hgt, hdle] at ih hlen ⊢
        omega

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.PartitionedAggregateStates.CumulativeDelayCountersToExclusiveBins.Goal := by
  intro xs
  let ds := xs.filterMap id
  change [countWhere ds (· ≤ 30), countWhere ds (· ≤ 60) - countWhere ds (· ≤ 30),
      countWhere ds (· ≤ 90) - countWhere ds (· ≤ 60),
      countWhere ds (· ≤ 120) - countWhere ds (· ≤ 90),
      ds.length - countWhere ds (· ≤ 120)] =
    (List.range 5).map (fun i => countWhere ds (fun d => bucket BS d == i))
  rw [show (List.range 5).map (fun i => countWhere ds (fun d => bucket BS d == i)) =
      [countWhere ds (fun d => bucket BS d == 0), countWhere ds (fun d => bucket BS d == 1),
       countWhere ds (fun d => bucket BS d == 2), countWhere ds (fun d => bucket BS d == 3),
       countWhere ds (fun d => bucket BS d == 4)] by simp [List.range_succ]]
  simp
  constructor
  · exact countWhere_congr ds (fun d : Int => decide (d ≤ 30))
      (fun d : Int => bucket BS d == 0) (by
        intro d hd
        apply Bool.eq_iff_iff.mpr
        simp [bucket_eq0])
  constructor
  · rw [count_diff ds (show (30 : Int) ≤ 60 by omega)]
    exact countWhere_congr ds (fun d : Int => decide (30 < d ∧ d ≤ 60))
      (fun d : Int => bucket BS d == 1) (by
        intro d hd
        apply Bool.eq_iff_iff.mpr
        simp [bucket_eq1])
  constructor
  · rw [count_diff ds (show (60 : Int) ≤ 90 by omega)]
    exact countWhere_congr ds (fun d : Int => decide (60 < d ∧ d ≤ 90))
      (fun d : Int => bucket BS d == 2) (by
        intro d hd
        apply Bool.eq_iff_iff.mpr
        simp [bucket_eq2])
  constructor
  · rw [count_diff ds (show (90 : Int) ≤ 120 by omega)]
    exact countWhere_congr ds (fun d : Int => decide (90 < d ∧ d ≤ 120))
      (fun d : Int => bucket BS d == 3) (by
        intro d hd
        apply Bool.eq_iff_iff.mpr
        simp [bucket_eq3])
  · rw [count_tail ds]
    exact countWhere_congr ds (fun d : Int => decide (120 < d))
      (fun d : Int => bucket BS d == 4) (by
        intro d hd
        apply Bool.eq_iff_iff.mpr
        simp [bucket_eq4])
end TinylambOpt.ResearchProofs.QueryRewrites.PartitionedAggregateStates.Goal_CumulativeDelayCountersToExclusiveBins
