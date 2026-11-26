import TinylambOpt.ResearchGoals.QueryRewrites.PartitionedAggregateStates

namespace TinylambOpt.ResearchProofs.Batch5.QueryRewrites.PartitionedAggregateStates.Goal_RangeWidthBoundOnVarianceNumerator

open TinylambOpt.ResearchGoals.QueryRewrites.PartitionedAggregateStates


/-!
The pairwise variance numerator is half the sum of squared pairwise differences.
Every difference is at most the range width, so twice the numerator is at most
`n * (n-1) * width^2`; clearing the positive divisor two gives the stated bound.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace Scratch.RangeWidthBoundOnVarianceNumerator

open TinylambOpt.ResearchGoals.QueryRewrites
open TinylambOpt.ResearchGoals.QueryRewrites.PartitionedAggregateStates

/-- The integer variance numerator used in the goal. -/
def vnum (xs : List Int) : Int :=
  (xs.length : Int) * (xs.map (fun v => v * v)).sum - xs.sum ^ 2

/-- Expanding the sum of squared differences from a fixed head. -/
theorem sum_sqdiff (x : Int) (ys : List Int) :
    ((ys.map (fun y => (x - y) ^ 2)).sum) =
      (ys.length : Int) * (x * x) - 2 * x * ys.sum + (ys.map (fun y => y * y)).sum := by
  induction ys with
  | nil => simp
  | cons y ys ih =>
      simp only [List.map_cons, List.sum_cons, List.length_cons]
      rw [ih]
      grind

/-- Doubling the numerator exposes one squared-difference term per tail element. -/
theorem vnum_cons (x : Int) (xs : List Int) :
    2 * vnum (x :: xs) = 2 * vnum xs + 2 * ((xs.map (fun y => (x - y) ^ 2)).sum) := by
  simp [vnum, sum_sqdiff]
  grind

/-- A difference between two range-bounded values is at most the range width. -/
theorem sq_le_range {x y lo hi : Int} (hlx : lo ≤ x) (hxh : x ≤ hi)
    (hly : lo ≤ y) (hyh : y ≤ hi) : (x - y) ^ 2 ≤ (hi - lo) ^ 2 := by
  have hw : 0 ≤ hi - lo := by omega
  rcases Int.le_total x y with hxy | hyx
  · have ha : 0 ≤ y - x := by omega
    have hab : y - x ≤ hi - lo := by omega
    have hsq := Int.mul_self_le_mul_self ha hab
    rw [Int.pow_succ, Int.pow_one, Int.pow_succ, Int.pow_one]
    have hneg : x - y = -(y - x) := by omega
    rw [hneg, Int.neg_mul_neg]
    exact hsq
  · have ha : 0 ≤ x - y := by omega
    have hab : x - y ≤ hi - lo := by omega
    have hsq := Int.mul_self_le_mul_self ha hab
    simpa [Int.pow_succ, Int.pow_one] using hsq

/-- Summing the squared differences from one head. -/
theorem sum_sqdiff_le (x : Int) (ys : List Int) (lo hi : Int)
    (hx : lo ≤ x ∧ x ≤ hi) (hys : ∀ y ∈ ys, lo ≤ y ∧ y ≤ hi) :
    (ys.map (fun y => (x - y) ^ 2)).sum ≤ (ys.length : Int) * (hi - lo) ^ 2 := by
  induction ys with
  | nil => simp
  | cons y ys ih =>
      have hy : lo ≤ y ∧ y ≤ hi := hys y (by simp)
      have htail : ∀ z ∈ ys, lo ≤ z ∧ z ≤ hi := by
        intro z hz
        exact hys z (by simp [hz])
      have iht := ih htail
      have hybound := sq_le_range hx.1 hx.2 hy.1 hy.2
      simp only [List.map_cons, List.sum_cons, List.length_cons]
      calc
        (x - y) ^ 2 + (List.map (fun y => (x - y) ^ 2) ys).sum
            ≤ (hi - lo) ^ 2 + (ys.length : Int) * (hi - lo) ^ 2 := Int.add_le_add hybound iht
        _ = (↑(ys.length + 1) : Int) * (hi - lo) ^ 2 := by grind

/-- Twice the variance numerator is bounded by the ordered-pair count times width squared. -/
theorem vnum_bound2 (xs : List Int) (lo hi : Int)
    (hb : ∀ v ∈ xs, lo ≤ v ∧ v ≤ hi) :
    2 * vnum xs ≤ (xs.length : Int) * ((xs.length : Int) - 1) * (hi - lo) ^ 2 := by
  induction xs with
  | nil => simp [vnum]
  | cons x xs ih =>
      have hx : lo ≤ x ∧ x ≤ hi := hb x (by simp)
      have htail : ∀ z ∈ xs, lo ≤ z ∧ z ≤ hi := by
        intro z hz
        exact hb z (by simp [hz])
      have iht := ih htail
      have hsum := sum_sqdiff_le x xs lo hi hx htail
      have hsum2 : 2 * (xs.map (fun y => (x - y) ^ 2)).sum ≤
          2 * ((xs.length : Int) * (hi - lo) ^ 2) :=
        Int.mul_le_mul_of_nonneg_left hsum (by decide : (0 : Int) ≤ 2)
      calc
        2 * vnum (x :: xs) =
            2 * vnum xs + 2 * ((xs.map (fun y => (x - y) ^ 2)).sum) := vnum_cons x xs
        _ ≤ (xs.length : Int) * ((xs.length : Int) - 1) * (hi - lo) ^ 2 +
              2 * ((xs.length : Int) * (hi - lo) ^ 2) := Int.add_le_add iht hsum2
        _ = (↑(xs.length + 1) : Int) * ((↑(xs.length + 1) : Int) - 1) * (hi - lo) ^ 2 := by
              grind

theorem sound : RangeWidthBoundOnVarianceNumerator.Goal := by
  intro xs lo hi hlo hb
  dsimp
  change vnum xs ≤ (xs.length : Int) * ((xs.length : Int) - 1) * (hi - lo) ^ 2 / 2
  rw [Int.le_ediv_iff_mul_le (by decide : (0 : Int) < 2)]
  have hbound := vnum_bound2 xs lo hi hb
  simpa [Int.mul_comm] using hbound

end Scratch.RangeWidthBoundOnVarianceNumerator
end TinylambOpt.ResearchProofs.Batch5.QueryRewrites.PartitionedAggregateStates.Goal_RangeWidthBoundOnVarianceNumerator
