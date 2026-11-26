import TinylambOpt.ResearchGoals.QueryRewrites.SparseSetAndWindowEvaluation

namespace TinylambOpt.ResearchProofs.Batch5.QueryRewrites.SparseSetAndWindowEvaluation.Goal_ReturnStateThresholdSortedSuffix

open TinylambOpt.ResearchGoals.QueryRewrites.SparseSetAndWindowEvaluation


/-!
# ReturnStateThresholdSortedSuffix

For ascending customer subtotals, the predicate `weight * v <= 6 * total` with a
non-negative weight is downward closed. Hence `dropWhile` removes exactly the
rows that the strict average threshold filter rejects, including ties at the
boundary. An executor may sort once and keep a single suffix of the partition.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unusedSimpArgs false

/-! On a sorted list, a downward-closed boolean predicate makes `dropWhile` and
the complement filter agree. -/
theorem dropWhile_eq_filter_of_pairwise (xs : List Int) (p : Int → Bool)
    (hmono : ∀ a b : Int, a ≤ b → p b = true → p a = true)
    (hs : xs.Pairwise (fun a b : Int => a ≤ b)) :
    xs.dropWhile p = xs.filter (fun v => !p v) := by
  induction xs with
  | nil => rfl
  | cons a t ih =>
      have hs' : (∀ b ∈ t, a ≤ b) ∧ t.Pairwise (fun b c : Int => b ≤ c) := by
        simpa using hs
      by_cases hpa : p a = true
      · rw [List.dropWhile_cons_of_pos hpa, ih hs'.2]
        simp [hpa]
      · rw [List.dropWhile_cons_of_neg hpa]
        have hpfalse : p a = false := by
          cases h : p a <;> simp_all
        have htfilter : t.filter (fun v => !p v) = t := by
          apply List.filter_eq_self.mpr
          intro b hb
          have hab : a ≤ b := hs'.1 b hb
          have hpb : ¬ p b = true := by intro hpb; exact hpa (hmono a b hab hpb)
          have hpbf : p b = false := by cases h : p b <;> simp_all
          simp [hpbf]
        simp [hpfalse, htfilter]

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.SparseSetAndWindowEvaluation.ReturnStateThresholdSortedSuffix.Goal := by
  intro xs hs
  dsimp only
  let weight : Int := 5 * xs.length
  let total : Int := 6 * xs.sum
  let p : Int → Bool := fun v => decide (weight * v ≤ total)
  have hW : 0 ≤ weight := by
    dsimp [weight]
    exact Int.mul_nonneg (by decide) (Int.natCast_nonneg xs.length)
  have hmono : ∀ a b : Int, a ≤ b → p b = true → p a = true := by
    intro a b hab hb
    dsimp [p] at hb ⊢
    simp only [decide_eq_true_eq] at hb ⊢
    have hmul : weight * a ≤ weight * b := Int.mul_le_mul_of_nonneg_left hab hW
    exact Int.le_trans hmul hb
  rw [dropWhile_eq_filter_of_pairwise xs p hmono hs]
  apply List.filter_congr
  intro v hv
  dsimp [p]
  change (!(decide (weight * v ≤ total))) = decide (total < weight * v)
  by_cases hle : weight * v ≤ total
  · have hnlt : ¬ total < weight * v := by omega
    simp [hle, hnlt]
  · have hlt : total < weight * v := by omega
    simp [hle, hlt]
end TinylambOpt.ResearchProofs.Batch5.QueryRewrites.SparseSetAndWindowEvaluation.Goal_ReturnStateThresholdSortedSuffix
