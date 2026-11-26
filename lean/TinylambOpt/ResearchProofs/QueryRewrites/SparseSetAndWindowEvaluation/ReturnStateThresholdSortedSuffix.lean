import TinylambOpt.ResearchGoals.QueryRewrites.SparseSetAndWindowEvaluation

/-!
# ReturnStateThresholdSortedSuffix
-/

set_option maxHeartbeats 0

namespace TinylambOpt.ResearchProofs.QueryRewrites.SparseSetAndWindowEvaluation.ReturnStateThresholdSortedSuffix
open TinylambOpt.ResearchGoals.QueryRewrites.SparseSetAndWindowEvaluation.ReturnStateThresholdSortedSuffix

theorem sound : Goal := by
  intro xs hsorted
  dsimp only
  have hgen : ∀ (l : List Int), l.Pairwise (· ≤ ·) →
      l.dropWhile (fun v => decide (5 * (xs.length : Int) * v ≤ 6 * xs.sum)) =
        l.filter (fun v => decide (6 * xs.sum < 5 * (xs.length : Int) * v)) := by
    intro l hl
    induction l with
    | nil => simp
    | cons a as ih =>
        have hpair : ∀ x ∈ as, a ≤ x := fun x hx => List.rel_of_pairwise_cons hl hx
        by_cases ha : 5 * (xs.length : Int) * a ≤ 6 * xs.sum
        · have hdrop : (a :: as).dropWhile (fun v => decide (5 * (xs.length : Int) * v ≤ 6 * xs.sum)) =
              as.dropWhile (fun v => decide (5 * (xs.length : Int) * v ≤ 6 * xs.sum)) := by
            simp [List.dropWhile_cons, ha]
          have hfilter : (a :: as).filter (fun v => decide (6 * xs.sum < 5 * (xs.length : Int) * v)) =
              as.filter (fun v => decide (6 * xs.sum < 5 * (xs.length : Int) * v)) := by
            have : ¬ (6 * xs.sum < 5 * (xs.length : Int) * a) := by omega
            simp [List.filter_cons, decide_eq_true_eq, this]
          rw [hdrop, hfilter]
          exact ih hl.tail
        · have ha' : 6 * xs.sum < 5 * (xs.length : Int) * a := by omega
          have hdrop : (a :: as).dropWhile (fun v => decide (5 * (xs.length : Int) * v ≤ 6 * xs.sum)) = a :: as := by
            rw [List.dropWhile_cons]
            simp [ha]
          have hfilter : (a :: as).filter (fun v => decide (6 * xs.sum < 5 * (xs.length : Int) * v)) = a :: as := by
            rw [List.filter_eq_self]
            intro x hx
            simp only [decide_eq_true_eq]
            cases hx with
            | head => exact ha'
            | tail _ hx' =>
                have hax : a ≤ x := hpair x hx'
                have hw0 : 0 ≤ 5 * (xs.length : Int) := by omega
                have hmul : 5 * (xs.length : Int) * a ≤ 5 * (xs.length : Int) * x :=
                  Int.mul_le_mul_of_nonneg_left hax hw0
                generalize hA : 5 * (xs.length : Int) * a = A at hmul ha' ⊢
                generalize hG : 5 * (xs.length : Int) * x = G at hmul ha' ⊢
                omega
          rw [hdrop, hfilter]
  exact hgen xs hsorted

#print axioms sound
end TinylambOpt.ResearchProofs.QueryRewrites.SparseSetAndWindowEvaluation.ReturnStateThresholdSortedSuffix
