import TinylambOpt.ResearchGoals.QueryRewrites.DictionaryFiltersAndRanking

/-!
# PageFrequencyUpperBoundExcludesTopK

A group's current URL count plus all unread rows bounds its final count. The executor
can discard groups that cannot reach the top-ten cutoff.
-/


set_option maxHeartbeats 0
namespace TinylambOpt.ResearchProofs.QueryRewrites.DictionaryFiltersAndRanking.PageFrequencyUpperBoundExcludesTopK
open TinylambOpt.ResearchGoals.QueryRewrites.DictionaryFiltersAndRanking

theorem count_page_le (page : List Nat) (i : Nat) :
    page.count i ≤ if page.contains i then page.length else 0 := by
  by_cases hmem : i ∈ page
  · have hc : page.contains i = true := by simpa using hmem
    simp [hmem, hc, List.count_le_length]
  · have hc : page.contains i = false := by simpa using hmem
    simp [hmem, hc, List.count_eq_zero.mpr hmem]

theorem count_upper : ∀ (seen : List Nat) (remaining : List (List Nat)) (i : Nat),
    (seen ++ remaining.flatten).count i ≤
      seen.count i + (remaining.map (fun page => if page.contains i then page.length else 0)).sum := by
  intro seen remaining
  induction remaining generalizing seen with
  | nil => intro i; simp
  | cons p ps ih =>
      intro i
      have hih := ih (seen ++ p) i
      simp only [List.flatten_cons, List.count_append, List.map_cons, List.sum_cons] at hih ⊢
      have hp := count_page_le p i
      omega

theorem filter_length_mono {α : Type} : ∀ (xs : List α) (p q : α → Bool),
    (∀ x ∈ xs, p x = true → q x = true) →
    (xs.filter p).length ≤ (xs.filter q).length := by
  intro xs
  induction xs with
  | nil => intro p q h; simp
  | cons x xs ih =>
      intro p q hsub
      by_cases hp : p x = true
      · have hq : q x = true := hsub x (by simp) hp
        have htail := ih p q (by
          intro y hy hyp
          exact hsub y (by simp [hy]) hyp)
        simp [hp, hq]
        omega
      · by_cases hq : q x = true
        · have htail := ih p q (by
            intro y hy hyp
            exact hsub y (by simp [hy]) hyp)
          simp [hp, hq]
          omega
        · simp [hp, hq]
          exact ih p q (by
            intro y hy hyp
            exact hsub y (by simp [hy]) hyp)

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.DictionaryFiltersAndRanking.PageFrequencyUpperBoundExcludesTopK.Goal := by
  intro seen remaining keys i threshold hkeys
  dsimp only
  intro hcount hupper
  simp only [TinylambOpt.ResearchGoals.QueryRewrites.countWhere] at hcount ⊢
  let allRows := seen ++ remaining.flatten
  have hub : allRows.count i ≤
      seen.count i + (remaining.map (fun page => if page.contains i then page.length else 0)).sum := by
    dsimp [allRows]
    exact count_upper seen remaining i
  have hsub : ∀ k ∈ keys, (decide (threshold ≤ seen.count k) = true) →
      (decide (allRows.count i < allRows.count k) = true) := by
    intro k hk hth
    have hle : seen.count k ≤ allRows.count k := by
      dsimp [allRows]
      rw [List.count_append]
      omega
    have hth' : threshold ≤ seen.count k := by simpa using hth
    have hgoal : allRows.count i < allRows.count k := by omega
    simpa using hgoal
  have hmono := filter_length_mono keys (fun k => decide (threshold ≤ seen.count k))
    (fun k => decide (allRows.count i < allRows.count k)) hsub
  change 10 ≤ (keys.filter (fun k => decide (allRows.count i < allRows.count k))).length
  omega

end TinylambOpt.ResearchProofs.QueryRewrites.DictionaryFiltersAndRanking.PageFrequencyUpperBoundExcludesTopK

-- C++ references to this model or its proof.
-- executor/detail/benchmark_rematch.hpp
