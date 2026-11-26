import TinylambOpt.ResearchGoals.QueryRewrites.DictionaryFiltersAndRanking

/-!
# UniqueLeadingKeyImpliesSingletonGroups

A partition with unique WatchIDs has singleton groups. The executor can construct each
aggregate directly on a partition whose uniqueness is known.
-/


set_option maxHeartbeats 0
namespace TinylambOpt.ResearchProofs.QueryRewrites.DictionaryFiltersAndRanking.UniqueLeadingKeyImpliesSingletonGroups
open TinylambOpt.ResearchGoals.QueryRewrites.DictionaryFiltersAndRanking

theorem count_zero_of_fst_not_mem : ∀ (xs : List (Nat × Nat)) (k : Nat × Nat),
    k.1 ∉ xs.map Prod.fst → xs.count k = 0 := by
  intro xs
  induction xs with
  | nil => intro k h; simp
  | cons a xs ih =>
      intro k h
      simp only [List.map_cons, List.mem_cons] at h
      have hne : a.1 ≠ k.1 := by
        intro heq
        exact h (Or.inl heq.symm)
      have hneak : a ≠ k := by
        intro heq
        exact hne (congrArg Prod.fst heq)
      have hbeq : (a == k) = false := by
        simp [beq_iff_eq, hneak]
      simp [List.count_cons, hbeq, ih k (by intro hk; exact h (Or.inr hk))]

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.DictionaryFiltersAndRanking.UniqueLeadingKeyImpliesSingletonGroups.Goal := by
  intro xs hnodup k hk
  induction xs with
  | nil => simp at hk
  | cons a xs ih =>
      simp only [List.map_cons, List.nodup_cons] at hnodup
      rcases hnodup with ⟨hnot, htail⟩
      simp only [List.mem_cons] at hk
      rcases hk with hka | hk
      · subst k
        have hz : xs.count a = 0 := count_zero_of_fst_not_mem xs a hnot
        simp [List.count_cons, hz]
      · have hne : a.1 ≠ k.1 := by
          intro heq
          exact hnot (List.mem_map.mpr ⟨k, hk, heq.symm⟩)
        have hneak : a ≠ k := by
          intro heq
          exact hne (congrArg Prod.fst heq)
        have hbeq : (a == k) = false := by
          simp [beq_iff_eq, hneak]
        have hone : xs.count k = 1 := ih htail hk
        simp [List.count_cons, hbeq, hone]

end TinylambOpt.ResearchProofs.QueryRewrites.DictionaryFiltersAndRanking.UniqueLeadingKeyImpliesSingletonGroups
