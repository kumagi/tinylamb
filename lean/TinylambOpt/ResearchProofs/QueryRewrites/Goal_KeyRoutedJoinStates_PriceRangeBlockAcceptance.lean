import TinylambOpt.ResearchGoals.QueryRewrites.KeyRoutedJoinStates

/-!
# Goal_KeyRoutedJoinStates_PriceRangeBlockAcceptance

Disjoint, full and mixed price pages can use different read paths while
returning the same item IDs in physical order. The min/max summary decides
which path is equivalent to the exact price filter.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.QueryRewrites.Goal_KeyRoutedJoinStates_PriceRangeBlockAcceptance

open TinylambOpt.ResearchGoals.QueryRewrites

private theorem read_page_eq_filter (p : List (Int × Nat)) :
    (if (p.map Prod.fst).min?.any (fun x => 100 < x) then p
     else if (p.map Prod.fst).max?.any (fun x => x ≤ 100) then []
     else p.filter (fun r => 100 < r.1)) = p.filter (fun r => 100 < r.1) := by
  cases hmin : (p.map Prod.fst).min? with
  | none =>
      rw [List.min?_eq_none_iff] at hmin
      have hp : p = [] := by
        apply List.eq_nil_iff_forall_not_mem.mpr
        intro r hr
        have : r.1 ∈ p.map Prod.fst := List.mem_map.mpr ⟨r, hr, rfl⟩
        simpa [hmin] using this
      subst p
      simp
  | some mn =>
      have hmn : mn ∈ p.map Prod.fst ∧ ∀ b ∈ p.map Prod.fst, mn ≤ b :=
        List.min?_eq_some_iff.mp hmin
      by_cases hlow : 100 < mn
      · have hf : p.filter (fun r => 100 < r.1) = p := by
          apply List.filter_eq_self.mpr
          intro r hr
          have hbmn : mn ≤ r.1 := hmn.2 r.1 (List.mem_map.mpr ⟨r, hr, rfl⟩)
          have hgt : 100 < r.1 := Int.lt_of_lt_of_le hlow hbmn
          simp [hgt]
        simp [hlow, hf]
      · have hnlow : (p.map Prod.fst).min?.any (fun x => 100 < x) = false := by simp [hmin, hlow]
        cases hmax : (p.map Prod.fst).max? with
        | none =>
            rw [List.max?_eq_none_iff] at hmax
            have hp : p = [] := by
              apply List.eq_nil_iff_forall_not_mem.mpr
              intro r hr
              have : r.1 ∈ p.map Prod.fst := List.mem_map.mpr ⟨r, hr, rfl⟩
              simpa [hmax] using this
            subst p
            simp at hmin
        | some mx =>
            have hmx : mx ∈ p.map Prod.fst ∧ ∀ b ∈ p.map Prod.fst, b ≤ mx :=
              List.max?_eq_some_iff.mp hmax
            by_cases hhigh : mx ≤ 100
            · have hf : p.filter (fun r => 100 < r.1) = [] := by
                apply List.eq_nil_iff_forall_not_mem.mpr
                intro r hr
                have hr' := List.mem_filter.mp hr
                have hbmx : r.1 ≤ mx := hmx.2 r.1 (List.mem_map.mpr ⟨r, hr'.1, rfl⟩)
                have : 100 < r.1 := by simpa using hr'.2
                omega
              simp [hlow, hhigh, hf]
            · simp [hlow, hhigh]

theorem sound :
    TinylambOpt.ResearchGoals.QueryRewrites.KeyRoutedJoinStates.PriceRangeBlockAcceptance.Goal := by
  intro pages
  dsimp only [TinylambOpt.ResearchGoals.QueryRewrites.KeyRoutedJoinStates.PriceRangeBlockAcceptance.Goal]
  induction pages with
  | nil => simp
  | cons p ps ih =>
      simp only [List.flatMap_cons]
      rw [read_page_eq_filter]
      simpa using ih

end TinylambOpt.ResearchProofs.QueryRewrites.Goal_KeyRoutedJoinStates_PriceRangeBlockAcceptance
