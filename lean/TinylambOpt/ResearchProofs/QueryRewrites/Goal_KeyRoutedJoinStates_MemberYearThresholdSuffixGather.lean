import TinylambOpt.ResearchGoals.QueryRewrites.KeyRoutedJoinStates

/-!
# Goal_KeyRoutedJoinStates_MemberYearThresholdSuffixGather

When forum memberships are sorted by joining year, dropping the sub-threshold
prefix retains exactly the memberships whose year reaches 2010. The weighted
suffix sum therefore equals the masked full sum.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.QueryRewrites.Goal_KeyRoutedJoinStates_MemberYearThresholdSuffixGather

open TinylambOpt.ResearchGoals.QueryRewrites

private theorem mask_sum_of_all_ge (rs : List (Nat × Nat)) (f : Nat → Nat)
    (h : ∀ t ∈ rs, 2010 ≤ t.1) :
    (rs.map (fun t => if 2010 ≤ t.1 then f t.2 else 0)).sum =
      (rs.map (fun t => f t.2)).sum := by
  induction rs with
  | nil => simp
  | cons t ts ih =>
      have ht : 2010 ≤ t.1 := h t (by simp)
      have hts : ∀ u ∈ ts, 2010 ≤ u.1 := by
        intro u hu
        exact h u (by simp [hu])
      simp [ht, ih hts]

theorem sound :
    TinylambOpt.ResearchGoals.QueryRewrites.KeyRoutedJoinStates.MemberYearThresholdSuffixGather.Goal := by
  intro memberships personMatches hsorted
  dsimp only [TinylambOpt.ResearchGoals.QueryRewrites.KeyRoutedJoinStates.MemberYearThresholdSuffixGather.Goal]
  induction memberships with
  | nil => simp
  | cons r rs ih =>
      rcases r with ⟨yr, p⟩
      change List.Pairwise (fun a b : Nat => a ≤ b) (yr :: rs.map Prod.fst) at hsorted
      rw [List.pairwise_cons] at hsorted
      have htail_sorted : (rs.map Prod.fst).Pairwise (fun a b => a ≤ b) := hsorted.2
      by_cases hyr : yr < 2010
      · have hyedge : (decide (yr < 2010)) = true := by simp [hyr]
        have ih' := ih htail_sorted
        have hnotge : ¬ 2010 ≤ yr := by omega
        simpa [hyedge, hnotge] using ih'
      · have hyge : 2010 ≤ yr := by omega
        have hall : ∀ t ∈ rs, 2010 ≤ t.1 := by
          intro t ht
          have hle : yr ≤ t.1 := hsorted.1 t.1 (List.mem_map.mpr ⟨t, ht, rfl⟩)
          omega
        have hmask := mask_sum_of_all_ge rs personMatches hall
        simp [hyr, hyge, hmask]

end TinylambOpt.ResearchProofs.QueryRewrites.Goal_KeyRoutedJoinStates_MemberYearThresholdSuffixGather
