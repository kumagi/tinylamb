import TinylambOpt.ResearchGoals.QueryRewrites.DictionaryFiltersAndRanking

/-!
# BlockExtremaPreservePointCount

A block with UserID bounds that exclude the search target contributes no matches. The
scan can omit that block's decode.
-/


set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.QueryRewrites.DictionaryFiltersAndRanking.BlockExtremaPreservePointCount
open Std
open TinylambOpt.ResearchGoals.QueryRewrites
open TinylambOpt.ResearchGoals.QueryRewrites.DictionaryFiltersAndRanking

theorem min_le_of_mem (b : List Int) {k : Int} (hk : k ∈ b) :
    (b.min?).any (fun x => x ≤ k) = true := by
  cases hmin : b.min? with
  | none =>
      have hs := List.isSome_min?_of_mem hk
      simp [hmin] at hs
  | some m =>
      have hm := (List.min?_eq_some_iff.mp hmin).2 k hk
      simp [hmin, hm]

theorem le_max_of_mem (b : List Int) {k : Int} (hk : k ∈ b) :
    (b.max?).any (fun x => k ≤ x) = true := by
  cases hmax : b.max? with
  | none =>
      have hs := List.isSome_max?_of_mem hk
      simp [hmax] at hs
  | some m =>
      have hm := (List.max?_eq_some_iff.mp hmax).2 k hk
      simp [hmax, hm]

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.DictionaryFiltersAndRanking.BlockExtremaPreservePointCount.Goal := by
  intro blocks k
  induction blocks with
  | nil => simp
  | cons b bs ih =>
      by_cases hc : ((b.min?).any (fun x => x ≤ k) && (b.max?).any (fun x => k ≤ x)) = true
      · simp [hc, ih, List.count_append]
      · have hnotmem : k ∉ b := by
          intro hk
          have hmin := min_le_of_mem b hk
          have hmax := le_max_of_mem b hk
          simp [hmin, hmax] at hc
        have hcount : b.count k = 0 := List.count_eq_zero.mpr hnotmem
        simp [hc, ih, hcount, List.count_append]
end TinylambOpt.ResearchProofs.QueryRewrites.DictionaryFiltersAndRanking.BlockExtremaPreservePointCount
