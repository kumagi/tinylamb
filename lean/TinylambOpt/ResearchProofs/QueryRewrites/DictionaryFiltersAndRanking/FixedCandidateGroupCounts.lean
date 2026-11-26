import TinylambOpt.ResearchGoals.QueryRewrites.DictionaryFiltersAndRanking

/-!
# FixedCandidateGroupCounts

After fixing the first ten groups by encounter order, later rows can update only those
groups. The executor can cap the modeled group-count state.
-/


set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.QueryRewrites.DictionaryFiltersAndRanking.FixedCandidateGroupCounts
open TinylambOpt.ResearchGoals.QueryRewrites
open TinylambOpt.ResearchGoals.QueryRewrites.DictionaryFiltersAndRanking

theorem map_count_filter {α : Type} [BEq α] [LawfulBEq α]
    (xs s : List α) (p : α → Bool) (hp : ∀ a ∈ s, p a = true) :
    s.map (fun k => (xs.filter p).count k) = s.map xs.count := by
  induction s with
  | nil => simp
  | cons a s ih =>
      have hp' : ∀ b ∈ s, p b = true := by
        intro b hb
        exact hp b (by simp [hb])
      have ha : p a = true := hp a (by simp)
      have ih' := ih hp'
      simp [List.count_filter ha, ih']

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.DictionaryFiltersAndRanking.FixedCandidateGroupCounts.Goal := by
  intro xs
  dsimp only
  apply map_count_filter
  intro a ha
  simpa using ha
end TinylambOpt.ResearchProofs.QueryRewrites.DictionaryFiltersAndRanking.FixedCandidateGroupCounts
