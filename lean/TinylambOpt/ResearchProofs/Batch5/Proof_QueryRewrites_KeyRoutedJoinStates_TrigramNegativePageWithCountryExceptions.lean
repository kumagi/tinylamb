import TinylambOpt.ResearchGoals.QueryRewrites.KeyRoutedJoinStates

namespace TinylambOpt.ResearchProofs.Batch5.QueryRewrites.KeyRoutedJoinStates.Goal_TrigramNegativePageWithCountryExceptions

open TinylambOpt.ResearchGoals.QueryRewrites.KeyRoutedJoinStates


/-! A page that rules out the required trigram cannot pass the text arm, so
    the country predicate alone decides which rows survive. -/

theorem contains_dat_of_contains_data (s : String) :
    s.contains "data" = true → s.contains "dat" = true := by
  intro h
  rw [String.contains_string_iff] at h ⊢
  exact (by decide : "dat".toList <:+: "data".toList).trans h

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.KeyRoutedJoinStates.TrigramNegativePageWithCountryExceptions.Goal := by
  intro page hneg
  induction page with
  | nil => simp
  | cons r rs ih =>
      have hnegRest : ∀ q ∈ rs, q.2.contains "dat" = false := by
        intro q hq
        exact hneg q (by simp [hq])
      have hfalse : r.2.contains "data" = false := by
        by_cases h : r.2.contains "data" = true
        · have hdat := contains_dat_of_contains_data r.2 h
          have hf := hneg r (by simp)
          rw [hf] at hdat
          cases hdat
        · exact Bool.eq_false_of_ne_true h
      rw [List.filter_cons, List.filter_cons]
      simp [hfalse, ih hnegRest]
end TinylambOpt.ResearchProofs.Batch5.QueryRewrites.KeyRoutedJoinStates.Goal_TrigramNegativePageWithCountryExceptions
