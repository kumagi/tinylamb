import TinylambOpt.ResearchGoals.QueryRewrites.AnalyticalJoins

/-!
# CountryYearSourceSemijoin

Restricting the edge scan to source people who match the country and year predicates
preserves the qualifying edges. The executor can discard other edges before aggregation.
-/


set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.QueryRewrites.AnalyticalJoins.CountryYearSourceSemijoin

theorem filter_eq_nil_of_not_mem (l : List Nat) (k : Nat) (h : ¬ k ∈ l) : l.filter (fun x => x == k) = [] := by
  induction l with
  | nil => simp
  | cons a t ih =>
      by_cases heq : (a == k) = true
      · have hak : a = k := by simpa using heq
        exact False.elim (h (by simp [hak]))
      · rw [List.filter_cons, if_neg heq]
        apply ih
        intro hk
        exact h (by simp [hk])

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.AnalyticalJoins.CountryYearSourceSemijoin.Goal := by
  intro edges peopleInCountry
  dsimp only
  induction edges with
  | nil => simp
  | cons k t ih =>
      by_cases hk : k ∈ peopleInCountry
      · have he : k ∈ peopleInCountry.eraseDups := (List.mem_eraseDups).2 hk
        simp [hk, he, ih]
      · have he : ¬ k ∈ peopleInCountry.eraseDups := by simpa [List.mem_eraseDups]
        have hnil := filter_eq_nil_of_not_mem peopleInCountry k hk
        simp [hk, he, hnil, ih]

end TinylambOpt.ResearchProofs.QueryRewrites.AnalyticalJoins.CountryYearSourceSemijoin

#print axioms TinylambOpt.ResearchProofs.QueryRewrites.AnalyticalJoins.CountryYearSourceSemijoin.sound
