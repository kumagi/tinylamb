import TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns

/-!
# CandidateStatesPreserveAverageInputs

The candidate return states determine which state averages need to be built. Each state
uses all eligible values for its modeled average.
-/


set_option maxHeartbeats 0
namespace TinylambOpt.ResearchProofs.QueryRewrites.SalesAndReturns.CandidateStatesPreserveAverageInputs

theorem contains_eq_mem : ∀ (l : List Nat) (k : Nat), l.contains k = true ↔ k ∈ l := by
  intro l
  induction l with
  | nil => intro k; simp
  | cons a l ih => intro k; simp [ih]

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.SalesAndReturns.CandidateStatesPreserveAverageInputs.Goal := by
  intro states returns k hk
  rw [List.filter_filter]
  apply List.filter_congr
  intro r hr
  by_cases hk' : (r.1 == k) = true
  · have hrk : r.1 = k := by simpa using hk'
    have hc : states.eraseDups.contains r.1 = true := by
      rw [contains_eq_mem, List.mem_eraseDups, hrk]
      exact hk
    have hrmem : r.1 ∈ states := by rw [hrk]; exact hk
    simp [hk', hc, hrmem]
  · simp [hk']

end TinylambOpt.ResearchProofs.QueryRewrites.SalesAndReturns.CandidateStatesPreserveAverageInputs

#print axioms TinylambOpt.ResearchProofs.QueryRewrites.SalesAndReturns.CandidateStatesPreserveAverageInputs.sound
