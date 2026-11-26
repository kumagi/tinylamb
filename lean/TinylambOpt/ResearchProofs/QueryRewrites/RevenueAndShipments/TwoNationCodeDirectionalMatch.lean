import TinylambOpt.ResearchGoals.QueryRewrites.RevenueAndShipments

/-!
# TwoNationCodeDirectionalMatch

An encoding for two distinct nations preserves the two opposite-direction pairs. The
join filter can compare compact codes.
-/


set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.QueryRewrites.RevenueAndShipments.TwoNationCodeDirectionalMatch

open TinylambOpt.ResearchGoals.QueryRewrites

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.RevenueAndShipments.TwoNationCodeDirectionalMatch.Goal := by
  intro rows a b hab
  induction rows with
  | nil => simp
  | cons r rs ih =>
    simp only [List.filter_cons]
    rcases r with ⟨s,c⟩
    simp only
    by_cases hs_a : s == a <;> by_cases hs_b : s == b <;>
      by_cases hc_a : c == a <;> by_cases hc_b : c == b <;> simp_all

end TinylambOpt.ResearchProofs.QueryRewrites.RevenueAndShipments.TwoNationCodeDirectionalMatch

#print axioms TinylambOpt.ResearchProofs.QueryRewrites.RevenueAndShipments.TwoNationCodeDirectionalMatch.sound
