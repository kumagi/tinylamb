import TinylambOpt.ResearchGoals.QueryRewrites.AnalyticalJoins
import TinylambOpt.ResearchProofs.QueryRewrites.NullableSum

/-!
# ShortCircuitCountryOrSubstringCount

Counting US rows first and testing the substring only for other rows preserves the OR
count. The scan can avoid passing US rows to LIKE.
-/


set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.QueryRewrites.AnalyticalJoins.ShortCircuitCountryOrSubstringCount

open TinylambOpt.ResearchGoals.QueryRewrites
open TinylambOpt.ResearchProofs.QueryRewrites.NullableSum (nsum_cons_getD)

theorem len_parts : ∀ (rs : List (Bool × Bool × Option Int)),
    (rs.filter (fun r => r.1 || r.2.1)).length =
      (rs.filter Prod.fst).length + (rs.filter (fun r => !r.1 && r.2.1)).length := by
  intro rs
  induction rs with
  | nil => simp
  | cons r t ih =>
      rcases r with ⟨u, v, o⟩
      cases u <;> cases v <;> simp [ih] <;> omega

theorem nsum_parts : ∀ (rs : List (Bool × Bool × Option Int)),
    (nsum ((rs.filter (fun r => r.1 || r.2.1)).map (fun r => r.2.2))).getD 0 =
      (nsum ((rs.filter Prod.fst).map (fun r => r.2.2))).getD 0 +
        (nsum ((rs.filter (fun r => !r.1 && r.2.1)).map (fun r => r.2.2))).getD 0 := by
  intro rs
  induction rs with
  | nil => simp [nsum]
  | cons r t ih =>
      rcases r with ⟨u, v, o⟩
      cases u <;> cases v <;> simp [ih, nsum_cons_getD] <;> omega

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.AnalyticalJoins.ShortCircuitCountryOrSubstringCount.Goal := by
  intro rs
  dsimp only
  apply Prod.ext
  · exact len_parts rs
  · exact nsum_parts rs

end TinylambOpt.ResearchProofs.QueryRewrites.AnalyticalJoins.ShortCircuitCountryOrSubstringCount

#print axioms TinylambOpt.ResearchProofs.QueryRewrites.AnalyticalJoins.ShortCircuitCountryOrSubstringCount.sound

-- C++ references to this model or its proof.
-- executor/detail/benchmark_rematch.hpp
