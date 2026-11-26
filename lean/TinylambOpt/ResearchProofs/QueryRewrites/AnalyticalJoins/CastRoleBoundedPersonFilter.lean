import TinylambOpt.ResearchGoals.QueryRewrites.AnalyticalJoins

/-!
# CastRoleBoundedPersonFilter

Restricting the female-person filter to IDs demanded by qualifying cast rows preserves
the modeled matches. The executor can allocate the name filter for the demanded IDs.
-/


set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.QueryRewrites.AnalyticalJoins.CastRoleBoundedPersonFilter

open TinylambOpt.ResearchGoals.QueryRewrites

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.AnalyticalJoins.CastRoleBoundedPersonFilter.Goal := by
  intro cast names lo hi hbound
  dsimp only
  induction cast with
  | nil => simp
  | cons k t ih =>
      have hk := hbound k (by simp)
      have hlo : lo ≤ k := hk.1
      have hhi : k ≤ hi := hk.2
      have ht : ∀ x ∈ t, lo ≤ x ∧ x ≤ hi := by
        intro x hx
        exact hbound x (by simp [hx])
      simp only [List.filter_cons]
      rw [ih ht]
      have hi_lt : k - lo < hi + 1 - lo := by omega
      have hget : (List.range (hi + 1 - lo))[k - lo]? = some (k - lo) := by
        simp [List.getElem?_range, hi_lt]
      have hbits :
          (((List.range (hi + 1 - lo)).map (fun i => names.contains (lo + i)))[k - lo]?) =
            some (names.contains k) := by
        rw [List.getElem?_map, hget]
        simp only [Option.map_some]
        have hadd : lo + (k - lo) = k := by omega
        rw [hadd]
      rw [hbits]
      simp

end TinylambOpt.ResearchProofs.QueryRewrites.AnalyticalJoins.CastRoleBoundedPersonFilter

#print axioms TinylambOpt.ResearchProofs.QueryRewrites.AnalyticalJoins.CastRoleBoundedPersonFilter.sound
