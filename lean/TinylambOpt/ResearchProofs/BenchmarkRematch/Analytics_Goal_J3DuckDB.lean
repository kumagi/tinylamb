import TinylambOpt.ResearchGoals.BenchmarkRematch.Analytics

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.BenchmarkRematch.Analytics.Goal_J3DuckDB

open TinylambOpt.ResearchGoals.BenchmarkRematch

theorem sound : TinylambOpt.ResearchGoals.BenchmarkRematch.Analytics.J3DuckDB.Goal := by
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

end TinylambOpt.ResearchProofs.BenchmarkRematch.Analytics.Goal_J3DuckDB

#print axioms TinylambOpt.ResearchProofs.BenchmarkRematch.Analytics.Goal_J3DuckDB.sound
