import TinylambOpt.ResearchGoals.November26.Tpch

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.November26.Tpch.Goal_Q20
open TinylambOpt.ResearchGoals.November26
open TinylambOpt.ResearchGoals.November26.Tpch
open TinylambOpt.ResearchGoals.BenchmarkRematch


theorem filter_filter_key (pairs : List (Nat × Nat))
    (lines : List ((Nat × Nat) × Option Int)) (k : Nat × Nat) (hk : k ∈ pairs) :
    (lines.filter (fun r => pairs.eraseDups.contains r.1)).filter (fun r => r.1 == k) =
      lines.filter (fun r => r.1 == k) := by
  have hk' : pairs.eraseDups.contains k = true := by
    simpa [List.contains_iff_mem, List.mem_eraseDups] using hk
  rw [List.filter_filter]
  apply List.filter_congr
  intro r hr
  by_cases h : r.1 == k
  · have hrk : r.1 = k := by simpa using h
    rw [hrk] at h ⊢
    rw [hk']
    simp [h]
  · simp [h]


theorem sound : TinylambOpt.ResearchGoals.November26.Tpch.Q20.Goal := by
  intro pairs lines k hk
  rw [forKey, forKey, TinylambOpt.ResearchProofs.November26.Tpch.Goal_Q20.filter_filter_key pairs lines k hk]
end TinylambOpt.ResearchProofs.November26.Tpch.Goal_Q20
