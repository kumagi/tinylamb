import TinylambOpt.ResearchGoals.November26.Tpcds

set_option maxHeartbeats 0
set_option maxRecDepth 10000


namespace TinylambOpt.ResearchProofs.November26.Tpcds.Goal_Q04
open TinylambOpt.ResearchGoals.BenchmarkRematch
open TinylambOpt.ResearchGoals.November26
open TinylambOpt.ResearchGoals.November26.Tpcds

theorem sound : TinylambOpt.ResearchGoals.November26.Tpcds.Q04.Goal := by
  intro prev sales k hk hact
  unfold forKey
  rw [List.filter_filter]
  apply congrArg (fun l : List (Nat × Option Int) => l.map Prod.snd)
  apply List.filter_congr
  intro r hr
  by_cases hkey : r.1 == k
  · have hrk : r.1 = k := by simpa only [beq_iff_eq] using hkey
    subst hrk
    simp [hact]
  · simp [hkey]
end TinylambOpt.ResearchProofs.November26.Tpcds.Goal_Q04
