import TinylambOpt.ProposedGoals.BenchmarkRematch.ClickBench

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.ProposedGoals.ClickBench.Goal_Q43_SingleMinuteBlockDirectBucketCountGoal

open TinylambOpt.ResearchGoals.QueryRewrites
open TinylambOpt.ProposedGoals.BenchmarkRematch.ClickBench

theorem sound : Q43_SingleMinuteBlockDirectBucketCountGoal := by
  intro times lo hi m hbounds hlo hhi
  simp only [countWhere]
  have hfilter : times.filter (fun t => t / 60 == m) = times := by
    apply List.filter_eq_self.mpr
    intro t ht
    have hlb := (hbounds t ht).1
    have hub := (hbounds t ht).2
    have hdivlo : lo / 60 ≤ t / 60 := Nat.div_le_div_right hlb
    have hdivhi : t / 60 ≤ hi / 60 := Nat.div_le_div_right hub
    rw [hlo] at hdivlo
    rw [hhi] at hdivhi
    have heq : t / 60 = m := by omega
    simp [heq]
  rw [hfilter]

end TinylambOpt.ResearchProofs.ProposedGoals.ClickBench.Goal_Q43_SingleMinuteBlockDirectBucketCountGoal
