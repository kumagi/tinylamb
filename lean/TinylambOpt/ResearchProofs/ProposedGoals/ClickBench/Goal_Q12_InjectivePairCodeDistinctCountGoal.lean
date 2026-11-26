import TinylambOpt.ProposedGoals.BenchmarkRematch.ClickBench

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.ProposedGoals.ClickBench.Goal_Q12_InjectivePairCodeDistinctCountGoal

open TinylambOpt.ResearchGoals.QueryRewrites
open TinylambOpt.ProposedGoals.BenchmarkRematch.ClickBench

theorem sound : Q12_InjectivePairCodeDistinctCountGoal := by
  intro rows pack k hinj
  have hfilters : rows.filter (fun r => pack r.1 == pack k) =
      rows.filter (fun r => r.1 == k) := by
    apply List.filter_congr
    intro r hr
    have hpackEq : pack r.1 = pack k ↔ r.1 = k := by
      constructor
      · exact hinj r.1 k
      · intro heq
        subst heq
        rfl
    by_cases hp : pack r.1 == pack k
    · have heq : r.1 = k := hinj r.1 k (by simpa using hp)
      have hrk : (r.1 == k) = true := by simp [heq]
      simp [hp, hrk]
    · have hpf : (pack r.1 == pack k) = false := by simpa using hp
      have hrkb : (r.1 == k) = false := by
        have hne : r.1 ≠ k := by
          intro heqk
          exact hp (by simp [heqk])
        simp [hne]
      rw [hpf, hrkb]
  rw [hfilters]

end TinylambOpt.ResearchProofs.ProposedGoals.ClickBench.Goal_Q12_InjectivePairCodeDistinctCountGoal
