import TinylambOpt.ProposedGoals.BenchmarkRematch.ClickBench

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.ProposedGoals.ClickBench.Goal_Q15_LocalDictCodeCountTranslationGoal

open TinylambOpt.ResearchGoals.QueryRewrites
open TinylambOpt.ProposedGoals.BenchmarkRematch.ClickBench

theorem sound : Q15_LocalDictCodeCountTranslationGoal := by
  intro rows dict eng phrase hinj hex c0 hc0
  have hfilters :
      rows.filter (fun r => (r.1 == eng) && (dict r.2 == phrase)) =
        rows.filter (fun r => (r.1 == eng) && (r.2 == c0)) := by
    apply List.filter_congr
    intro r hr
    have hdict : dict r.2 = phrase ↔ r.2 = c0 := by
      constructor
      · intro heq
        exact hinj r.2 c0 (heq.trans hc0.symm)
      · intro heq
        subst heq
        exact hc0
    have hleft : ((r.1 == eng) && (dict r.2 == phrase)) = true ↔
        ((r.1 == eng) && (r.2 == c0)) = true := by
      simp only [Bool.and_eq_true, beq_iff_eq, hdict]
    rw [Bool.eq_iff_iff, hleft]
  simp only [countWhere]
  rw [hfilters]

end TinylambOpt.ResearchProofs.ProposedGoals.ClickBench.Goal_Q15_LocalDictCodeCountTranslationGoal
