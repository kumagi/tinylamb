import TinylambOpt.ResearchGoals.DatabaseCorrectness.PinnedScan

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.DatabaseCorrectness.Goal_PinnedScan_ReusingVerdictAcrossRepin

open TinylambOpt.ResearchGoals.DatabaseCorrectness.PinnedScan
open TinylambOpt.ResearchGoals.ResourcePolicies.VersionVacuum


theorem refute : ¬ ReusingVerdictAcrossRepin.Goal := by
  intro h
  have got := h ⟨some 7, false, [⟨1, some 7⟩]⟩ 1 (some 8) (by simp) (by simp [readAt, winner])
  simp [readWithVerdict, pin, stage, readAt, winner] at got

end TinylambOpt.ResearchProofs.DatabaseCorrectness.Goal_PinnedScan_ReusingVerdictAcrossRepin
