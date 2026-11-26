import TinylambOpt.ResearchProofs.JitFrontiers.DemandSSA.Common_DemandSSA

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitFrontiers.DemandSSA.Goal_CaseAnd_MaskedRhs

open TinylambOpt.ResearchGoals.JitFrontiers.DemandSSA
open TinylambOpt.ResearchProofs.JitFrontiers.DemandSSA.Common

theorem sound : CaseAnd.MaskedRhs.Goal := by
  intro s
  simp [run, after, CaseAnd.rule]

end TinylambOpt.ResearchProofs.JitFrontiers.DemandSSA.Goal_CaseAnd_MaskedRhs
