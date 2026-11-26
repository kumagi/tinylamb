import TinylambOpt.ResearchProofs.JitFrontiers.DemandSSA.Common_DemandSSA

/-!
SQL expression lowering must preserve which values and errors are demanded on each
active control-flow path.

Proved

- `IdenticalArms.FaultPriority` — `∀ s t, (run [.fault s,.fault t] rule.src).1=.fault
  s`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitFrontiers.DemandSSA.Goal_IdenticalArms_FaultPriority

open TinylambOpt.ResearchGoals.JitFrontiers.DemandSSA
open TinylambOpt.ResearchProofs.JitFrontiers.DemandSSA.Common

theorem sound : IdenticalArms.FaultPriority.Goal := by
  intro s t
  simp [run, after, IdenticalArms.rule]

end TinylambOpt.ResearchProofs.JitFrontiers.DemandSSA.Goal_IdenticalArms_FaultPriority
