import TinylambOpt.ResearchProofs.JitFrontiers.DemandSSA.Common_DemandSSA

/-!
SQL expression lowering must preserve which values and errors are demanded on each
active control-flow path.

Proved

- `DoubleNegation.FaultSlot` — `∀ s, run [.fault s] rule.src=(.fault s,[0])`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitFrontiers.DemandSSA.Goal_DoubleNegation_FaultSlot

open TinylambOpt.ResearchGoals.JitFrontiers.DemandSSA
open TinylambOpt.ResearchProofs.JitFrontiers.DemandSSA.Common

theorem sound : DoubleNegation.FaultSlot.Goal := by
  intro s
  simp [run, after, DoubleNegation.rule]

end TinylambOpt.ResearchProofs.JitFrontiers.DemandSSA.Goal_DoubleNegation_FaultSlot
