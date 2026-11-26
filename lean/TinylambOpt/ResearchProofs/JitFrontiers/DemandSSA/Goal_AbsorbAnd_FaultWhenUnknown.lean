import TinylambOpt.ResearchProofs.JitFrontiers.DemandSSA.Common_DemandSSA

/-!
SQL expression lowering must preserve which values and errors are demanded on each
active control-flow path.

Proved

- `AbsorbAnd.FaultWhenUnknown` — `∀ s, (run [.null,.fault s] rule.src).1=.fault s`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitFrontiers.DemandSSA.Goal_AbsorbAnd_FaultWhenUnknown

open TinylambOpt.ResearchGoals.JitFrontiers.DemandSSA
open TinylambOpt.ResearchProofs.JitFrontiers.DemandSSA.Common

theorem sound : AbsorbAnd.FaultWhenUnknown.Goal := by
  intro s
  simp [run, after, AbsorbAnd.rule]

end TinylambOpt.ResearchProofs.JitFrontiers.DemandSSA.Goal_AbsorbAnd_FaultWhenUnknown
