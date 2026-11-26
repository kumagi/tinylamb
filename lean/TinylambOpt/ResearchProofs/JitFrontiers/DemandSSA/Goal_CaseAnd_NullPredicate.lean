import TinylambOpt.ResearchProofs.JitFrontiers.DemandSSA.Common_DemandSSA

/-!
SQL expression lowering must preserve which values and errors are demanded on each
active control-flow path.

Proved

- `CaseAnd.NullPredicate` — `(run [.null,.yes] rule.src).1=.no ∧ (run [.null,.yes]
  rule.dst).1=.null`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitFrontiers.DemandSSA.Goal_CaseAnd_NullPredicate

open TinylambOpt.ResearchGoals.JitFrontiers.DemandSSA
open TinylambOpt.ResearchProofs.JitFrontiers.DemandSSA.Common

theorem sound : CaseAnd.NullPredicate.Goal := by
  constructor <;> simp [CaseAnd.NullPredicate.Goal, run, after, slot, CaseAnd.rule]

end TinylambOpt.ResearchProofs.JitFrontiers.DemandSSA.Goal_CaseAnd_NullPredicate
