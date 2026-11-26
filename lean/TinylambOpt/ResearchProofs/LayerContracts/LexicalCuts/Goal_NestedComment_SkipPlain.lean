import TinylambOpt.ResearchGoals.LayerContracts.LexicalCuts
import TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Common

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Goal_NestedComment_SkipPlain

open TinylambOpt.ResearchGoals.LayerContracts.LexicalCuts
open TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Common

theorem sound : NestedComment.SkipPlain.Goal := by
  apply Common.skipPlain_of_stable NestedComment.model
  intro q c hc
  cases c <;> simp [NestedComment.model, quoted, Common.sym_beq_true, Common.sym_beq_false] at hc ⊢

end TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Goal_NestedComment_SkipPlain
