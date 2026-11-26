import TinylambOpt.ResearchGoals.LayerContracts.LexicalCuts
import TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Common

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Goal_DashComment_DiscardCutPrefix

open TinylambOpt.ResearchGoals.LayerContracts.LexicalCuts
open TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Common

theorem sound : DashComment.DiscardCutPrefix.Goal := by
  apply Common.discard_of_cutResets DashComment.model
  intro q c h
  by_cases hq : q = 2
  · cases c <;> simp [DashComment.model, hq, Common.sym_beq_true, Common.sym_beq_false] at h
  · cases c <;> simp [DashComment.model, hq, Common.sym_beq_true, Common.sym_beq_false] at h ⊢ <;> omega

end TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Goal_DashComment_DiscardCutPrefix
