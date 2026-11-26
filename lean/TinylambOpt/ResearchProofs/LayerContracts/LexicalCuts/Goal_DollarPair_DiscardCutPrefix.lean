import TinylambOpt.ResearchGoals.LayerContracts.LexicalCuts
import TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Common

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Goal_DollarPair_DiscardCutPrefix

open TinylambOpt.ResearchGoals.LayerContracts.LexicalCuts
open TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Common

theorem sound : DollarPair.DiscardCutPrefix.Goal := by
  apply Common.discard_of_cutResets DollarPair.model
  intro q c h
  cases c <;> simp [DollarPair.model, Common.sym_beq_true, Common.sym_beq_false] at h ⊢ <;> omega

end TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Goal_DollarPair_DiscardCutPrefix
