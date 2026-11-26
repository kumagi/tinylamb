import TinylambOpt.ResearchGoals.LayerContracts.LexicalCuts
import TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Common

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Goal_EscapedString_DiscardCutPrefix

open TinylambOpt.ResearchGoals.LayerContracts.LexicalCuts
open TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Common

theorem sound : EscapedString.DiscardCutPrefix.Goal := by
  apply Common.discard_of_cutResets EscapedString.model
  intro q c h
  by_cases hq : q = 2
  · cases c <;> simp [EscapedString.model, escaped, quoted, hq, Common.sym_beq_true, Common.sym_beq_false] at h
  · by_cases hq1 : q = 1
    · cases c <;> simp [EscapedString.model, escaped, quoted, hq, hq1, Common.sym_beq_true, Common.sym_beq_false] at h ⊢ <;> omega
    · cases c <;> simp [EscapedString.model, escaped, quoted, hq, hq1, Common.sym_beq_true, Common.sym_beq_false] at h ⊢ <;> omega

end TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Goal_EscapedString_DiscardCutPrefix
