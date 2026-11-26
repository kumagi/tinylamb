import TinylambOpt.ResearchGoals.LayerContracts.LexicalCuts
import TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Common

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Goal_ArrayComma_DiscardCutPrefix

open TinylambOpt.ResearchGoals.LayerContracts.LexicalCuts
open TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Common

theorem refute : ¬ ArrayComma.DiscardCutPrefix.Goal := by
  intro h
  have hh := h 1 [Sym.comma, Sym.comma] 0 (by native_decide)
  have hneq : (scan ArrayComma.model 1 [Sym.comma, Sym.comma]).2.tail ≠
      ((scan ArrayComma.model 0 ([Sym.comma, Sym.comma].drop 1)).2.map (1+1+·)) := by
    native_decide
  exact hneq hh

end TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Goal_ArrayComma_DiscardCutPrefix
