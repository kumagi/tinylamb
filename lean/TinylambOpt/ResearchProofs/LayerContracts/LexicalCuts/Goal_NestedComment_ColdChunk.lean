import TinylambOpt.ResearchGoals.LayerContracts.LexicalCuts
import TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Common

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Goal_NestedComment_ColdChunk

open TinylambOpt.ResearchGoals.LayerContracts.LexicalCuts
open TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Common

theorem refute : ¬ NestedComment.ColdChunk.Goal := by
  intro h
  have hh := h [Sym.openComment] [Sym.semi]
  have hneq : (scan NestedComment.model 0 ([Sym.openComment]++[Sym.semi])).2 ≠
      joinAt [Sym.openComment].length (scan NestedComment.model 0 [Sym.openComment]).2 (scan NestedComment.model 0 [Sym.semi]).2 := by
    native_decide
  exact hneq hh

end TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Goal_NestedComment_ColdChunk
