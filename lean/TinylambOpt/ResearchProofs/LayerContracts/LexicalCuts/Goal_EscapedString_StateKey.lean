import TinylambOpt.ResearchGoals.LayerContracts.LexicalCuts
import TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Common

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Goal_EscapedString_StateKey

open TinylambOpt.ResearchGoals.LayerContracts.LexicalCuts
open TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Common

theorem refute : ¬ EscapedString.StateKey.Goal := by
  intro h
  have hq : reachable EscapedString.model 1 := ⟨[Sym.sq], by native_decide⟩
  have hr : reachable EscapedString.model 2 := ⟨[Sym.sq, Sym.slash], by native_decide⟩
  have hh := h 1 2 [Sym.sq] hq hr (by decide)
  have hneq : ((scan EscapedString.model 1 [Sym.sq]).2 ≠ (scan EscapedString.model 2 [Sym.sq]).2) ∨
      (EscapedString.model.cacheKey (scan EscapedString.model 1 [Sym.sq]).1 ≠ EscapedString.model.cacheKey (scan EscapedString.model 2 [Sym.sq]).1) := by
    native_decide
  rcases hh with ⟨h1, h2⟩
  rcases hneq with hn | hn
  · exact hn h1
  · exact hn h2

end TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Goal_EscapedString_StateKey
