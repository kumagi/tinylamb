import TinylambOpt.ResearchGoals.LayerContracts.LexicalCuts
import TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Common

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Goal_DollarPair_StateKey

open TinylambOpt.ResearchGoals.LayerContracts.LexicalCuts
open TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Common

theorem refute : ¬ DollarPair.StateKey.Goal := by
  intro h
  have hq : reachable DollarPair.model 0 := ⟨[], by native_decide⟩
  have hr : reachable DollarPair.model 1 := ⟨[Sym.dollar], by native_decide⟩
  have hh := h 0 1 [Sym.dollar] hq hr (by decide)
  have hneq : ((scan DollarPair.model 0 [Sym.dollar]).2 ≠ (scan DollarPair.model 1 [Sym.dollar]).2) ∨
      (DollarPair.model.cacheKey (scan DollarPair.model 0 [Sym.dollar]).1 ≠ DollarPair.model.cacheKey (scan DollarPair.model 1 [Sym.dollar]).1) := by
    native_decide
  rcases hh with ⟨h1, h2⟩
  rcases hneq with hn | hn
  · exact hn h1
  · exact hn h2

end TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Goal_DollarPair_StateKey
