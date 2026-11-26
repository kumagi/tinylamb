import TinylambOpt.ResearchGoals.LayerContracts.LexicalCuts
import TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Common

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Goal_BacktickPath_StateKey

open TinylambOpt.ResearchGoals.LayerContracts.LexicalCuts
open TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Common

theorem refute : ¬ BacktickPath.StateKey.Goal := by
  intro h
  have hq : reachable BacktickPath.model 1 := ⟨[Sym.tick], by native_decide⟩
  have hr : reachable BacktickPath.model 2 := ⟨[Sym.tick, Sym.slash], by native_decide⟩
  have hh := h 1 2 [Sym.tick] hq hr (by decide)
  have hneq : ((scan BacktickPath.model 1 [Sym.tick]).2 ≠ (scan BacktickPath.model 2 [Sym.tick]).2) ∨
      (BacktickPath.model.cacheKey (scan BacktickPath.model 1 [Sym.tick]).1 ≠ BacktickPath.model.cacheKey (scan BacktickPath.model 2 [Sym.tick]).1) := by
    native_decide
  rcases hh with ⟨h1, h2⟩
  rcases hneq with hn | hn
  · exact hn h1
  · exact hn h2

end TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Goal_BacktickPath_StateKey
