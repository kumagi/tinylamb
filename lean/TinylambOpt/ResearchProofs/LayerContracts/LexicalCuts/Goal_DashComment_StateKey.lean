import TinylambOpt.ResearchGoals.LayerContracts.LexicalCuts
import TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Common

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Goal_DashComment_StateKey

open TinylambOpt.ResearchGoals.LayerContracts.LexicalCuts
open TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Common

theorem refute : ¬ DashComment.StateKey.Goal := by
  intro h
  have hq : reachable DashComment.model 1 := ⟨[Sym.dash], by native_decide⟩
  have hr : reachable DashComment.model 2 := ⟨[Sym.dash, Sym.dash], by native_decide⟩
  have hh := h 1 2 [Sym.semi] hq hr (by decide)
  have hneq : ((scan DashComment.model 1 [Sym.semi]).2 ≠ (scan DashComment.model 2 [Sym.semi]).2) ∨
      (DashComment.model.cacheKey (scan DashComment.model 1 [Sym.semi]).1 ≠ DashComment.model.cacheKey (scan DashComment.model 2 [Sym.semi]).1) := by
    native_decide
  rcases hh with ⟨h1, h2⟩
  rcases hneq with hn | hn
  · exact hn h1
  · exact hn h2

end TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Goal_DashComment_StateKey
