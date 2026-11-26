import TinylambOpt.ResearchGoals.LayerContracts.LexicalCuts
import TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Common

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Goal_CrLf_SkipPlain

open TinylambOpt.ResearchGoals.LayerContracts.LexicalCuts
open TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Common

theorem refute : ¬ CrLf.SkipPlain.Goal := by
  intro h
  have hr : reachable CrLf.model 1 := ⟨[Sym.cr], by native_decide⟩
  have hh := h 1 [Sym.atom] hr (by decide)
  have hneq : scan CrLf.model 1 [Sym.atom] ≠ (1,[]) := by native_decide
  exact hneq hh

end TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Goal_CrLf_SkipPlain
