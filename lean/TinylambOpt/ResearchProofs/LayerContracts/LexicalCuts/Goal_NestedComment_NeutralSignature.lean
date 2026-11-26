import TinylambOpt.ResearchGoals.LayerContracts.LexicalCuts
import TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Common

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Goal_NestedComment_NeutralSignature

open TinylambOpt.ResearchGoals.LayerContracts.LexicalCuts
open TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Common

theorem refute : ¬ NestedComment.NeutralSignature.Goal := by
  intro h
  have hlen : [Sym.closeComment, Sym.atom].length = [Sym.atom, Sym.atom].length := by decide
  have hscan : scan NestedComment.model 0 [Sym.closeComment, Sym.atom] = scan NestedComment.model 0 [Sym.atom, Sym.atom] := by native_decide
  have hr : reachable NestedComment.model 1 := ⟨[Sym.openComment], by native_decide⟩
  have hh := h [Sym.closeComment, Sym.atom] [Sym.atom, Sym.atom] hlen hscan 1 hr
  have hneq : scan NestedComment.model 1 [Sym.closeComment, Sym.atom] ≠ scan NestedComment.model 1 [Sym.atom, Sym.atom] := by native_decide
  exact hneq hh

end TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Goal_NestedComment_NeutralSignature
