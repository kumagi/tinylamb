import TinylambOpt.ResearchGoals.LayerContracts.LexicalCuts
import TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Common

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Goal_CrLf_NeutralSignature

open TinylambOpt.ResearchGoals.LayerContracts.LexicalCuts
open TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Common

theorem refute : ¬ CrLf.NeutralSignature.Goal := by
  intro h
  have hlen : [Sym.lf, Sym.cr].length = [Sym.atom, Sym.cr].length := by decide
  have hscan : scan CrLf.model 0 [Sym.lf, Sym.cr] = scan CrLf.model 0 [Sym.atom, Sym.cr] := by native_decide
  have hr : reachable CrLf.model 1 := ⟨[Sym.cr], by native_decide⟩
  have hh := h [Sym.lf, Sym.cr] [Sym.atom, Sym.cr] hlen hscan 1 hr
  have hneq : scan CrLf.model 1 [Sym.lf, Sym.cr] ≠ scan CrLf.model 1 [Sym.atom, Sym.cr] := by native_decide
  exact hneq hh

end TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Goal_CrLf_NeutralSignature
