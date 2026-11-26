import TinylambOpt.ResearchGoals.LayerContracts.LexicalCuts
import TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Common

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Goal_QuotedField_NeutralSignature

open TinylambOpt.ResearchGoals.LayerContracts.LexicalCuts
open TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Common

theorem refute : ¬ QuotedField.NeutralSignature.Goal := by
  intro h
  have hlen : [Sym.dq, Sym.dot, Sym.dq].length = [Sym.dq, Sym.atom, Sym.dq].length := by decide
  have hscan : scan QuotedField.model 0 [Sym.dq, Sym.dot, Sym.dq] = scan QuotedField.model 0 [Sym.dq, Sym.atom, Sym.dq] := by native_decide
  have hr : reachable QuotedField.model 1 := ⟨[Sym.dq], by native_decide⟩
  have hh := h [Sym.dq, Sym.dot, Sym.dq] [Sym.dq, Sym.atom, Sym.dq] hlen hscan 1 hr
  have hneq : scan QuotedField.model 1 [Sym.dq, Sym.dot, Sym.dq] ≠ scan QuotedField.model 1 [Sym.dq, Sym.atom, Sym.dq] := by native_decide
  exact hneq hh

end TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Goal_QuotedField_NeutralSignature
