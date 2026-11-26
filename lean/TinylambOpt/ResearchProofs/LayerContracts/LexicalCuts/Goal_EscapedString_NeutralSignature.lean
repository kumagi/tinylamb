import TinylambOpt.ResearchGoals.LayerContracts.LexicalCuts
import TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Common

/-!
The SQL frontend can scan input in chunks when its carried lexical state preserves
delimiter recognition across chunk boundaries.

Refuted by counterexample

- `EscapedString.NeutralSignature` — Two chunks equivalent from state zero can
  substitute at any scanner entry.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Goal_EscapedString_NeutralSignature

open TinylambOpt.ResearchGoals.LayerContracts.LexicalCuts
open TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Common

theorem refute : ¬ EscapedString.NeutralSignature.Goal := by
  intro h
  have hlen : [Sym.sq, Sym.semi, Sym.sq].length = [Sym.sq, Sym.atom, Sym.sq].length := by decide
  have hscan : scan EscapedString.model 0 [Sym.sq, Sym.semi, Sym.sq] = scan EscapedString.model 0 [Sym.sq, Sym.atom, Sym.sq] := by native_decide
  have hr : reachable EscapedString.model 1 := ⟨[Sym.sq], by native_decide⟩
  have hh := h [Sym.sq, Sym.semi, Sym.sq] [Sym.sq, Sym.atom, Sym.sq] hlen hscan 1 hr
  have hneq : scan EscapedString.model 1 [Sym.sq, Sym.semi, Sym.sq] ≠ scan EscapedString.model 1 [Sym.sq, Sym.atom, Sym.sq] := by native_decide
  exact hneq hh

end TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Goal_EscapedString_NeutralSignature
