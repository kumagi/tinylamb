import TinylambOpt.ResearchGoals.LayerContracts.LexicalCuts
import TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Common

/-!
The SQL frontend can scan input in chunks when its carried lexical state preserves
delimiter recognition across chunk boundaries.

Refuted by counterexample

- `ArrayComma.NeutralSignature` — Two chunks equivalent from state zero can substitute
  at any scanner entry.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Goal_ArrayComma_NeutralSignature

open TinylambOpt.ResearchGoals.LayerContracts.LexicalCuts
open TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Common

theorem refute : ¬ ArrayComma.NeutralSignature.Goal := by
  intro h
  have hlen : [Sym.closeArray, Sym.atom].length = [Sym.atom, Sym.atom].length := by decide
  have hscan : scan ArrayComma.model 0 [Sym.closeArray, Sym.atom] = scan ArrayComma.model 0 [Sym.atom, Sym.atom] := by native_decide
  have hr : reachable ArrayComma.model 1 := ⟨[Sym.openArray], by native_decide⟩
  have hh := h [Sym.closeArray, Sym.atom] [Sym.atom, Sym.atom] hlen hscan 1 hr
  have hneq : scan ArrayComma.model 1 [Sym.closeArray, Sym.atom] ≠ scan ArrayComma.model 1 [Sym.atom, Sym.atom] := by native_decide
  exact hneq hh

end TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Goal_ArrayComma_NeutralSignature
