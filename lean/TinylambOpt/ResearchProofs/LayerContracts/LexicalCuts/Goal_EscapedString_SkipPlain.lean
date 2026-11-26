import TinylambOpt.ResearchGoals.LayerContracts.LexicalCuts
import TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Common

/-!
The SQL frontend can scan input in chunks when its carried lexical state preserves
delimiter recognition across chunk boundaries.

Refuted by counterexample

- `EscapedString.SkipPlain` — Bypass scanning bytes with no control symbols, even while
  resuming a partial token.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Goal_EscapedString_SkipPlain

open TinylambOpt.ResearchGoals.LayerContracts.LexicalCuts
open TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Common

theorem refute : ¬ EscapedString.SkipPlain.Goal := by
  intro h
  have hr : reachable EscapedString.model 2 := ⟨[Sym.sq, Sym.slash], by native_decide⟩
  have hh := h 2 [Sym.atom] hr (by decide)
  have hneq : scan EscapedString.model 2 [Sym.atom] ≠ (2,[]) := by native_decide
  exact hneq hh

end TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Goal_EscapedString_SkipPlain
