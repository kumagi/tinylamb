import TinylambOpt.ResearchGoals.LayerContracts.LexicalCuts
import TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Common

/-!
The SQL frontend can scan input in chunks when its carried lexical state preserves
delimiter recognition across chunk boundaries.

Refuted by counterexample

- `DashComment.SkipPlain` — Bypass scanning bytes with no control symbols, even while
  resuming a partial token.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Goal_DashComment_SkipPlain

open TinylambOpt.ResearchGoals.LayerContracts.LexicalCuts
open TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Common

theorem refute : ¬ DashComment.SkipPlain.Goal := by
  intro h
  have hr : reachable DashComment.model 1 := ⟨[Sym.dash], by native_decide⟩
  have hh := h 1 [Sym.atom] hr (by decide)
  have hneq : scan DashComment.model 1 [Sym.atom] ≠ (1,[]) := by native_decide
  exact hneq hh

end TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Goal_DashComment_SkipPlain
