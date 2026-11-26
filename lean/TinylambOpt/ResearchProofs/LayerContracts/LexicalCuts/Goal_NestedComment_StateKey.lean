import TinylambOpt.ResearchGoals.LayerContracts.LexicalCuts
import TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Common

/-!
The SQL frontend can scan input in chunks when its carried lexical state preserves
delimiter recognition across chunk boundaries.

Refuted by counterexample

- `NestedComment.StateKey` — Can a compiled chunk summary be indexed by only the
  proposed entry-state class?
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Goal_NestedComment_StateKey

open TinylambOpt.ResearchGoals.LayerContracts.LexicalCuts
open TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Common

theorem refute : ¬ NestedComment.StateKey.Goal := by
  intro h
  have hq : reachable NestedComment.model 1 := ⟨[Sym.openComment], by native_decide⟩
  have hr : reachable NestedComment.model 2 := ⟨[Sym.openComment, Sym.openComment], by native_decide⟩
  have hh := h 1 2 [Sym.closeComment] hq hr (by decide)
  have hneq : ((scan NestedComment.model 1 [Sym.closeComment]).2 ≠ (scan NestedComment.model 2 [Sym.closeComment]).2) ∨
      (NestedComment.model.cacheKey (scan NestedComment.model 1 [Sym.closeComment]).1 ≠ NestedComment.model.cacheKey (scan NestedComment.model 2 [Sym.closeComment]).1) := by
    native_decide
  rcases hh with ⟨h1, h2⟩
  rcases hneq with hn | hn
  · exact hn h1
  · exact hn h2

end TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Goal_NestedComment_StateKey
