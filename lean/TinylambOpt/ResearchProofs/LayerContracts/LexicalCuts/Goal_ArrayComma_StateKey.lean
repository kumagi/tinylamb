import TinylambOpt.ResearchGoals.LayerContracts.LexicalCuts
import TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Common

/-!
The SQL frontend can scan input in chunks when its carried lexical state preserves
delimiter recognition across chunk boundaries.

Refuted by counterexample

- `ArrayComma.StateKey` — Can a compiled chunk summary be indexed by only the proposed
  entry-state class?
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Goal_ArrayComma_StateKey

open TinylambOpt.ResearchGoals.LayerContracts.LexicalCuts
open TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Common

theorem refute : ¬ ArrayComma.StateKey.Goal := by
  intro h
  have hq : reachable ArrayComma.model 1 := ⟨[Sym.openArray], by native_decide⟩
  have hr : reachable ArrayComma.model 2 := ⟨[Sym.openArray, Sym.openArray], by native_decide⟩
  have hh := h 1 2 [Sym.comma] hq hr (by decide)
  have hneq : ((scan ArrayComma.model 1 [Sym.comma]).2 ≠ (scan ArrayComma.model 2 [Sym.comma]).2) ∨
      (ArrayComma.model.cacheKey (scan ArrayComma.model 1 [Sym.comma]).1 ≠ ArrayComma.model.cacheKey (scan ArrayComma.model 2 [Sym.comma]).1) := by
    native_decide
  rcases hh with ⟨h1, h2⟩
  rcases hneq with hn | hn
  · exact hn h1
  · exact hn h2

end TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Goal_ArrayComma_StateKey
