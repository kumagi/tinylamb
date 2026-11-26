import TinylambOpt.ResearchGoals.LayerContracts.LexicalCuts
import TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Common

/-!
The SQL frontend can scan input in chunks when its carried lexical state preserves
delimiter recognition across chunk boundaries.

Refuted by counterexample

- `CrLf.ColdChunk` — Can each chunk be scanned independently from the neutral state?
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Goal_CrLf_ColdChunk

open TinylambOpt.ResearchGoals.LayerContracts.LexicalCuts
open TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Common

theorem refute : ¬ CrLf.ColdChunk.Goal := by
  intro h
  have hh := h [Sym.cr] [Sym.lf]
  have hneq : (scan CrLf.model 0 ([Sym.cr]++[Sym.lf])).2 ≠
      joinAt [Sym.cr].length (scan CrLf.model 0 [Sym.cr]).2 (scan CrLf.model 0 [Sym.lf]).2 := by
    native_decide
  exact hneq hh

end TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Goal_CrLf_ColdChunk
