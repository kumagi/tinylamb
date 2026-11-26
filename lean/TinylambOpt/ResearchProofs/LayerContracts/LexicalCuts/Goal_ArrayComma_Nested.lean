import TinylambOpt.ResearchGoals.LayerContracts.LexicalCuts
import TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Common

/-!
The SQL frontend can scan input in chunks when its carried lexical state preserves
delimiter recognition across chunk boundaries.

Proved

- `ArrayComma.Nested` — `∀ n q, 1<q → (scan model q (List.replicate n .comma)).2=[]`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Goal_ArrayComma_Nested

open TinylambOpt.ResearchGoals.LayerContracts.LexicalCuts
open TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Common

theorem sound : ArrayComma.Nested.Goal := by
  intro n q hq
  induction n with
  | zero => simp [scan]
  | succ n ih =>
      rw [List.replicate_succ, Common.scan_cons]
      have hs' : ArrayComma.model.step q Sym.comma = (q,false) := by
        simp [ArrayComma.model, sym_beq_true, sym_beq_false]
        omega
      rw [hs']
      simp [ih]

end TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Goal_ArrayComma_Nested
