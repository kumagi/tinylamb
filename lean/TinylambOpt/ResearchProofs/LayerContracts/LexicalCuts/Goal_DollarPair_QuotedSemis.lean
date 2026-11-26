import TinylambOpt.ResearchGoals.LayerContracts.LexicalCuts
import TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Common

/-!
The SQL frontend can scan input in chunks when its carried lexical state preserves
delimiter recognition across chunk boundaries.

Proved

- `DollarPair.QuotedSemis` — `∀ n, (scan model 2 (List.replicate n .semi)).2=[]`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Goal_DollarPair_QuotedSemis

open TinylambOpt.ResearchGoals.LayerContracts.LexicalCuts
open TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Common

theorem sound : DollarPair.QuotedSemis.Goal := by
  have hs : DollarPair.model.step 2 Sym.semi = (2,false) := by native_decide
  intro n
  induction n with
  | zero => simp [scan]
  | succ n ih =>
      rw [List.replicate_succ, Common.scan_cons, hs]
      simp [ih]

end TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Goal_DollarPair_QuotedSemis
