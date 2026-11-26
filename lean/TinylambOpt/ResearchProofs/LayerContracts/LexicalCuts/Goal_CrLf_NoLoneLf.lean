import TinylambOpt.ResearchGoals.LayerContracts.LexicalCuts
import TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Common

/-!
The SQL frontend can scan input in chunks when its carried lexical state preserves
delimiter recognition across chunk boundaries.

Proved

- `CrLf.NoLoneLf` — `∀ n, (scan model 0 (List.replicate n .lf)).2=[]`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Goal_CrLf_NoLoneLf

open TinylambOpt.ResearchGoals.LayerContracts.LexicalCuts
open TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Common

theorem sound : CrLf.NoLoneLf.Goal := by
  have hs0 : CrLf.model.step 0 Sym.lf = (0,false) := by native_decide
  have hfull : ∀ n, scan CrLf.model 0 (List.replicate n Sym.lf) = (0,[]) := by
    intro n
    induction n with
    | zero => simp [scan]
    | succ n ih =>
        rw [List.replicate_succ, Common.scan_cons, hs0]
        simp [ih]
  intro n
  rw [hfull n]

end TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Goal_CrLf_NoLoneLf
