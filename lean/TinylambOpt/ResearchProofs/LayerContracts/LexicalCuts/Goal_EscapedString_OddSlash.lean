import TinylambOpt.ResearchGoals.LayerContracts.LexicalCuts
import TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Common

/-!
The SQL frontend can scan input in chunks when its carried lexical state preserves
delimiter recognition across chunk boundaries.

Proved

- `EscapedString.OddSlash` — `∀ n, (scan model 1 (List.replicate (2*n+1) .slash)).1=2`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Goal_EscapedString_OddSlash

open TinylambOpt.ResearchGoals.LayerContracts.LexicalCuts
open TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Common

theorem sound : EscapedString.OddSlash.Goal := by
  have hs1 : EscapedString.model.step 1 Sym.slash = (2,false) := by native_decide
  have hs2 : EscapedString.model.step 2 Sym.slash = (1,false) := by native_decide
  have h : ∀ k, (scan EscapedString.model 1 (List.replicate k Sym.slash)).1 = 1 + k % 2 ∧
      (scan EscapedString.model 2 (List.replicate k Sym.slash)).1 = 2 - k % 2 := by
    intro k
    induction k with
    | zero => simp [scan]
    | succ k ih =>
        constructor
        · rw [List.replicate_succ, Common.scan_cons, hs1, ih.2]
          omega
        · rw [List.replicate_succ, Common.scan_cons, hs2, ih.1]
          omega
  intro n
  rw [(h (2*n+1)).1]
  have : (2*n+1) % 2 = 1 := by omega
  omega

end TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Goal_EscapedString_OddSlash
