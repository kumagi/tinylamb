import TinylambOpt.ResearchGoals.LayerContracts.LexicalCuts
import TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Common

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Goal_QuotedField_UnquotedDots

open TinylambOpt.ResearchGoals.LayerContracts.LexicalCuts
open TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Common

private theorem range_succ_cons (n : Nat) : List.range (n+1) = 0 :: (List.range n).map (1+·) := by
  rw [show n + 1 = 1 + n by omega, List.range_add]
  simp

theorem sound : QuotedField.UnquotedDots.Goal := by
  have hs : QuotedField.model.step 0 Sym.dot = (0,true) := by native_decide
  intro n
  induction n with
  | zero => simp [scan]
  | succ n ih =>
      rw [List.replicate_succ, Common.scan_cons, hs]
      simp [ih, range_succ_cons]

end TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Goal_QuotedField_UnquotedDots
