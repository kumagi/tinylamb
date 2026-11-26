import TinylambOpt.ResearchGoals.LayerContracts.LexicalCuts
import TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Common

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Goal_QuotedField_EmbeddedDot

open TinylambOpt.ResearchGoals.LayerContracts.LexicalCuts
open TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Common

private theorem range_succ_cons (n : Nat) : List.range (n+1) = 0 :: (List.range n).map (1+·) := by
  rw [show n + 1 = 1 + n by omega, List.range_add]
  simp

theorem sound : QuotedField.EmbeddedDot.Goal := by
  have hpre : scan QuotedField.model 0 [Sym.dq] = (1,[]) := by native_decide
  have hlast : scan QuotedField.model 1 [Sym.dq] = (0,[]) := by native_decide
  have hmid : ∀ n, scan QuotedField.model 1 (List.replicate n Sym.dot) = (1,[]) := by
    have hs : QuotedField.model.step 1 Sym.dot = (1,false) := by native_decide
    intro n; induction n with
    | zero => simp [scan]
    | succ n ih =>
        rw [List.replicate_succ, Common.scan_cons, hs]
        simp [ih]
  intro n
  have hc := Common.carry_general QuotedField.model 0 [Sym.dq] (List.replicate n Sym.dot ++ [Sym.dq])
  rw [hpre] at hc
  have hc2 : scan QuotedField.model 1 (List.replicate n Sym.dot ++ [Sym.dq]) = (0,[]) := by
    have h := Common.carry_general QuotedField.model 1 (List.replicate n Sym.dot) [Sym.dq]
    simpa [hmid n, hlast, joinAt] using h
  have hsnd := congrArg Prod.snd hc
  simp [joinAt, hc2] at hsnd
  exact hsnd

end TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Goal_QuotedField_EmbeddedDot
