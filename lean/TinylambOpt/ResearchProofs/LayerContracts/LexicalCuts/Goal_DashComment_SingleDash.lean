import TinylambOpt.ResearchGoals.LayerContracts.LexicalCuts
import TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Common

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Goal_DashComment_SingleDash

open TinylambOpt.ResearchGoals.LayerContracts.LexicalCuts
open TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Common

private theorem range_succ_cons (n : Nat) : List.range (n+1) = 0 :: (List.range n).map (1+·) := by
  rw [show n + 1 = 1 + n by omega, List.range_add]
  simp

theorem sound : DashComment.SingleDash.Goal := by
  have hs1 : DashComment.model.step 1 Sym.atom = (0,false) := by native_decide
  have hs0 : DashComment.model.step 0 Sym.semi = (0,true) := by native_decide
  have hsemis : ∀ n, (scan DashComment.model 0 (List.replicate n Sym.semi)).2 = List.range n := by
    intro n; induction n with
    | zero => simp [scan]
    | succ n ih =>
        rw [List.replicate_succ, Common.scan_cons, hs0]
        simp [ih, range_succ_cons]
  intro n
  have hc : (scan DashComment.model 1 ([Sym.atom] ++ List.replicate n Sym.semi)).2 =
      (scan DashComment.model 0 (List.replicate n Sym.semi)).2.map (1+·) := by
    have h := Common.scan_cons DashComment.model 1 Sym.atom (List.replicate n Sym.semi)
    rw [hs1] at h
    have hsnd := congrArg Prod.snd h
    simpa using hsnd
  change (scan DashComment.model 1 ([Sym.atom] ++ List.replicate n Sym.semi)).2 = _
  rw [hc, hsemis n]

end TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Goal_DashComment_SingleDash
