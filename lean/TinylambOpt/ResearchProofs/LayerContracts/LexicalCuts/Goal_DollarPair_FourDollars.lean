import TinylambOpt.ResearchGoals.LayerContracts.LexicalCuts
import TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Common

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Goal_DollarPair_FourDollars

open TinylambOpt.ResearchGoals.LayerContracts.LexicalCuts
open TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Common

theorem sound : DollarPair.FourDollars.Goal := by
  have hs0 : DollarPair.model.step 0 Sym.dollar = (1,false) := by native_decide
  have hs1 : DollarPair.model.step 1 Sym.dollar = (2,false) := by native_decide
  have hs2 : DollarPair.model.step 2 Sym.dollar = (3,false) := by native_decide
  have hs3 : DollarPair.model.step 3 Sym.dollar = (0,false) := by native_decide
  have h : ∀ q k, q < 4 → (scan DollarPair.model q (List.replicate k Sym.dollar)).1 = (q+k) % 4 := by
    intro q k hqsmall
    induction k generalizing q with
    | zero =>
        change (scan DollarPair.model q []).1 = (q+0) % 4
        simp [scan, Nat.mod_eq_of_lt hqsmall]
    | succ k ih =>
        rw [List.replicate_succ, Common.scan_cons]
        have hq : q = 0 ∨ q = 1 ∨ q = 2 ∨ q = 3 := by omega
        rcases hq with rfl | rfl | rfl | rfl
        · simp only [hs0]; rw [ih 1 (by omega)]; omega
        · simp only [hs1]; rw [ih 2 (by omega)]; omega
        · simp only [hs2]; rw [ih 3 (by omega)]; omega
        · simp only [hs3]; rw [ih 0 (by omega)]; omega
  intro n
  rw [h 0 (4*n) (by omega)]
  omega

end TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Goal_DollarPair_FourDollars
