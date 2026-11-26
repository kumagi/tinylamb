import TinylambOpt.ResearchGoals.LayerContracts.LexicalCuts
import TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Common

/-!
The SQL frontend can scan input in chunks when its carried lexical state preserves
delimiter recognition across chunk boundaries.

Proved

- `StringStmt.DoubleQuote` — `∀ q xs, q<2 → scan model q ([.sq,.sq]++xs) = ((scan model
  q xs).1,(scan model q xs).2.map (2+·))`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Goal_StringStmt_DoubleQuote

open TinylambOpt.ResearchGoals.LayerContracts.LexicalCuts
open TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Common

theorem sound : StringStmt.DoubleQuote.Goal := by
  intro q xs hq
  have hq' : q = 0 ∨ q = 1 := by omega
  rcases hq' with rfl | rfl
  · have hpre : scan StringStmt.model 0 [Sym.sq, Sym.sq] = (0,[]) := by native_decide
    have hc := Common.carry_general StringStmt.model 0 [Sym.sq, Sym.sq] xs
    rw [hpre] at hc
    rw [hc]
    simp [joinAt]
  · have hpre : scan StringStmt.model 1 [Sym.sq, Sym.sq] = (1,[]) := by native_decide
    have hc := Common.carry_general StringStmt.model 1 [Sym.sq, Sym.sq] xs
    rw [hpre] at hc
    rw [hc]
    simp [joinAt]

end TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Goal_StringStmt_DoubleQuote
