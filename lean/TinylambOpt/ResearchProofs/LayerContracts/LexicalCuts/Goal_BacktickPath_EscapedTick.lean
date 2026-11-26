import TinylambOpt.ResearchGoals.LayerContracts.LexicalCuts
import TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Common

/-!
The SQL frontend can scan input in chunks when its carried lexical state preserves
delimiter recognition across chunk boundaries.

Proved

- `BacktickPath.EscapedTick` — `∀ xs, (scan model 1 ([.slash,.tick]++xs)).1 = (scan
  model 1 xs).1`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Goal_BacktickPath_EscapedTick

open TinylambOpt.ResearchGoals.LayerContracts.LexicalCuts
open TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Common

theorem sound : BacktickPath.EscapedTick.Goal := by
  intro xs
  have hpre : scan BacktickPath.model 1 [Sym.slash, Sym.tick] = (1,[]) := by native_decide
  have hc := Common.carry_general BacktickPath.model 1 [Sym.slash, Sym.tick] xs
  rw [hpre] at hc
  have hsnd := congrArg Prod.fst hc
  simpa using hsnd

end TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Goal_BacktickPath_EscapedTick
