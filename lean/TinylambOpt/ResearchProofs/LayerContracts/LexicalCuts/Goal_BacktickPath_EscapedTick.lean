import TinylambOpt.ResearchGoals.LayerContracts.LexicalCuts
import TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Common

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
