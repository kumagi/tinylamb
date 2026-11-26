import TinylambOpt.ResearchGoals.LayerContracts.LexicalCuts
import TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Common

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Goal_BacktickPath_NeutralSignature

open TinylambOpt.ResearchGoals.LayerContracts.LexicalCuts
open TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Common

theorem refute : ¬ BacktickPath.NeutralSignature.Goal := by
  intro h
  have hlen : [Sym.tick, Sym.dot, Sym.tick].length = [Sym.tick, Sym.atom, Sym.tick].length := by decide
  have hscan : scan BacktickPath.model 0 [Sym.tick, Sym.dot, Sym.tick] = scan BacktickPath.model 0 [Sym.tick, Sym.atom, Sym.tick] := by native_decide
  have hr : reachable BacktickPath.model 1 := ⟨[Sym.tick], by native_decide⟩
  have hh := h [Sym.tick, Sym.dot, Sym.tick] [Sym.tick, Sym.atom, Sym.tick] hlen hscan 1 hr
  have hneq : scan BacktickPath.model 1 [Sym.tick, Sym.dot, Sym.tick] ≠ scan BacktickPath.model 1 [Sym.tick, Sym.atom, Sym.tick] := by native_decide
  exact hneq hh

end TinylambOpt.ResearchProofs.LayerContracts.LexicalCuts.Goal_BacktickPath_NeutralSignature
