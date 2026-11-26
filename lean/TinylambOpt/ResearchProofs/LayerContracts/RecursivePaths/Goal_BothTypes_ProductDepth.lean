import TinylambOpt.ResearchProofs.LayerContracts.RecursivePaths.Common
import TinylambOpt.ResearchProofs.LayerContracts.RecursivePaths.Goal_BothTypes_StateDomain

/-!
A recursive query uses visited states and frontier updates to avoid redundant work while
retaining visits that affect path acceptance.

Proved

- `BothTypes.ProductDepth` — Finite graph times automaton size is enough rounds for all
  terminal vertices.
-/

namespace TinylambOpt.ResearchProofs.LayerContracts.RecursivePaths.Goal_BothTypes_ProductDepth

open TinylambOpt.ResearchGoals.LayerContracts.RecursivePaths

theorem sound : BothTypes.ProductDepth.Goal :=
  TinylambOpt.ResearchProofs.LayerContracts.RecursivePaths.Generic.productDepth_of_stateDomain _
    TinylambOpt.ResearchProofs.LayerContracts.RecursivePaths.Goal_BothTypes_StateDomain.sound

end TinylambOpt.ResearchProofs.LayerContracts.RecursivePaths.Goal_BothTypes_ProductDepth
