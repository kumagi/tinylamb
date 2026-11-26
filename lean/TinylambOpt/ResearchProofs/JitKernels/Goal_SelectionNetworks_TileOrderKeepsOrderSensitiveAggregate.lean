import TinylambOpt.ResearchGoals.JitKernels.SelectionNetworks

/-!
Tile sorting can move a later, smaller ordinal in front of the first selected
row when the selection vector is not already tile-ordered. A first-value
aggregate observes that reordering.
-/
namespace TinylambOpt.ResearchProofs.JitKernels.Goal_SelectionNetworks_TileOrderKeepsOrderSensitiveAggregate

open TinylambOpt.ResearchGoals.JitKernels.SelectionNetworks

theorem refute : ¬ TinylambOpt.ResearchGoals.JitKernels.SelectionNetworks.TileOrderKeepsOrderSensitiveAggregate.Goal := by
  intro h
  have h' := h [10, 20, 30, 40, 50, 60, 70, 80, 90] [8, 1] 4 (by omega)
  have hne : ¬ ((gather [10, 20, 30, 40, 50, 60, 70, 80, 90] (tile [8, 1] 4)).head? =
      (gather [10, 20, 30, 40, 50, 60, 70, 80, 90] [8, 1]).head?) := by
    native_decide
  exact hne h'

end TinylambOpt.ResearchProofs.JitKernels.Goal_SelectionNetworks_TileOrderKeepsOrderSensitiveAggregate
