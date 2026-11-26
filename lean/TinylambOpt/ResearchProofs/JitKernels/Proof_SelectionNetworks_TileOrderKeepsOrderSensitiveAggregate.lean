import TinylambOpt.ResearchGoals.JitKernels.SelectionNetworks

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace Nov26.Proof_SelectionNetworks_TileOrderKeepsOrderSensitiveAggregate
open TinylambOpt.ResearchGoals.JitKernels.SelectionNetworks
open TinylambOpt.ResearchGoals.JitKernels.SelectionNetworks.TileOrderKeepsOrderSensitiveAggregate
open TinylambOpt.ResearchGoals.JitKernels.SelectionNetworks TileOrderKeepsOrderSensitiveAggregate

/-- Helper: the tile sort of the concrete ordinal list used in the counterexample. -/
private theorem tile_cex : tile [7,3] 5 = [3,7] := by
  show List.mergeSort [7, 3] (fun a b => decide (a / 5 ≤ b / 5)) = [3, 7]
  simp [List.mergeSort, List.merge]

/--
`TileOrderKeepsOrderSensitiveAggregate.Goal` claims that gathering in tile order leaves a
first-value aggregate (`head?`) unchanged. This is false: tile order reorders the requests by
memory tile, and the first tile-order request need not be the first original request, so the
first gathered value changes. Counterexample: `xs = [0,0,0,0,0,0,0,9]`, `sel = [7,3]`, `n = 5`;
tile order gathers `xs 3 = 0` first, while source order gathers `xs 7 = 9` first.
-/
theorem refute : ¬ Goal := by
  intro h
  have hcontra : (gather [0,0,0,0,0,0,0,9] (tile [7,3] 5)).head? ≠
      (gather [0,0,0,0,0,0,0,9] [7,3]).head? := by
    rw [tile_cex]
    decide
  exact hcontra (h [0,0,0,0,0,0,0,9] [7,3] 5 (by decide))

end Nov26.Proof_SelectionNetworks_TileOrderKeepsOrderSensitiveAggregate