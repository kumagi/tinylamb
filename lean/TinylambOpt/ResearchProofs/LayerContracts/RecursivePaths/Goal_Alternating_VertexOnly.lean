import TinylambOpt.ResearchGoals.LayerContracts.RecursivePaths

/-!
A recursive query uses visited states and frontier updates to avoid redundant work while
retaining visits that affect path acceptance.

Refuted by counterexample

- `Alternating.VertexOnly` — Can DISTINCT use only the vertex before testing the
  accepting state?
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.RecursivePaths.Goal_Alternating_VertexOnly

open TinylambOpt.ResearchGoals.LayerContracts.RecursivePaths
open TinylambOpt.ResearchGoals.LayerContracts.RecursivePaths.Alternating

theorem refute : ¬ Alternating.VertexOnly.Goal := by
  intro h
  let g : List Edge := [⟨0,0,0⟩, ⟨0,1,1⟩]
  have hnot : ¬ (1 ∈ query Alternating.model g 0 2 (fun p => (p.1,0)) ↔
                 1 ∈ query Alternating.model g 0 2 id) := by
    native_decide
  exact hnot (h g 0 2 1)

end TinylambOpt.ResearchProofs.LayerContracts.RecursivePaths.Goal_Alternating_VertexOnly
