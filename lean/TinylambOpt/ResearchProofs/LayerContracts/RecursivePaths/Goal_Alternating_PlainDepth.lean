import TinylambOpt.ResearchGoals.LayerContracts.RecursivePaths

/-!
A recursive query uses visited states and frontier updates to avoid redundant work while
retaining visits that affect path acceptance.

Refuted by counterexample

- `Alternating.PlainDepth` — Aggressive recursion cap: one round per graph vertex even
  with path constraints.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 100000

namespace TinylambOpt.ResearchProofs.LayerContracts.RecursivePaths.Goal_Alternating_PlainDepth

open TinylambOpt.ResearchGoals.LayerContracts.RecursivePaths

/-- Chain 0→1→2→3 with label 0 and a label-1 self loop at every vertex: reaching vertex 3
needs five alternating steps although the graph only has four vertices. -/
def gC : List Edge :=
  [⟨0,1,0⟩, ⟨1,2,0⟩, ⟨2,3,0⟩, ⟨0,0,1⟩, ⟨1,1,1⟩, ⟨2,2,1⟩, ⟨3,3,1⟩]

theorem sound : ¬ Alternating.PlainDepth.Goal := by
  intro h
  have h5 : 3 ∈ query Alternating.model gC 0 5 id := by decide
  have h4 : 3 ∉ query Alternating.model gC 0 (vertices gC 0).length id := by decide
  exact h4 (h gC 0 5 3 h5)

end TinylambOpt.ResearchProofs.LayerContracts.RecursivePaths.Goal_Alternating_PlainDepth
