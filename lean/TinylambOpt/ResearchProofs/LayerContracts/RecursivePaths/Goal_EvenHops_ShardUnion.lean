import TinylambOpt.ResearchGoals.LayerContracts.RecursivePaths

/-!
A recursive query uses visited states and frontier updates to avoid redundant work while
retaining visits that affect path acceptance.

Refuted by counterexample

- `EvenHops.ShardUnion` — Can independently traversed edge shards be unioned without a
  cross-shard work queue?
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.RecursivePaths.Goal_EvenHops_ShardUnion

open TinylambOpt.ResearchGoals.LayerContracts.RecursivePaths
open TinylambOpt.ResearchGoals.LayerContracts.RecursivePaths.EvenHops

theorem refute : ¬ EvenHops.ShardUnion.Goal := by
  intro h
  let a : List Edge := [⟨0,0,0⟩]
  let b : List Edge := [⟨0,1,0⟩]
  have hi := h a b 0 2 1
  have hl : 1 ∈ query EvenHops.model (a++b) 0 2 id := by native_decide
  have hr : ¬ (1 ∈ query EvenHops.model a 0 2 id ∨ 1 ∈ query EvenHops.model b 0 2 id) := by
    native_decide
  exact hr (hi.mp hl)

end TinylambOpt.ResearchProofs.LayerContracts.RecursivePaths.Goal_EvenHops_ShardUnion
