import TinylambOpt.ResearchGoals.LayerContracts.RecursivePaths

/-!
A recursive query uses visited states and frontier updates to avoid redundant work while
retaining visits that affect path acceptance.

Refuted by counterexample

- `OrderedClass.ShardUnion` — Can independently traversed edge shards be unioned
  without a cross-shard work queue?
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.RecursivePaths.Goal_OrderedClass_ShardUnion

open TinylambOpt.ResearchGoals.LayerContracts.RecursivePaths
open TinylambOpt.ResearchGoals.LayerContracts.RecursivePaths.OrderedClass

theorem refute : ¬ OrderedClass.ShardUnion.Goal := by
  intro h
  let a : List Edge := [⟨0,1,0⟩]
  let b : List Edge := [⟨1,2,0⟩]
  have hi := h a b 0 2 2
  have hl : 2 ∈ query OrderedClass.model (a++b) 0 2 id := by native_decide
  have hr : ¬ (2 ∈ query OrderedClass.model a 0 2 id ∨ 2 ∈ query OrderedClass.model b 0 2 id) := by
    native_decide
  exact hr (hi.mp hl)

end TinylambOpt.ResearchProofs.LayerContracts.RecursivePaths.Goal_OrderedClass_ShardUnion
