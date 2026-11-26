import TinylambOpt.ResearchGoals.LayerContracts.RecursivePaths

/-!
A recursive query uses visited states and frontier updates to avoid redundant work while
retaining visits that affect path acceptance.

Refuted by counterexample

- `LastBlue.ShardUnion` — Can independently traversed edge shards be unioned without a
  cross-shard work queue?
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.LayerContracts.RecursivePaths.Goal_LastBlue_ShardUnion

open TinylambOpt.ResearchGoals.LayerContracts.RecursivePaths
open TinylambOpt.ResearchGoals.LayerContracts.RecursivePaths.LastBlue

theorem refute : ¬ LastBlue.ShardUnion.Goal := by
  intro h
  let a : List Edge := [⟨0,0,2⟩]
  let b : List Edge := [⟨1,0,0⟩]
  have hi := h a b 1 2 0
  have hl : 0 ∈ query LastBlue.model (a++b) 1 2 id := by native_decide
  have hr : ¬ (0 ∈ query LastBlue.model a 1 2 id ∨ 0 ∈ query LastBlue.model b 1 2 id) := by
    native_decide
  exact hr (hi.mp hl)

end TinylambOpt.ResearchProofs.LayerContracts.RecursivePaths.Goal_LastBlue_ShardUnion
