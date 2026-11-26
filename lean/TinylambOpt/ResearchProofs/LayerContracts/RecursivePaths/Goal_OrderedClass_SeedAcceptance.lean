import TinylambOpt.ResearchGoals.LayerContracts.RecursivePaths

/-!
A recursive query uses visited states and frontier updates to avoid redundant work while
retaining visits that affect path acceptance.

Proved

- `OrderedClass.SeedAcceptance` — Stopping on first physical encounter with a vertex is
  enough to decide acceptance.
-/

set_option maxHeartbeats 0

namespace TinylambOpt.ResearchProofs.LayerContracts.RecursivePaths.Goal_OrderedClass_SeedAcceptance

open TinylambOpt.ResearchGoals.LayerContracts.RecursivePaths

theorem sound : OrderedClass.SeedAcceptance.Goal := by
  intro labels a b h1 h2 h3
  simp [OrderedClass.model]

end TinylambOpt.ResearchProofs.LayerContracts.RecursivePaths.Goal_OrderedClass_SeedAcceptance
