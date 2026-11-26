import TinylambOpt.ResearchGoals.November26.Tpch

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.November26.Tpch.Goal_Q10
open TinylambOpt.ResearchGoals.November26
open TinylambOpt.ResearchGoals.November26.Tpch

theorem sound : TinylambOpt.ResearchGoals.November26.Tpch.Q10.Goal := by
  intro ids rank attrs
  dsimp [sortedBy]
  rw [← List.map_mergeSort (f := fun i => (i, attrs i))
        (r := fun a b : Nat => decide (rank a ≤ rank b))
        (s := fun a b : Nat × String => decide (rank a.1 ≤ rank b.1))]
  · rw [List.map_take]
  · intro a ha b hb
    rfl
end TinylambOpt.ResearchProofs.November26.Tpch.Goal_Q10
