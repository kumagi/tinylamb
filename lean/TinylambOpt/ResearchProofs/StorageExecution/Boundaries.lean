import TinylambOpt.ResearchGoals.StorageExecution.Boundaries

namespace TinylambOpt.ResearchProofs.StorageExecution.Boundaries

open TinylambOpt.ResearchGoals.StorageExecution

/-- The empty instance costs zero. -/
theorem boundaryCost_zero (edge : Nat → Nat → Nat) :
    boundaryCost 0 edge = 0 := by
  unfold boundaryCost
  simp

/-- A single nonempty segment costs its edge. -/
theorem boundaryCost_one (edge : Nat → Nat → Nat) :
    boundaryCost 1 edge = edge 0 1 := by
  unfold boundaryCost
  simp

/-- Two positions choose the cheaper of one or two segments. -/
theorem boundaryCost_two (edge : Nat → Nat → Nat) :
    boundaryCost 2 edge = min (edge 0 1 + edge 1 2) (edge 0 2) := by
  unfold boundaryCost
  simp [List.range_succ, List.map, List.min?]

/-- The documented guard example. -/
theorem boundaryCost_guard_example :
    boundaryCost 4 (fun a b => 4 + (b - a) ^ 2) = 16 := by
  native_decide

end TinylambOpt.ResearchProofs.StorageExecution.Boundaries
