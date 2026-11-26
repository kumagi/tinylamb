import TinylambOpt.ResearchGoals.JitKernels.DispatchTables

set_option maxHeartbeats 0
set_option maxRecDepth 10000

/-! Two adjacent biased jump tables concatenate to the table for the combined
range. Emitting separate hot and cold sub-tables therefore lets a single
comparison choose a half without changing any entry of the dispatch table. -/

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_DispatchTables_SplitTableAtHotBoundary

open TinylambOpt.ResearchGoals.JitKernels.DispatchTables

theorem sound : TinylambOpt.ResearchGoals.JitKernels.DispatchTables.SplitTableAtHotBoundary.Goal := by
  intro as base a b
  simp [tab, List.range_add]
  intro i hi
  have heq : base + (a : Int) + (i : Int) = base + ((a : Int) + (i : Int)) := by omega
  rw [heq]

end TinylambOpt.ResearchProofs.JitKernels.Goal_DispatchTables_SplitTableAtHotBoundary