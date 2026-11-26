import TinylambOpt.ResearchGoals.DatabaseCorrectness.ChunkSlices

/-! A LIMIT that lands on the boundary between a constant empty string and a
    NULL chunk must keep the empty string distinct from NULL. -/
set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.DatabaseCorrectness.ChunkSlices.Goal_EmptyStringNullSliceBoundary

open TinylambOpt.ResearchGoals.DatabaseCorrectness.ChunkSlices

theorem sound : EmptyStringNullSliceBoundary.Goal := by
  intro n m hn hm
  have hnm : n - (n - 1) = 1 := by omega
  have hn0 : (n - 1) - n = 0 := by omega
  have hm0 : m - 0 = m := by omega
  have h1m : 1 ≤ m := by omega
  simp [unionSlice, portion, rows, hnm, hn0, hm0, Nat.min_eq_left h1m]

end TinylambOpt.ResearchProofs.DatabaseCorrectness.ChunkSlices.Goal_EmptyStringNullSliceBoundary
