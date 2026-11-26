import TinylambOpt.ResearchGoals.JitKernels.ConflictScatter

/-! Last-writer-wins is not a reduction of signed deltas: two lanes for the
same counter contribute both values, while only the last one survives the
substitution. -/
set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_ConflictScatter_LastLaneRmwSubstitution

open TinylambOpt.ResearchGoals.JitKernels.ConflictScatter

theorem refute : ¬ TinylambOpt.ResearchGoals.JitKernels.ConflictScatter.LastLaneRmwSubstitution.Goal := by
  intro h
  have bad := h [(0, 1), (0, 2)] (fun _ => 0) 0 (by native_decide)
  simp [sequential, delta, lastLane] at bad

end TinylambOpt.ResearchProofs.JitKernels.Goal_ConflictScatter_LastLaneRmwSubstitution
