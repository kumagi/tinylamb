import TinylambOpt.ResearchGoals.JitKernels.ConflictScatter

/-! A slot whose contributions cancel still exists. Removing zero reduced
deltas from the occupancy list would lose such a group from emitted SQL rows. -/
set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_ConflictScatter_ZeroDeltaOccupancyElision

open TinylambOpt.ResearchGoals.JitKernels.ConflictScatter

theorem refute : ¬ TinylambOpt.ResearchGoals.JitKernels.ConflictScatter.ZeroDeltaOccupancyElision.Goal := by
  intro h
  have bad := h [(0, 1), (0, -1)]
  have hc : combine [(0, 1), (0, -1)] = [(0, 0)] := by native_decide
  have hf : (combine [(0, 1), (0, -1)]).filter (fun u => u.2 != 0) = [] := by
    rw [hc]
    native_decide
  have hl : leaders [(0, 1), (0, -1)] = [0] := by native_decide
  have hempty : leaders ([] : Updates) = [] := by native_decide
  rw [hf, hl, hempty] at bad
  cases bad

end TinylambOpt.ResearchProofs.JitKernels.Goal_ConflictScatter_ZeroDeltaOccupancyElision