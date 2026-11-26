import TinylambOpt.ResearchGoals.JitKernels.DeltaLaneScan

/-! For a constant-delta run the delta at each position is added once per later
row, so row `i` equals base plus `(i+1)` times the delta. SQL can address such
runs arithmetically instead of materializing the prefix scan. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_DeltaLaneScan_RepeatedDeltaRunDirectSelect

open TinylambOpt.ResearchGoals.JitKernels.DeltaLaneScan

theorem sound : RepeatedDeltaRunDirectSelect.Goal := by
  intro b d n i hi
  simp [TinylambOpt.ResearchGoals.JitKernels.DeltaLaneScan.get, List.take_replicate]
  have hmin : min (i + 1) n = i + 1 := by omega
  rw [hmin]
  simp

end TinylambOpt.ResearchProofs.JitKernels.Goal_DeltaLaneScan_RepeatedDeltaRunDirectSelect
