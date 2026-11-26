import TinylambOpt.ResearchGoals.JitKernels.DeltaLaneScan

/-! A positive net drift says nothing about interior rows: deltas `[10, -5]`
end at five but the first decoded row is ten, above the endpoint cap. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.DeltaLaneScan.Goal_DeltaZoneMapFromEndpoints

open TinylambOpt.ResearchGoals.JitKernels.DeltaLaneScan

theorem refute : ¬ DeltaZoneMapFromEndpoints.Goal := by
  intro h
  have hw := h 0 [10, -5] 5 (by native_decide) (by native_decide)
  have hm : (10 : Int) ∈ values 0 [10, -5] := by native_decide
  have hb := (List.all_eq_true.mp hw) 10 hm
  simp at hb

end TinylambOpt.ResearchProofs.JitKernels.DeltaLaneScan.Goal_DeltaZoneMapFromEndpoints
