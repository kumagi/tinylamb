import TinylambOpt.ResearchGoals.JitKernels.DeltaLaneScan

/-! Initial and final values fitting a narrow signed range do not constrain the
intermediate prefixes. Base five with deltas `[-10, 10]` ends at five but
passes through minus five. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.DeltaLaneScan.Goal_ScanWithModularCarry

open TinylambOpt.ResearchGoals.JitKernels.DeltaLaneScan

theorem refute : ¬ ScanWithModularCarry.Goal := by
  intro h
  have hw := h 5 [-10, 10] 3 (by decide) (by native_decide) (by native_decide)
  have hm : (-5 : Int) ∈ values 5 [-10, 10] := by native_decide
  have hb := hw (-5) hm
  simp at hb

end TinylambOpt.ResearchProofs.JitKernels.DeltaLaneScan.Goal_ScanWithModularCarry
