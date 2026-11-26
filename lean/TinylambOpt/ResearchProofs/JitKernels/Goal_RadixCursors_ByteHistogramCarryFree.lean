import TinylambOpt.ResearchGoals.JitKernels.RadixCursors

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_RadixCursors_ByteHistogramCarryFree

open TinylambOpt.ResearchGoals.JitKernels.RadixCursors

theorem refute : ¬ TinylambOpt.ResearchGoals.JitKernels.RadixCursors.ByteHistogramCarryFree.Goal := by
  intro h
  let a : List (Option Nat) := List.replicate 128 (some 0)
  let b : List (Option Nat) := List.replicate 128 (some 0)
  have ha : (active a).count 0 < 256 := by native_decide
  have hb : (active b).count 0 < 256 := by native_decide
  have hres := h a b 0 ha hb
  have hbad : (active (a ++ b)).count 0 % 256 ≠ (active (a ++ b)).count 0 := by
    native_decide
  exact hbad hres

end TinylambOpt.ResearchProofs.JitKernels.Goal_RadixCursors_ByteHistogramCarryFree
