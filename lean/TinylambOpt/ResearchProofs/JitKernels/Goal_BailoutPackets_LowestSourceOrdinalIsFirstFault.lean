import TinylambOpt.ResearchGoals.JitKernels.BailoutPackets

/-
Refutes `LowestSourceOrdinalIsFirstFault`: a selection vector that reorders
faulty rows can report a later source ordinal as the first compacted fault.
The witness uses `sel = [1,0]` with errors at both sources.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.BailoutPackets.Goal_LowestSourceOrdinalIsFirstFault

open TinylambOpt.ResearchGoals.JitKernels.BailoutPackets

theorem refute : ¬ LowestSourceOrdinalIsFirstFault.Goal := by
  intro h
  let xs : List Cell := [.error 7, .error 5]
  let sel : List Nat := [1, 0]
  have hf : first (gather xs sel) = some (0, 5) := by native_decide
  have hs : sel[1]? = some 0 := by rfl
  have hx : xs[0]? = some (.error 7) := by rfl
  have hbad := h xs sel 0 5 hf 1 0 7 hs hx
  simp [sel] at hbad

end TinylambOpt.ResearchProofs.JitKernels.BailoutPackets.Goal_LowestSourceOrdinalIsFirstFault
