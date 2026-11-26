import TinylambOpt.ResearchGoals.JitKernels.BailoutPackets

/-
Refutes `MisalignedPacketResume`: vector-aligned resume skips a fault that lies
inside the current packet. The witness has faults at lanes 0 and 1 with
width 2, so resuming at the next aligned offset loses lane 1.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.BailoutPackets.Goal_MisalignedPacketResume

open TinylambOpt.ResearchGoals.JitKernels.BailoutPackets

theorem refute : ¬ MisalignedPacketResume.Goal := by
  intro h
  let xs : List Cell := [.error 0, .error 1, .ok (some 5)]
  have hf : first xs = some (0, 0) := by native_decide
  have hbad := h xs 0 0 2 (by omega) hf
  have hl : prefixOut (xs.drop 1) = [] := by native_decide
  have hr : prefixOut (xs.drop 2) = [5] := by native_decide
  rw [hl, hr] at hbad
  contradiction

end TinylambOpt.ResearchProofs.JitKernels.BailoutPackets.Goal_MisalignedPacketResume
