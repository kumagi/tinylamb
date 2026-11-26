import TinylambOpt.ResearchGoals.JitKernels.BailoutPackets

/-
Refutes `LaneMajorSitePriority`: lane-major merging lets a later site's error
win over an earlier row's error from the other pass. The witness puts an error
at row 1 of `a` and row 0 of `b`.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.BailoutPackets.Goal_LaneMajorSitePriority

open TinylambOpt.ResearchGoals.JitKernels.BailoutPackets

theorem refute : ¬ LaneMajorSitePriority.Goal := by
  intro h
  let a : List Cell := [.ok (some 0), .error 5]
  let b : List Cell := [.error 7, .ok (some 0)]
  have hlen : a.length = b.length := by native_decide
  have hbad := h a b hlen
  simp [first, bad, a, b] at hbad

end TinylambOpt.ResearchProofs.JitKernels.BailoutPackets.Goal_LaneMajorSitePriority
