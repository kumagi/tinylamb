import TinylambOpt.ResearchGoals.JitKernels.BailoutPackets

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.JitKernels.BailoutPackets_LaneMajorSitePriority
open TinylambOpt.ResearchGoals.JitKernels.BailoutPackets

/--
`LaneMajorSitePriority.Goal` claims the site-major result `first a orElse first b`
equals the first fault of the lane-merged vector. This is false: an error site in
`a` at lane 1 must beat an error site in `b` at lane 0, but the merged lanes carry
the `b`-error already at lane 0 (lane-major priority), since `a`'s lane 0 is fine.
-/
theorem refute : ¬ LaneMajorSitePriority.Goal := by
  intro h
  exact absurd (h [.ok (some 0), .error 1] [.error 2, .ok (some 0)] rfl) (by decide)

end TinylambOpt.ResearchProofs.JitKernels.BailoutPackets_LaneMajorSitePriority
