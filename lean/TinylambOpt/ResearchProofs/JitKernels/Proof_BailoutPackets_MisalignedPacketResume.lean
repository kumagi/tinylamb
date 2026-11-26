import TinylambOpt.ResearchGoals.JitKernels.BailoutPackets

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.JitKernels.BailoutPackets_MisalignedPacketResume
open TinylambOpt.ResearchGoals.JitKernels.BailoutPackets

/--
`MisalignedPacketResume.Goal` claims that resuming right after the fault row
(`i+1`) yields the same prefix output as resuming at the next word-aligned offset
(`(i/w+1)*w`). This is false: the alignment jump skips successful rows that follow
the fault inside the same word.
Witness: `xs = [.error 1, .ok (some 9)]`, `i = 0`, `w = 2`: dropping 1 keeps 9,
dropping 2 keeps nothing.
-/
theorem refute : ¬ MisalignedPacketResume.Goal := by
  intro h
  exact absurd (h [.error 1, .ok (some 9)] 0 1 2 (by decide) rfl) (by decide)

end TinylambOpt.ResearchProofs.JitKernels.BailoutPackets_MisalignedPacketResume
