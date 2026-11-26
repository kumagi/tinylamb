import TinylambOpt.ResearchGoals.JitKernels.DecimalChunks

/-! Dropping all leading zeros can make the input empty, and the empty input is
rejected by the parser, while an all-zero decimal word parses as zero. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.DecimalChunks.Goal_TrimZerosPreservesAllZeroInput

open TinylambOpt.ResearchGoals.JitKernels.DecimalChunks

theorem refute : ¬ TrimZerosPreservesAllZeroInput.Goal := by
  intro h
  have hw := h 0 false [48] (by simp [digits])
  simp [parse, digits, magnitude] at hw

end TinylambOpt.ResearchProofs.JitKernels.DecimalChunks.Goal_TrimZerosPreservesAllZeroInput
