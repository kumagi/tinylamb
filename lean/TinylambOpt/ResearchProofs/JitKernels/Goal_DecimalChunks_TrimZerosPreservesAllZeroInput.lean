import TinylambOpt.ResearchGoals.JitKernels.DecimalChunks

/-! Trimming a single zero digit produces the empty input, which the parser
    rejects, while the original all-zero input parses successfully. -/

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_DecimalChunks_TrimZerosPreservesAllZeroInput

open TinylambOpt.ResearchGoals.JitKernels.DecimalChunks

theorem refute : ¬ TinylambOpt.ResearchGoals.JitKernels.DecimalChunks.TrimZerosPreservesAllZeroInput.Goal := by
  intro h
  have hh := h 1 false [48] (by native_decide)
  have hne : (parse 1 false ([48].dropWhile (fun c => c == 48)) ≠ parse 1 false [48]) := by
    native_decide
  exact hne hh

end TinylambOpt.ResearchProofs.JitKernels.Goal_DecimalChunks_TrimZerosPreservesAllZeroInput
