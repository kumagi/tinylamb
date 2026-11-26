import TinylambOpt.ResearchGoals.JitKernels.CivilTextGathers

/-! Equal row lengths do not certify equal digits. Two rows of the same length
may contain different bytes at the validated hour offset, so the per-row digit
check cannot be elided from SQL execution. -/
set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_CivilTextGathers_FirstRowFormatCertificate

open TinylambOpt.ResearchGoals.JitKernels.CivilTextGathers

theorem refute : ¬ FirstRowFormatCertificate.Goal := by
  intro h
  have bad := h (List.replicate 19 48) (List.replicate 19 97) (by simp) (by simp) (by native_decide)
  simp [pair] at bad

end TinylambOpt.ResearchProofs.JitKernels.Goal_CivilTextGathers_FirstRowFormatCertificate
