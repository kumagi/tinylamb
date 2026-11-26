import TinylambOpt.ResearchGoals.JitKernels.ReciprocalWindows

/-! Exactness at the range maximum does not imply exactness at every smaller
    dividend: an overestimate can later be caught up by the true quotient. -/

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_ReciprocalWindows_LastDividendCertifiesRange

open TinylambOpt.ResearchGoals.JitKernels.ReciprocalWindows

theorem refute : ¬ TinylambOpt.ResearchGoals.JitKernels.ReciprocalWindows.LastDividendCertifiesRange.Goal := by
  intro h
  have hpos : 0 < (3 : Nat) := by omega
  have hlast : exactAt 3 3 1 := by
    unfold exactAt guess magic
    native_decide
  have hh := h 3 3 1 hpos hlast 2 (by omega)
  have hbad : ¬ exactAt 2 3 1 := by
    unfold exactAt guess magic
    native_decide
  exact hbad hh

end TinylambOpt.ResearchProofs.JitKernels.Goal_ReciprocalWindows_LastDividendCertifiesRange
