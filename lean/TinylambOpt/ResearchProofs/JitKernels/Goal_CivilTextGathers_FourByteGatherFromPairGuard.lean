import TinylambOpt.ResearchGoals.JitKernels.CivilTextGathers
set_option linter.unusedSimpArgs false

/-! A guard for two bytes does not license a four-byte load: the row may end
immediately after the validated pair. This refutation keeps SQL execution from
widening the gather at the end of a buffer. -/
set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_CivilTextGathers_FourByteGatherFromPairGuard

open TinylambOpt.ResearchGoals.JitKernels.CivilTextGathers

theorem refute : ¬ FourByteGatherFromPairGuard.Goal := by
  intro h
  have bad := h 0 2 0 (by omega)
  omega

end TinylambOpt.ResearchProofs.JitKernels.Goal_CivilTextGathers_FourByteGatherFromPairGuard
