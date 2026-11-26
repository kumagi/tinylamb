import TinylambOpt.ResearchGoals.JitKernels.InListLowering

/-!
`WHERE IN` acceptance and `WHERE NOT IN` acceptance treat NULL build keys differently.
Dropping NULL build rows therefore preserves `keep` but need not preserve the
`some false` observation: a probe with no matching value becomes FALSE instead
of NULL. The explicit list `[NULL]` witnesses that the stronger claim is false.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.InListLowering.Goal_NullEntryAntiFilterElision

open TinylambOpt.ResearchGoals.JitKernels.InListLowering

theorem refute : ¬ NullEntryAntiFilterElision.Goal := by
  intro h
  have t := h [none] (some (0 : Int))
  simp [test] at t

end TinylambOpt.ResearchProofs.JitKernels.InListLowering.Goal_NullEntryAntiFilterElision
