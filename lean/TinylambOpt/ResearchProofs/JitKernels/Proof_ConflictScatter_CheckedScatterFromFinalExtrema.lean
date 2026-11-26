import TinylambOpt.ResearchGoals.JitKernels.ConflictScatter

/-!
Bounding only the initial and the final reduced counter value does not certify
that every intermediate partial sum of the checked scatter stays in range: with
`us = (0,5) :: (0,-5) :: []`, `s = 0`, `lo = hi = 0` the endpoints are `0` and
`0`, yet the scan visits `5`. Hence `CheckedScatterFromFinalExtrema.Goal` fails
and the guard must see more than the two endpoints.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.JitKernels.ConflictScatter_CheckedScatterFromFinalExtrema
open TinylambOpt.ResearchGoals.JitKernels.ConflictScatter

theorem refute : ¬ CheckedScatterFromFinalExtrema.Goal := by
  intro h
  simp only [CheckedScatterFromFinalExtrema.Goal] at h
  have hchk : ¬ check 0 0 0 [(0, 5), (0, -5)] 0 := by
    unfold check
    decide
  exact hchk (h [(0, 5), (0, -5)] 0 0 0 0 (by decide) (by decide) (by decide) (by decide))

end TinylambOpt.ResearchProofs.JitKernels.ConflictScatter_CheckedScatterFromFinalExtrema
