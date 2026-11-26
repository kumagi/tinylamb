import TinylambOpt.ResearchGoals.JitKernels.CivilTextGathers

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.JitKernels.CivilTextGathers_ShiftBeforeDayFloor
open TinylambOpt.ResearchGoals.JitKernels.CivilTextGathers

/--
`ShiftBeforeDayFloor.Goal` claims truncating the local timestamp to its day
before subtracting the timezone displacement equals subtracting first. This is
false: the time-of-day remainder (up to 1439 minutes) can lift the sum across
the day boundary, changing the floor.
Witness: `d = 0`, `h = 23`, `m = 59`, `z = 1439`: left is (1439-1439)/1440 = 0,
right is (0-1439)/1440 = -1.
-/
theorem refute : ¬ ShiftBeforeDayFloor.Goal := by
  intro h
  exact absurd (h 0 23 59 1439 (by decide) (by decide) (by decide) (by decide))
    (by decide)

end TinylambOpt.ResearchProofs.JitKernels.CivilTextGathers_ShiftBeforeDayFloor
