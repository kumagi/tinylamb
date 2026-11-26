import TinylambOpt.ResearchGoals.JitKernels.ConflictScatter

/-!
Zero-delta leader elision does not preserve the occupied-slot list: when a
slot's reduced delta is exactly zero the slot disappears from the filtered
combination but remains a leader, e.g. `us = (0,0) :: []` has leaders `[0]`
while the filtered combination has no leaders. SQL group emission cannot use
the nonzero-delta leaders as the occupancy list.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.JitKernels.ConflictScatter_ZeroDeltaOccupancyElision
open TinylambOpt.ResearchGoals.JitKernels.ConflictScatter

theorem refute : ¬ ZeroDeltaOccupancyElision.Goal := by
  intro h
  have hne : leaders ((combine ([(0, 0)] : Updates)).filter fun u => u.2 != 0)
      ≠ leaders ([(0, 0)] : Updates) := by decide
  exact hne (h ([(0, 0)] : Updates))

end TinylambOpt.ResearchProofs.JitKernels.ConflictScatter_ZeroDeltaOccupancyElision
