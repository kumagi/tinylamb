import TinylambOpt.ResearchGoals.EngineBoundaries.SpillLeases

/-!
A spillable operator's resource accounting must protect partitions still referenced by
execution.

Refuted by counterexample

- `WrappedEpoch` — Limiting the stored generation to k bits keeps generations
  distinguishable for all lifetimes.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.EngineBoundaries.Goal_SpillLeases_WrappedEpoch

open TinylambOpt.ResearchGoals.EngineBoundaries.SpillLeases

theorem refute : ¬ WrappedEpoch.Goal := by
  intro h
  have hh := h 2 1 5 (by decide) (by decide)
  have hbad : ¬ (1 % 2^2 ≠ 5 % 2^2) := by native_decide
  exact hbad hh

end TinylambOpt.ResearchProofs.EngineBoundaries.Goal_SpillLeases_WrappedEpoch
