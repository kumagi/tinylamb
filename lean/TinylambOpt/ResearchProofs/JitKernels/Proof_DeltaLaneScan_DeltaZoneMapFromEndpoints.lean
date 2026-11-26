import TinylambOpt.ResearchGoals.JitKernels.DeltaLaneScan

/-!
Refutation: positive net drift does not bound intermediate rows when individual
deltas have mixed signs. With `b = 0`, `ds = [10,-10]`, `cap = 0`: the sum is
`0 ≥ 0` and `b + ds.sum = 0 ≤ cap`, yet `values 0 [10,-10] = [10, 0]` contains
10, so `(values b ds).all (fun x => x ≤ cap)` is false. Zone maps must still
track running maxima (or per-run bounds), not just endpoints.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.JitKernels.DeltaLaneScan_DeltaZoneMapFromEndpoints
open TinylambOpt.ResearchGoals.JitKernels.DeltaLaneScan

theorem refute : ¬ DeltaZoneMapFromEndpoints.Goal := by
  intro h
  have h' := h 0 [10,-10] 0 (by decide) (by decide)
  exact absurd h' (by decide)

end TinylambOpt.ResearchProofs.JitKernels.DeltaLaneScan_DeltaZoneMapFromEndpoints
