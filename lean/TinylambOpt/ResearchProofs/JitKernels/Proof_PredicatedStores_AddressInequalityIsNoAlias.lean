import TinylambOpt.ResearchGoals.JitKernels.PredicatedStores

set_option maxHeartbeats 4000000
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.JitKernels.PredicatedStores_AddressInequalityIsNoAlias
open TinylambOpt.ResearchGoals.JitKernels.PredicatedStores

/-- REFUTED: unequal aligned bases do not imply disjointness for wide stores.
With `a = 0` writing eight `1`-bytes and `b = 4` writing four `2`-bytes, the
interval `[4,8)` overlaps, and byte 5 reads `2` in one order but `1` in the other. -/
theorem refute : ¬ AddressInequalityIsNoAlias.Goal := by
  intro h
  have hc :=
    h (fun _ => 0) 0 4 (List.replicate 8 1) (List.replicate 4 2)
      (by decide) (by decide) (by decide) (by decide) (by decide)
  exact absurd (congrFun hc 5) (by decide)

end TinylambOpt.ResearchProofs.JitKernels.PredicatedStores_AddressInequalityIsNoAlias
