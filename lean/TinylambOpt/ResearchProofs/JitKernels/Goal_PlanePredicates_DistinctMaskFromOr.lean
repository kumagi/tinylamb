import TinylambOpt.ResearchGoals.JitKernels.PlanePredicates

/-!
The OR of all lanes is a lossy synopsis: it records which bits appear anywhere,
but not which values occur together in a lane. This file refutes the claim that
two equal-length frames with the same OR have the same set of distinct values.

Refuted by counterexample: `[3,3]` and `[1,2]` have OR `3`, but their distinct
value sets are `{3}` and `{1,2}`.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_PlanePredicates_DistinctMaskFromOr

open TinylambOpt.ResearchGoals.JitKernels.PlanePredicates

theorem refute : ¬ DistinctMaskFromOr.Goal := by
  intro h
  have heq := h [3, 3] [1, 2] (by native_decide) (by native_decide)
  exact (by native_decide : ¬ List.Perm [3, 3].eraseDups [1, 2].eraseDups) heq

end TinylambOpt.ResearchProofs.JitKernels.Goal_PlanePredicates_DistinctMaskFromOr
