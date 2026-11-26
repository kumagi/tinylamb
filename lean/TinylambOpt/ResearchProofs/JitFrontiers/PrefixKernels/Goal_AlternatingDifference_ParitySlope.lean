import TinylambOpt.ResearchGoals.JitFrontiers.PrefixKernels
import TinylambOpt.ResearchProofs.JitFrontiers.PrefixKernels.Common_PrefixKernels

/-!
Prefix computations can use vector or block summaries when their carried state produces
the same per-row results.

Proved

- `AlternatingDifference.ParitySlope` — `∀ rs, (whole update
  rs).1=(-1:Int)^((rs.filterMap id).length)`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitFrontiers.PrefixKernels.Goal_AlternatingDifference_ParitySlope

open TinylambOpt.ResearchProofs.JitFrontiers.PrefixKernels
open TinylambOpt.ResearchGoals.JitFrontiers.PrefixKernels
open TinylambOpt.ResearchGoals.JitFrontiers.CheckedRewrites (fits limit)

theorem sound : AlternatingDifference.ParitySlope.Goal := by
  intro rs
  induction rs with
  | nil => rfl
  | cons r rs ih =>
      rw [whole_cons]
      cases r with
      | none =>
          simp [AlternatingDifference.update, ih, compose]
      | some x =>
          rcases h : whole AlternatingDifference.update rs with ⟨A, B⟩
          rw [h] at ih
          change A = (-1 : Int) ^ (List.filterMap id rs).length at ih
          change A * (-1 : Int) = (-1 : Int) ^ ((List.filterMap id rs).length + 1)
          rw [ih, Int.pow_succ]


end TinylambOpt.ResearchProofs.JitFrontiers.PrefixKernels.Goal_AlternatingDifference_ParitySlope
