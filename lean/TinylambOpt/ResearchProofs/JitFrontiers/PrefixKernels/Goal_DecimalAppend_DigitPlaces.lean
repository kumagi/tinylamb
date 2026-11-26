import TinylambOpt.ResearchGoals.JitFrontiers.PrefixKernels
import TinylambOpt.ResearchProofs.JitFrontiers.PrefixKernels.Common_PrefixKernels

/-!
Prefix computations can use vector or block summaries when their carried state produces
the same per-row results.

Proved

- `DecimalAppend.DigitPlaces` — `∀ rs, (whole update rs).1=(10:Int)^((rs.filterMap
  id).length)`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitFrontiers.PrefixKernels.Goal_DecimalAppend_DigitPlaces

open TinylambOpt.ResearchProofs.JitFrontiers.PrefixKernels
open TinylambOpt.ResearchGoals.JitFrontiers.PrefixKernels
open TinylambOpt.ResearchGoals.JitFrontiers.CheckedRewrites (fits limit)

theorem sound : DecimalAppend.DigitPlaces.Goal := by
  intro rs
  induction rs with
  | nil => rfl
  | cons r rs ih =>
      rw [whole_cons]
      cases r with
      | none =>
          simp [DecimalAppend.update, compose, ih]
      | some x =>
          rcases h : whole DecimalAppend.update rs with ⟨A, B⟩
          have ihA : A = (10 : Int) ^ (List.filterMap id rs).length := by simpa [h] using ih
          have hgoal : (compose (A, B) (DecimalAppend.update (some x))).fst =
              (10 : Int) ^ ((List.filterMap id (some x :: rs)).length) := by
            simp [DecimalAppend.update, compose, ihA, Int.pow_succ]
          simpa [h] using hgoal


end TinylambOpt.ResearchProofs.JitFrontiers.PrefixKernels.Goal_DecimalAppend_DigitPlaces
