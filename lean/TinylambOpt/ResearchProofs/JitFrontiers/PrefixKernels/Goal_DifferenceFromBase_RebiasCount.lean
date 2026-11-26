import TinylambOpt.ResearchGoals.JitFrontiers.PrefixKernels
import TinylambOpt.ResearchProofs.JitFrontiers.PrefixKernels.Common_PrefixKernels

/-!
Prefix computations can use vector or block summaries when their carried state produces
the same per-row results.

Proved

- `DifferenceFromBase.RebiasCount` — `∀ rs, (whole update rs).2=(rs.filterMap id).sum -
  100*((rs.filterMap id).length:Int)`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitFrontiers.PrefixKernels.Goal_DifferenceFromBase_RebiasCount

open TinylambOpt.ResearchProofs.JitFrontiers.PrefixKernels
open TinylambOpt.ResearchGoals.JitFrontiers.PrefixKernels
open TinylambOpt.ResearchGoals.JitFrontiers.CheckedRewrites (fits limit)

theorem sound : DifferenceFromBase.RebiasCount.Goal := by
  intro rs
  have hmain : ∀ rs : List Row,
      whole DifferenceFromBase.update rs =
        (1, (rs.filterMap id).sum - 100 * ((rs.filterMap id).length : Int)) := by
    intro rs
    induction rs with
    | nil => rfl
    | cons r rs ih =>
        rw [whole_cons]
        cases r with
        | none =>
            rw [ih]
            simp [DifferenceFromBase.update, compose_one]
        | some x =>
            rw [ih]
            simp [DifferenceFromBase.update, compose_one]
            omega
  exact congrArg Prod.snd (hmain rs)


end TinylambOpt.ResearchProofs.JitFrontiers.PrefixKernels.Goal_DifferenceFromBase_RebiasCount
