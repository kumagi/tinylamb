import TinylambOpt.ResearchGoals.JitFrontiers.PrefixKernels
import TinylambOpt.ResearchProofs.JitFrontiers.PrefixKernels.Common_PrefixKernels

/-!
Prefix computations can use vector or block summaries when their carried state produces
the same per-row results.

Proved

- `Product.ZeroSuffix` — `∀ w s rs, execute update w 0 s (some 0::rs)=.ok 0`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitFrontiers.PrefixKernels.Goal_Product_ZeroSuffix

open TinylambOpt.ResearchProofs.JitFrontiers.PrefixKernels
open TinylambOpt.ResearchGoals.JitFrontiers.PrefixKernels
open TinylambOpt.ResearchGoals.JitFrontiers.CheckedRewrites (fits limit)

theorem sound : Product.ZeroSuffix.Goal := by
  intro w s rs
  have hmain : ∀ i, execute Product.update w i 0 rs = Except.ok 0 := by
    intro i
    induction rs generalizing i with
    | nil => simp [execute]
    | cons r rs ih =>
        unfold execute
        simp [Product.update, apply, fits_zero]
        exact ih (i + 1)
  unfold execute
  simp [Product.update, apply, fits_zero]
  exact hmain 1


end TinylambOpt.ResearchProofs.JitFrontiers.PrefixKernels.Goal_Product_ZeroSuffix
