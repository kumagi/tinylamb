import TinylambOpt.ResearchGoals.JitFrontiers.PrefixKernels
import TinylambOpt.ResearchProofs.JitFrontiers.PrefixKernels.Common_PrefixKernels

/-!
Prefix computations can use vector or block summaries when their carried state produces
the same per-row results.

Proved

- `NonNullCount.MonotoneGuard` — `∀ w s rs, fits w s → (cert update w s rs=true ↔ fits
  w (s+(rs.filterMap id).length)=true)`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitFrontiers.PrefixKernels.Goal_NonNullCount_MonotoneGuard

open TinylambOpt.ResearchProofs.JitFrontiers.PrefixKernels
open TinylambOpt.ResearchGoals.JitFrontiers.PrefixKernels
open TinylambOpt.ResearchGoals.JitFrontiers.CheckedRewrites (fits limit)

theorem nn_sum (rs : List Row) :
    (rs.map (fun r => (NonNullCount.update r).2)).sum = ((rs.filterMap id).length : Int) := by
  induction rs with
  | nil => rfl
  | cons r rs ih =>
      rw [List.map_cons, List.sum_cons, ih]
      cases r <;> simp [NonNullCount.update] <;> omega

theorem sound : NonNullCount.MonotoneGuard.Goal := by
  intro w s rs hs
  have hc := cert_slope_one_nonneg NonNullCount.update w
    (by intro r; rfl) (by intro r; simp [NonNullCount.update]) s rs hs
  rw [hc, nn_sum]


end TinylambOpt.ResearchProofs.JitFrontiers.PrefixKernels.Goal_NonNullCount_MonotoneGuard
