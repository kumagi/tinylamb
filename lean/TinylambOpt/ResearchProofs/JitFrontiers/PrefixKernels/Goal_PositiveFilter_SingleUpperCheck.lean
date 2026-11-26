import TinylambOpt.ResearchGoals.JitFrontiers.PrefixKernels
import TinylambOpt.ResearchProofs.JitFrontiers.PrefixKernels.Common_PrefixKernels

/-!
Prefix computations can use vector or block summaries when their carried state produces
the same per-row results.

Proved

- `PositiveFilter.SingleUpperCheck` — `∀ w s rs, fits w s → (cert update w s rs=true ↔
  apply (whole update rs) s < limit w)`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitFrontiers.PrefixKernels.Goal_PositiveFilter_SingleUpperCheck

open TinylambOpt.ResearchProofs.JitFrontiers.PrefixKernels
open TinylambOpt.ResearchGoals.JitFrontiers.PrefixKernels
open TinylambOpt.ResearchGoals.JitFrontiers.CheckedRewrites (fits limit)

theorem sound : PositiveFilter.SingleUpperCheck.Goal := by
  intro w s rs hs
  have hc := cert_slope_one_nonneg PositiveFilter.update w
    (by intro r; simp [PositiveFilter.update])
    (by intro r; exact Int.le_max_left 0 (Option.getD r 0)) s rs hs
  rw [hc]
  have hsum : apply (whole PositiveFilter.update rs) s =
      s + (rs.map (fun r => (PositiveFilter.update r).2)).sum := by
    rw [whole_slope_one PositiveFilter.update (by intro r; simp [PositiveFilter.update])]
    simp [apply]
  rw [hsum]
  rw [fits_true]
  constructor
  · intro h
    exact h.2
  · intro hupper
    have hsL := (fits_true w s).mp hs
    have hnonneg : 0 ≤ (rs.map (fun r => (PositiveFilter.update r).2)).sum := by
      apply sum_nonneg
      intro x hx
      rcases List.mem_map.mp hx with ⟨r, hr, hxr⟩
      subst x
      exact Int.le_max_left 0 (Option.getD r 0)
    constructor
    · omega
    · exact hupper


end TinylambOpt.ResearchProofs.JitFrontiers.PrefixKernels.Goal_PositiveFilter_SingleUpperCheck
