import TinylambOpt.ResearchGoals.JitFrontiers.PrefixKernels
import TinylambOpt.ResearchProofs.JitFrontiers.PrefixKernels.Common_PrefixKernels

/-!
Prefix computations can use vector or block summaries when their carried state produces
the same per-row results.

Proved

- `SignedMagnitude.NegativePair` — `∀ x y, x<0 → y<0 → whole update [some x,some
  y]=(1,x-y)`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitFrontiers.PrefixKernels.Goal_SignedMagnitude_NegativePair

open TinylambOpt.ResearchProofs.JitFrontiers.PrefixKernels
open TinylambOpt.ResearchGoals.JitFrontiers.PrefixKernels
open TinylambOpt.ResearchGoals.JitFrontiers.CheckedRewrites (fits limit)

theorem sound : SignedMagnitude.NegativePair.Goal := by
  intro x y hx hy
  rw [whole_cons, whole_cons]
  simp [whole, SignedMagnitude.update, compose]
  have hxle : x ≤ 0 := by omega
  have hyle : y ≤ 0 := by omega
  simp [SignedMagnitude.update, compose, hx, hy,
    Int.ofNat_natAbs_of_nonpos hxle, Int.ofNat_natAbs_of_nonpos hyle]
  omega


end TinylambOpt.ResearchProofs.JitFrontiers.PrefixKernels.Goal_SignedMagnitude_NegativePair
