import TinylambOpt.ResearchGoals.JitKernels.TierInvestment

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

/-!
Soundness of "a single joint dispatch for two batches with identical guard outcomes never
costs more than two dispatches": when both batches take the same compiled path, the fused
batch pays the fixed guard fee once instead of twice, and the per-row charge is additive
(`fast·(ra+rb) = fast·ra + fast·rb`), so fusion of same-shape batches is always a win (or
a tie). This justifies the JIT kernel fusing adjacent batches whose guards agree.
-/
namespace TinylambOpt.ResearchProofs.JitKernels.TierInvestment_SameShapeBatchFusion
open TinylambOpt.ResearchGoals.JitKernels.TierInvestment

theorem sound : SameShapeBatchFusion.Goal := by
  intro t a b hp
  rcases t with ⟨c, gf, sl, fs, rp⟩
  rcases a with ⟨ra, pa⟩
  rcases b with ⟨rb, pb⟩
  rcases pa with _ | _
  · have hp' : pb = false := hp.symm
    subst hp'
    show gf + (rp + sl * (ra + rb)) ≤ (gf + (rp + sl * ra)) + (gf + (rp + sl * rb))
    rw [Nat.mul_add]
    omega
  · have hp' : pb = true := hp.symm
    subst hp'
    show gf + (fs * (ra + rb)) ≤ (gf + fs * ra) + (gf + fs * rb)
    rw [Nat.mul_add]
    omega

end TinylambOpt.ResearchProofs.JitKernels.TierInvestment_SameShapeBatchFusion
