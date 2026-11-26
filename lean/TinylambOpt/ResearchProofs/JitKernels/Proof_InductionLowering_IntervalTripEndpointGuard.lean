import TinylambOpt.ResearchGoals.JitKernels.InductionLowering

/-! Independent proof record for the InductionLowering.IntervalTripEndpointGuard research goal: the proposed rewrite is sound and preserves the modeled result. -/

/-!
For a monotone affine induction, both endpoints being in the signed range
forces every intermediate iteration to be in range. With a nonnegative step
the values lie between the two endpoints; with a negative step the order is
reversed. SQL execution may therefore validate only the two loop endpoints.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace Nov26.Proof_InductionLowering_IntervalTripEndpointGuard
open TinylambOpt.ResearchGoals.JitKernels.InductionLowering
open TinylambOpt.ResearchGoals.JitKernels.InductionLowering.IntervalTripEndpointGuard
set_option linter.unusedSimpArgs false

open TinylambOpt.ResearchGoals.JitKernels.InductionLowering

theorem sound : Goal := by
  intro w base step n i hi hb he
  unfold signedFits at hb he ⊢
  rcases hb with ⟨hb1, hb2⟩
  rcases he with ⟨he1, he2⟩
  by_cases hs : 0 ≤ step
  · have hle : (i : Int) * step ≤ (n : Int) * step := by
      apply Int.mul_le_mul_of_nonneg_right
      · omega
      · exact hs
    have hnonneg : 0 ≤ (i : Int) * step := Int.mul_nonneg (by omega) hs
    omega
  · have hs' : step ≤ 0 := by omega
    have hle : (n : Int) * step ≤ (i : Int) * step := by
      apply Int.mul_le_mul_of_nonpos_right
      · omega
      · exact hs'
    have hnonpos : (i : Int) * step ≤ 0 :=
      Int.mul_nonpos_of_nonneg_of_nonpos (by omega) hs'
    omega

end Nov26.Proof_InductionLowering_IntervalTripEndpointGuard
