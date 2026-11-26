import TinylambOpt.ResearchGoals.JitKernels.DeltaLaneScan

/-! With nonnegative deltas every decoded prefix is at most the final endpoint.
An endpoint below the bound therefore fences every row, letting SQL replace a
per-row range check with one check on the frame sum. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.DeltaLaneScan.Goal_EndpointDeltaRangeFence

open TinylambOpt.ResearchGoals.JitKernels.DeltaLaneScan

theorem sumNonneg (ds : List Int) (h : ∀ d ∈ ds, 0 ≤ d) : 0 ≤ ds.sum := by
  induction ds with
  | nil => simp
  | cons d ds ih =>
      have hd : 0 ≤ d := h d (by simp)
      have ht : ∀ e ∈ ds, 0 ≤ e := by intro e he; exact h e (by simp [he])
      have hs := ih ht
      simp [List.sum_cons]
      omega

theorem scanlEqConsValues (b : Int) (ds : List Int) :
    List.scanl (· + ·) b ds = b :: values b ds := by
  cases ds with
  | nil => simp [values]
  | cons d ds => simp [values, List.scanl_cons]

theorem sound : EndpointDeltaRangeFence.Goal := by
  intro b ds threshold hnonneg hlt
  induction ds generalizing b with
  | nil => simp [values]
  | cons d ds ih =>
      have hd : 0 ≤ d := hnonneg d (by simp)
      have ht : ∀ e ∈ ds, 0 ≤ e := by intro e he; exact hnonneg e (by simp [he])
      have htailNonneg : 0 ≤ ds.sum := sumNonneg ds ht
      have hbd_le : b + d ≤ b + (d :: ds).sum := by simp [List.sum_cons]; omega
      have hbd : b + d < threshold := by omega
      have htail_lt : (b + d) + ds.sum < threshold := by
        simp [List.sum_cons] at hlt
        omega
      have ihres := ih (b + d) ht htail_lt
      simp only [values, List.scanl_cons]
      rw [List.drop_one]
      rw [scanlEqConsValues]
      simp only [List.tail_cons]
      simp [hbd, ihres]

end TinylambOpt.ResearchProofs.JitKernels.DeltaLaneScan.Goal_EndpointDeltaRangeFence
