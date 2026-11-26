import TinylambOpt.ResearchGoals.JitKernels.DeltaLaneScan

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_DeltaLaneScan_EndpointDeltaRangeFence

theorem sound : TinylambOpt.ResearchGoals.JitKernels.DeltaLaneScan.EndpointDeltaRangeFence.Goal := by
  intro b ds threshold hnonneg hfinal
  have hsumNonneg : ∀ ds : List Int, (∀ d ∈ ds, 0 ≤ d) → 0 ≤ ds.sum := by
    intro ds
    induction ds with
    | nil => intro h; simp
    | cons d ds ih =>
        intro h
        have hd : 0 ≤ d := h d (by simp)
        have hds : 0 ≤ ds.sum := ih (by intro z hz; exact h z (by simp [hz]))
        simp only [List.sum_cons]
        omega
  have hscan : ∀ (ds : List Int) (acc : Int),
      (∀ d ∈ ds, 0 ≤ d) → acc + ds.sum < threshold →
      (ds.scanl (· + ·) acc).all (fun v => v < threshold) := by
    intro ds
    induction ds with
    | nil =>
        intro acc hds hacc
        simpa using hacc
    | cons d ds ih =>
        intro acc hds hacc
        have hd : 0 ≤ d := hds d (by simp)
        have hds' : ∀ z ∈ ds, 0 ≤ z := by
          intro z hz
          exact hds z (by simp [hz])
        have hsum : acc + (d + ds.sum) < threshold := by
          simpa only [List.sum_cons] using hacc
        have hdsSum : 0 ≤ ds.sum := hsumNonneg ds hds'
        have hacc' : acc + d + ds.sum < threshold := by omega
        have hstep : acc + d < threshold := by omega
        have hhead : acc < threshold := by omega
        have htail := ih (acc + d) hds' hacc'
        simpa [List.scanl_cons, hhead, hstep, htail]
  have hfull := hscan ds b hnonneg hfinal
  simp at hfull
  simp [TinylambOpt.ResearchGoals.JitKernels.DeltaLaneScan.values]
  intro x hx
  apply hfull x
  cases hsl : ds.scanl (fun acc d => acc + d) b with
  | nil => simp [TinylambOpt.ResearchGoals.JitKernels.DeltaLaneScan.values, hsl] at hx
  | cons y ys =>
      simp [TinylambOpt.ResearchGoals.JitKernels.DeltaLaneScan.values, hsl] at hx ⊢
      exact Or.inr hx

end TinylambOpt.ResearchProofs.JitKernels.Goal_DeltaLaneScan_EndpointDeltaRangeFence
