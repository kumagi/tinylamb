import TinylambOpt.ResearchGoals.JitKernels.DeltaLaneScan

/-!
With nonnegative deltas every decoded row is at most the final endpoint, so
checking `b + ds.sum < threshold` once certifies
`(values b ds).all (fun v => v < threshold)`. Zone-map style fence checks can
therefore examine only the run endpoint when all encoded deltas are
nonnegative, skipping the per-row comparison.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.JitKernels.DeltaLaneScan_EndpointDeltaRangeFence
open TinylambOpt.ResearchGoals.JitKernels.DeltaLaneScan

private theorem scanl_head (acc : Int) (l : List Int) :
    l.scanl (·+·) acc = acc :: (l.scanl (·+·) acc).drop 1 := by
  induction l with
  | nil => rfl
  | cons d t ih => rw [List.scanl_cons]; rfl

private theorem values_cons (b d : Int) (t : List Int) :
    values b (d::t) = (b+d) :: values (b+d) t := by
  unfold values
  rw [List.scanl_cons]
  rw [show List.drop 1 (b :: List.scanl (·+·) (b+d) t) = List.scanl (·+·) (b+d) t from rfl,
    ← scanl_head]

private theorem sum_nonneg (l : List Int) : (∀ x ∈ l, 0 ≤ x) → 0 ≤ l.sum := by
  induction l with
  | nil => intro _; exact Int.le_refl 0
  | cons a t ih =>
      intro h
      rw [List.sum_cons]
      exact Int.add_nonneg (h a (by simp)) (ih fun x hx => h x (by simp [hx]))

private theorem values_bound (ds : List Int) (b : Int) :
    (∀ d ∈ ds, 0 ≤ d) → ∀ y ∈ values b ds, y ≤ b + ds.sum := by
  induction ds generalizing b with
  | nil => intro _ y hy; simp [values] at hy
  | cons d t ih =>
      intro h y hy
      rw [values_cons] at hy
      have h0 : 0 ≤ (t : List Int).sum := sum_nonneg t fun x hx => h x (by simp [hx])
      rcases List.mem_cons.mp hy with rfl | hy'
      · rw [List.sum_cons]
        omega
      · have hle := ih (b + d) (fun x hx => h x (by simp [hx])) y hy'
        rw [List.sum_cons]
        omega

theorem sound : EndpointDeltaRangeFence.Goal := by
  intro b ds threshold h hlt
  rw [List.all_eq_true]
  intro y hy
  have hle := values_bound ds b h y hy
  have hyt : y < threshold := by omega
  simp [hyt]

end TinylambOpt.ResearchProofs.JitKernels.DeltaLaneScan_EndpointDeltaRangeFence
