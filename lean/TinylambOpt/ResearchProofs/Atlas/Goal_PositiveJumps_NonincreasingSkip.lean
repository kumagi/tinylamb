import TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

/-!
SQL execution can use cumulative counts or ordered measures to answer range and rank
queries under the modeled bounds.

Proved

- `PositiveJumps.NonincreasingSkip` — `∀ x, x.Pairwise (· ≥ ·) → measure x = 0`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_PositiveJumps_NonincreasingSkip

open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

private theorem delta_cons₂ (a b : Int) (xs : Series) :
    delta (a :: b :: xs) = (b - a) :: delta (b :: xs) := by
  simp [delta, adj]

private theorem sum_max0_eq_zero {l : List Int} (h : ∀ v ∈ l, v ≤ 0) :
    (l.map (max 0)).sum = 0 := by
  induction l with
  | nil => simp
  | cons v vs ih =>
      have hv : v ≤ 0 := h v (by simp)
      have hvs : ∀ w ∈ vs, w ≤ 0 := by
        intro w hw
        exact h w (by simp [hw])
      have hm : max 0 v = 0 := by omega
      simp [hm, ih hvs]

private theorem delta_nonpos_of_pairwise : ∀ x : Series,
    x.Pairwise (fun a b : Int => a ≥ b) → ∀ v ∈ delta x, v ≤ 0 := by
  intro x
  induction x with
  | nil =>
      intro hx v hv
      simp [delta, adj] at hv
  | cons a xs ih =>
      intro hx
      cases xs with
      | nil =>
          intro v hv
          simp [delta, adj] at hv
      | cons b ys =>
          cases hx with
          | cons hhead htail =>
              have hba : b ≤ a := hhead b (by simp)
              have hrec := ih htail
              intro v hv
              rw [delta_cons₂] at hv
              simp only [List.mem_cons] at hv
              rcases hv with hv | hv
              · subst v
                omega
              · exact hrec v hv

theorem sound_PositiveJumps_NonincreasingSkip : PositiveJumps.NonincreasingSkip.Goal := by
  intro x hx
  unfold PositiveJumps.measure
  exact sum_max0_eq_zero (delta_nonpos_of_pairwise x hx)

end TinylambOpt.ResearchProofs.Atlas.Goal_PositiveJumps_NonincreasingSkip
