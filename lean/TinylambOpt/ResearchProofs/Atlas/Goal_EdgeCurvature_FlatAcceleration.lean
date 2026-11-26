import TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

/-!
SQL execution can use cumulative counts or ordered measures to answer range and rank
queries under the modeled bounds.

Proved

- `EdgeCurvature.FlatAcceleration` — `∀ x a, (∀ v ∈ delta (delta x), v = a) → measure x
  = ((x.length-2:Nat):Int)*a^2`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_EdgeCurvature_FlatAcceleration

open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

/-!
`EdgeCurvature.FlatAcceleration.Goal` は、二階差分が定数 `a` の列について、
二乗エネルギーが `(長さ - 2) * a^2` で与えられることを述べる。

真偽確認: 等差列 `[0,1,2,3]`, `[0,2,4,6,8]`, `[1,3,5]`, `[5,5,5,5]` の
`EdgeCurvature.measure` はすべて 0（端点確認済み）。

証明の要点: `measure x` は `((delta (delta x)).map (·^2)).sum` なので、
仮定「全二階差分が a」から各項が `a^2` に等しいことを使って和を
`長さ(delta (delta x)) * a^2` に置き換え、`delta` の長さが毎回 1 減ることを
示して `x.length - 2` に一致させる。
-/

private theorem delta_length (x : Series) : (delta x).length = x.length - 1 := by
  induction x with
  | nil => simp [delta, adj]
  | cons a xs ih =>
      cases xs with
      | nil => simp [delta, adj]
      | cons b bs =>
          simp [delta, adj]

private theorem sum_sq_eq_of_all_eq (l : Series) (a : Int)
    (h : ∀ v ∈ l, v = a) :
    (l.map (fun v => v ^ 2)).sum = (l.length : Int) * a ^ 2 := by
  induction l with
  | nil => simp
  | cons v vs ih =>
      have hv : v = a := h v (by simp)
      have ht : ∀ w ∈ vs, w = a := by
        intro w hw
        exact h w (by simp [hw])
      simp only [List.map_cons, List.sum_cons, List.length_cons]
      rw [hv, ih ht]
      have hcast : ((vs.length + 1 : Nat) : Int) = (vs.length : Int) + 1 := by omega
      rw [hcast, Int.add_mul]
      simp
      omega

theorem sound_EdgeCurvature_FlatAcceleration : EdgeCurvature.FlatAcceleration.Goal := by
  intro x a h
  unfold EdgeCurvature.measure QuadraticVariation.measure
  let l : Series := delta (delta x)
  have hs : (l.map (fun v => v ^ 2)).sum = (l.length : Int) * a ^ 2 :=
    sum_sq_eq_of_all_eq l a (by simpa [l] using h)
  rw [hs]
  have hlen : l.length = x.length - 2 := by
    dsimp [l]
    rw [delta_length, delta_length]
    omega
  rw [hlen]

end TinylambOpt.ResearchProofs.Atlas.Goal_EdgeCurvature_FlatAcceleration
