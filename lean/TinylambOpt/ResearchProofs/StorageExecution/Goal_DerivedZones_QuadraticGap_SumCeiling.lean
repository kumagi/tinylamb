import TinylambOpt.ResearchGoals.StorageExecution.DerivedZones

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_QuadraticGap_SumCeiling

open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones
open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.QuadraticGap

-- 真偽確認: 端点・x, y, t を小さい範囲で全網羅し、inside を満たす組について
-- gapMin ≤ gap ≤ gapMax および score が bounds 内であることを #eval で確認済み。
-- よって各スコアが区間内に収まり、和も len*lo .. len*hi に収まる（真）。

/-- For a covered row, the absolute gap lies between the independently derived
    minimum and maximum endpoint separations. -/
private theorem gap_bounds {a b : Bounds} {x y : Nat}
    (hx : inside a x) (hy : inside b y) :
    gapMin a b ≤ gap x y ∧ gap x y ≤ gapMax a b := by
  simp only [inside] at hx hy
  simp only [gapMin, gapMax, gap]
  constructor <;> omega

/-- Squaring and adding the threshold preserve the interval bounds. -/
private theorem quadratic_score_bounds {t : Nat} {p : List (Option Row)} {a b : Bounds}
    (hc : covers p a b) {v : Nat} (hv : v ∈ scores kernel t p) :
    (kernel.bounds t a b).lo ≤ v ∧ v ≤ (kernel.bounds t a b).hi := by
  rcases List.mem_filterMap.mp hv with ⟨r, hrp, hmap⟩
  rcases Option.map_eq_some_iff.mp hmap with ⟨xy, hrxy, hscore⟩
  rcases xy with ⟨x, y⟩
  have hcovxy := hc r hrp x y hrxy
  simp only [inside] at hcovxy
  rw [← hscore]
  have hg := gap_bounds hcovxy.1 hcovxy.2
  dsimp [kernel]
  constructor
  · exact Nat.add_le_add_right (Nat.pow_le_pow_left hg.1 2) t
  · exact Nat.add_le_add_right (Nat.pow_le_pow_left hg.2 2) t

/-- A list of n values, each between lo and hi, has its sum between n*lo
    and n*hi. -/
private theorem sum_bounds {lo hi : Nat} {vs : List Nat}
    (hv : ∀ v ∈ vs, lo ≤ v ∧ v ≤ hi) :
    vs.length * lo ≤ vs.sum ∧ vs.sum ≤ vs.length * hi := by
  induction vs with
  | nil => simp
  | cons v vs ih =>
    have hhead := hv v (by simp)
    have htail := ih (fun x hx => hv x (by simp [hx]))
    simp only [List.length_cons, List.sum_cons, Nat.add_mul]
    omega

theorem sound_SumCeiling : QuadraticGap.SumCeiling.Goal := by
  intro t p a b hc
  exact sum_bounds (fun v hv => quadratic_score_bounds (t := t) hc hv)

end TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_QuadraticGap_SumCeiling
