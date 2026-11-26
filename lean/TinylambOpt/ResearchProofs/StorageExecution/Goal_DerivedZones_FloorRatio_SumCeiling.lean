import TinylambOpt.ResearchGoals.StorageExecution.DerivedZones

/-!
Scan pruning can exploit derived bounds only when every matching row remains covered by
the block summary.

Proved

- `FloorRatio.SumCeiling` — A footer gives a deterministic interval for the SUM
  accumulator over non-NULL scores.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_FloorRatio_SumCeiling

open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones
open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.FloorRatio

-- 真偽確認: t ∈ {0,3}, 小さなページ/端点について native_decide で true を確認済み。

/-- Integer division by a positive denominator is monotone in the numerator and
    antitone in the denominator, so every non-NULL score of a covered page lies
    between the independently derived floor-ratio endpoints (SoundInterval). -/
private theorem floorratio_score_bounds {t : Nat} {p : List (Option Row)} {a b : Bounds}
    (hc : covers p a b) {v : Nat} (hv : v ∈ scores kernel t p) :
    (a.lo * t) / (b.hi + 1) ≤ v ∧ v ≤ (a.hi * t) / (b.lo + 1) := by
  rcases List.mem_filterMap.mp hv with ⟨r, hrp, hmap⟩
  rcases Option.map_eq_some_iff.mp hmap with ⟨xy, hrxy, hscore⟩
  rcases xy with ⟨x, y⟩
  have hcovxy := hc r hrp x y hrxy
  simp only [inside] at hcovxy
  rw [← hscore]
  dsimp [kernel]
  constructor
  · exact Nat.div_le_div (Nat.mul_le_mul hcovxy.1.1 (Nat.le_refl t)) (by omega) (by omega)
  · exact Nat.div_le_div (Nat.mul_le_mul hcovxy.1.2 (Nat.le_refl t)) (by omega) (by omega)

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

theorem sound_SumCeiling : FloorRatio.SumCeiling.Goal := by
  intro t p a b hc
  change (scores kernel t p).length * ((a.lo * t) / (b.hi + 1)) ≤ (scores kernel t p).sum ∧
    (scores kernel t p).sum ≤ (scores kernel t p).length * ((a.hi * t) / (b.lo + 1))
  exact sum_bounds (fun v hv => floorratio_score_bounds (t := t) hc hv)

end TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_FloorRatio_SumCeiling
