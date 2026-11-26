import TinylambOpt.ResearchGoals.StorageExecution.DerivedZones

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_BucketSum_SumCeiling

open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones
open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.BucketSum

-- 真偽確認: t, 各端点, x, y < 4 の全組み合わせを native_decide で網羅し true を確認済み。

/-- Division by a positive constant is monotone, so every non-NULL score of a
    covered page lies between the independently derived bucket endpoints. -/
private theorem bucketsum_score_bounds {t : Nat} {p : List (Option Row)} {a b : Bounds}
    (hc : covers p a b) {v : Nat} (hv : v ∈ scores kernel t p) :
    (a.lo + b.lo) / (t + 1) ≤ v ∧ v ≤ (a.hi + b.hi) / (t + 1) := by
  rcases List.mem_filterMap.mp hv with ⟨r, hrp, hmap⟩
  rcases Option.map_eq_some_iff.mp hmap with ⟨xy, hrxy, hscore⟩
  rcases xy with ⟨x, y⟩
  have hcovxy := hc r hrp x y hrxy
  simp only [inside] at hcovxy
  rw [← hscore]
  dsimp [kernel]
  constructor
  · exact Nat.div_le_div_right (by omega)
  · exact Nat.div_le_div_right (by omega)

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

theorem sound_SumCeiling : BucketSum.SumCeiling.Goal := by
  intro t p a b hc
  change (scores kernel t p).length * ((a.lo + b.lo) / (t + 1)) ≤ (scores kernel t p).sum ∧
    (scores kernel t p).sum ≤ (scores kernel t p).length * ((a.hi + b.hi) / (t + 1))
  exact sum_bounds (fun v hv => bucketsum_score_bounds (t := t) hc hv)

end TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_BucketSum_SumCeiling
