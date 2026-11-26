import TinylambOpt.ResearchGoals.StorageExecution.DerivedZones

/-!
Scan pruning can exploit derived bounds only when every matching row remains covered by
the block summary.

Proved

- `Product.SumCeiling` — A footer gives a deterministic interval for the SUM
  accumulator over non-NULL scores.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_Product_SumCeiling

open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones
open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.Product

/-- Multiplication of naturals is monotone, so every non-NULL score of a
    covered page lies between the independently derived corner products. -/
private theorem product_score_bounds {t : Nat} {p : List (Option Row)} {a b : Bounds}
    (hc : covers p a b) {v : Nat} (hv : v ∈ scores kernel t p) :
    a.lo * b.lo ≤ v ∧ v ≤ a.hi * b.hi := by
  rcases List.mem_filterMap.mp hv with ⟨r, hrp, hmap⟩
  rcases Option.map_eq_some_iff.mp hmap with ⟨xy, hrxy, hscore⟩
  rcases xy with ⟨x, y⟩
  have hcovxy := hc r hrp x y hrxy
  simp only [inside] at hcovxy
  rw [← hscore]
  dsimp [kernel]
  exact ⟨Nat.mul_le_mul hcovxy.1.1 hcovxy.2.1,
    Nat.mul_le_mul hcovxy.1.2 hcovxy.2.2⟩

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

theorem sound_SumCeiling : Product.SumCeiling.Goal := by
  intro t p a b hc
  change (scores kernel t p).length * (a.lo * b.lo) ≤ (scores kernel t p).sum ∧
    (scores kernel t p).sum ≤ (scores kernel t p).length * (a.hi * b.hi)
  exact sum_bounds (fun v hv => product_score_bounds (t := t) hc hv)

end TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_Product_SumCeiling
