import TinylambOpt.ResearchGoals.Atlas.PlanSearch

/-!
The SQL optimizer can prune or reorganize its plan search only under the relevant
ordering and cost assumptions.

Proved

- `RiskEnvelope.RareWorkload` — `∀ xs extra z a b, (∀ y, loss xs z a b ≤ loss xs y a b)
  → ∀ y, loss (xs++extra) z a b ≤ loss (xs++extra) y a b + max a b * (z-y).natAbs *
  mass extra (fun _ => true)`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.PlanSearch_RareWorkload

open TinylambOpt.ResearchGoals.Atlas.PlanSearch
open TinylambOpt.ResearchGoals.Atlas.PlanSearch.RiskEnvelope

/-! The per-sample cost `a·(x-z)⁺ + b·(z-x)⁺` is `max a b`-Lipschitz in `z`.
Hence, at the old optimum `z`, the extra workload can only gain at most
`max a b * |z-y|` per unit of its weight when compared with any other `y`.
The base workload contributes no regret by the optimality hypothesis, and
`loss` is additive over list concatenation. -/

private theorem natAbs_pos_parts (z y : Int) : max (z - y) 0 + max (y - z) 0 = (z - y).natAbs := by
  have hneg : -(z - y) = y - z := by omega
  by_cases h : 0 ≤ z - y
  · have h' : y - z ≤ 0 := by omega
    rw [Int.max_eq_left h, Int.max_eq_right h']
    rw [Int.natAbs_of_nonneg h]
    omega
  · have hle : z - y ≤ 0 := by omega
    have h' : 0 ≤ y - z := by omega
    rw [Int.max_eq_right hle, Int.max_eq_left h']
    rw [← Int.natAbs_neg (z - y), hneg, Int.natAbs_of_nonneg h']
    omega

private theorem pos_part_triangle (d e : Int) : max d 0 ≤ max e 0 + max (d - e) 0 := by
  rw [Int.max_le]
  constructor
  · have h1 := Int.le_max_left e 0
    have h2 := Int.le_max_left (d - e) 0
    omega
  · have h1 : (0 : Int) ≤ max e 0 := Int.le_max_right e 0
    have h2 : (0 : Int) ≤ max (d - e) 0 := Int.le_max_right (d - e) 0
    omega

private theorem point_lip_int (x z y : Int) (A B M : Int)
    (hA : 0 ≤ A) (hB : 0 ≤ B) (hMA : A ≤ M) (hMB : B ≤ M) :
    A * max (x - z) 0 + B * max (z - x) 0 ≤
      A * max (x - y) 0 + B * max (y - x) 0 + M * (z - y).natAbs := by
  have htri1 : max (x - z) 0 ≤ max (x - y) 0 + max (y - z) 0 := by
    have h := pos_part_triangle (x - z) (x - y)
    rw [show x - z - (x - y) = y - z by omega] at h
    exact h
  have htri2 : max (z - x) 0 ≤ max (y - x) 0 + max (z - y) 0 := by
    have h := pos_part_triangle (z - x) (y - x)
    rw [show z - x - (y - x) = z - y by omega] at h
    exact h
  have hm1 : A * max (x - z) 0 ≤ A * (max (x - y) 0 + max (y - z) 0) :=
    Int.mul_le_mul_of_nonneg_left htri1 hA
  have hm2 : B * max (z - x) 0 ≤ B * (max (y - x) 0 + max (z - y) 0) :=
    Int.mul_le_mul_of_nonneg_left htri2 hB
  have hM1 : A * max (y - z) 0 ≤ M * max (y - z) 0 :=
    Int.mul_le_mul_of_nonneg_right hMA (Int.le_max_right (y - z) 0)
  have hM2 : B * max (z - y) 0 ≤ M * max (z - y) 0 :=
    Int.mul_le_mul_of_nonneg_right hMB (Int.le_max_right (z - y) 0)
  have hsum := Int.add_le_add hm1 hm2
  rw [← natAbs_pos_parts z y, Int.mul_add]
  rw [Int.mul_add, Int.mul_add] at hsum
  have hnonneg1 : (0 : Int) ≤ max (y - z) 0 := Int.le_max_right (y - z) 0
  have hnonneg2 : (0 : Int) ≤ max (z - y) 0 := Int.le_max_right (z - y) 0
  omega

private theorem point_lip_nat (x z y : Int) (a b : Nat) :
    a * (x - z).toNat + b * (z - x).toNat ≤
      a * (x - y).toNat + b * (y - x).toNat + max a b * (z - y).natAbs := by
  have hint : ((a * (x - z).toNat + b * (z - x).toNat : Nat) : Int) ≤
      ((a * (x - y).toNat + b * (y - x).toNat + max a b * (z - y).natAbs : Nat) : Int) := by
    push_cast
    rw [Int.toNat_eq_max, Int.toNat_eq_max, Int.toNat_eq_max, Int.toNat_eq_max]
    exact point_lip_int x z y (a : Int) (b : Int) ((max a b : Nat) : Int)
      (by omega) (by omega) (by exact_mod_cast Nat.le_max_left a b) (by exact_mod_cast Nat.le_max_right a b)
  exact_mod_cast hint

private theorem entry_lip (x z y : Int) (w a b : Nat) :
    w * (a * (x - z).toNat + b * (z - x).toNat) ≤
      w * (a * (x - y).toNat + b * (y - x).toNat) +
        (max a b * (z - y).natAbs) * w := by
  have hp := point_lip_nat x z y a b
  have hm := Nat.mul_le_mul_left w hp
  simpa [Nat.mul_add, Nat.add_mul, Nat.mul_assoc, Nat.mul_comm, Nat.mul_left_comm] using hm

private theorem mass_all (xs : Costs) : mass xs (fun _ => true) = (xs.map Prod.snd).sum := by
  unfold mass
  have hf : List.filter (fun x : Int × Nat => true) xs = xs := by
    apply (List.filter_eq_self).2
    intro a ha
    rfl
  rw [hf]

private theorem extra_lip (extra : Costs) (z y : Int) (a b : Nat) :
    loss extra z a b ≤ loss extra y a b + max a b * (z - y).natAbs * mass extra (fun _ => true) := by
  induction extra with
  | nil => simp [loss, mass]
  | cons e es ih =>
      rcases e with ⟨x, w⟩
      rw [mass_all] at ih ⊢
      change w * (a * (x - z).toNat + b * (z - x).toNat) + loss es z a b ≤
        w * (a * (x - y).toNat + b * (y - x).toNat) + loss es y a b +
          max a b * (z - y).natAbs * (List.map Prod.snd ((x, w) :: es)).sum
      simp only [List.map_cons, List.sum_cons]
      have hK : max a b * (z - y).natAbs * (w + (List.map Prod.snd es).sum) =
          max a b * (z - y).natAbs * w + max a b * (z - y).natAbs * (List.map Prod.snd es).sum :=
        Nat.mul_add _ _ _
      rw [hK]
      have he := entry_lip x z y w a b
      omega

theorem sound_RareWorkload : RiskEnvelope.RareWorkload.Goal := by
  intro xs extra z a b hopt y
  have hex := extra_lip extra z y a b
  have happz : loss (xs ++ extra) z a b = loss xs z a b + loss extra z a b := by
    simp [loss]
  have happy : loss (xs ++ extra) y a b = loss xs y a b + loss extra y a b := by
    simp [loss]
  rw [happz, happy]
  have hbase := hopt y
  omega

end TinylambOpt.ResearchProofs.Atlas.PlanSearch_RareWorkload
