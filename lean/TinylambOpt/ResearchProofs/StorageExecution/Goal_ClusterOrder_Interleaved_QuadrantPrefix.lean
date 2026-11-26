import TinylambOpt.ResearchGoals.StorageExecution.ClusterOrder

/-!
Ordered SQL scans and limits depend on preserving key order and ties across physical
clusters.

Proved

- `Interleaved.QuadrantPrefix` — `∀ x y h, (key (x,y)).1/4^h = morton (x/2^h) (y/2^h)`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_ClusterOrder_Interleaved_QuadrantPrefix

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.ClusterOrder
open TinylambOpt.ResearchGoals.StorageExecution.ClusterOrder.Interleaved

private def term (x y i : Nat) : Nat :=
  (if x.testBit i then 2 ^ (2 * i) else 0) + (if y.testBit i then 2 ^ (2 * i + 1) else 0)

private def M (k x y : Nat) : Nat := ((List.range k).map (term x y)).sum

private theorem morton_eq_M (x y : Nat) :
    morton x y = M (max (width x) (width y)) x y := by
  rfl

private theorem M_succ (k x y : Nat) : M (k + 1) x y = M k x y + term x y k := by
  simp [M, List.range_succ]

private theorem term_zero_of_lt {x y i : Nat} (hx : x < 2 ^ i) (hy : y < 2 ^ i) :
    term x y i = 0 := by
  simp [term, Nat.testBit_lt_two_pow hx, Nat.testBit_lt_two_pow hy]

private theorem term_shift (h i x y : Nat) :
    term x y (h + i) = 2 ^ (2 * h) * term (x / 2 ^ h) (y / 2 ^ h) i := by
  have hbx : x.testBit (h + i) = (x / 2 ^ h).testBit i := by
    rw [Nat.add_comm h i]
    exact (Nat.testBit_div_two_pow (n := h) x i).symm
  have hby : y.testBit (h + i) = (y / 2 ^ h).testBit i := by
    rw [Nat.add_comm h i]
    exact (Nat.testBit_div_two_pow (n := h) y i).symm
  unfold term
  rw [hbx, hby]
  have e1 : 2 * (h + i) = 2 * h + 2 * i := by omega
  have e2 : 2 * (h + i) + 1 = 2 * h + (2 * i + 1) := by omega
  rw [e2, e1, Nat.pow_add, Nat.pow_add]
  by_cases hx : (x / 2 ^ h).testBit i <;> by_cases hy : (y / 2 ^ h).testBit i <;>
    simp [hx, hy, Nat.mul_add]

private theorem M_stable {k k' x y : Nat} (hx : x < 2 ^ k) (hy : y < 2 ^ k)
    (hkk : k ≤ k') : M k' x y = M k x y := by
  induction hkk with
  | refl => rfl
  | @step k' hkk ih =>
      rw [M_succ]
      have hpow : 2 ^ k ≤ 2 ^ k' := Nat.pow_le_pow_right (by omega) hkk
      have hx' : x < 2 ^ k' := Nat.lt_of_lt_of_le hx hpow
      have hy' : y < 2 ^ k' := Nat.lt_of_lt_of_le hy hpow
      rw [term_zero_of_lt hx' hy']
      rw [ih]
      simp

private theorem lt_pow_of_width (n : Nat) : n < 2 ^ width n := by
  by_cases hn : n = 0
  · simp [width, hn]
  · simpa [width, hn] using (Nat.lt_log2_self (n := n))

private theorem width_le_of_lt {n k : Nat} (h : n < 2 ^ k) : width n ≤ k := by
  by_cases hn : n = 0
  · simp [width, hn]
  · have hw : width n = n.log2 + 1 := by simp [width, hn]
    rw [hw]
    have hlog : n.log2 < k := by
      by_cases hle : k ≤ n.log2
      · have hp : 2 ^ k ≤ n := (Nat.le_log2 hn).mp hle
        omega
      · exact Nat.lt_of_not_ge hle
    omega

private theorem morton_eq_M_of_bound {x y k : Nat} (hx : x < 2 ^ k) (hy : y < 2 ^ k) :
    morton x y = M k x y := by
  have hxw : width x ≤ k := width_le_of_lt hx
  have hyw : width y ≤ k := width_le_of_lt hy
  have hm : max (width x) (width y) ≤ k := (Nat.max_le).2 ⟨hxw, hyw⟩
  have hp1 : 2 ^ width x ≤ 2 ^ max (width x) (width y) :=
    Nat.pow_le_pow_right (by omega) (Nat.le_max_left (width x) (width y))
  have hp2 : 2 ^ width y ≤ 2 ^ max (width x) (width y) :=
    Nat.pow_le_pow_right (by omega) (Nat.le_max_right (width x) (width y))
  have hs : M k x y = M (max (width x) (width y)) x y :=
    M_stable (Nat.lt_of_lt_of_le (lt_pow_of_width x) hp1)
      (Nat.lt_of_lt_of_le (lt_pow_of_width y) hp2) hm
  rw [morton_eq_M]
  exact hs.symm

private theorem M_split (k h x y : Nat) :
    M (h + k) x y = M h x y + 2 ^ (2 * h) * M k (x / 2 ^ h) (y / 2 ^ h) := by
  induction k generalizing h with
  | zero => simp [M]
  | succ j ih =>
      rw [show h + (j + 1) = (h + j) + 1 by omega, M_succ]
      rw [ih h]
      rw [term_shift]
      rw [M_succ (j) (x / 2 ^ h) (y / 2 ^ h)]
      rw [Nat.mul_add]
      simp [Nat.add_assoc]

private theorem term_le (x y i : Nat) : term x y i ≤ 3 * 2 ^ (2 * i) := by
  have hpow : 2 ^ (2 * i + 1) = 2 * 2 ^ (2 * i) := by
    rw [show 2 * i + 1 = (2 * i).succ by omega, Nat.pow_succ']
  unfold term
  by_cases hx : x.testBit i <;> by_cases hy : y.testBit i <;> simp [hx, hy, hpow] <;> omega

private theorem M_lt_pow (k x y : Nat) : M k x y < 2 ^ (2 * k) := by
  induction k with
  | zero => simp [M]
  | succ j ih =>
      rw [M_succ]
      have ht := term_le x y j
      have he : 2 ^ (2 * (j + 1)) = 4 * 2 ^ (2 * j) := by
        rw [show 2 * (j + 1) = 2 * j + 2 by omega, Nat.pow_add]
        rw [show 2 ^ 2 = 4 by rfl, Nat.mul_comm]
      rw [he]
      omega

private theorem four_pow (h : Nat) : 4 ^ h = 2 ^ (2 * h) := by
  rw [show 4 = 2 ^ 2 by rfl, Nat.pow_mul]

theorem sound_QuadrantPrefix : QuadrantPrefix.Goal := by
  intro x y h
  change morton x y / 4 ^ h = morton (x / 2 ^ h) (y / 2 ^ h)
  let k0 := max (width x) (width y)
  let K := k0 + h
  have hKpow : 2 ^ k0 ≤ 2 ^ K := Nat.pow_le_pow_right (by omega) (by dsimp [K]; omega)
  have hx0 : x < 2 ^ k0 := by
    dsimp [k0]
    exact Nat.lt_of_lt_of_le (lt_pow_of_width x)
      (Nat.pow_le_pow_right (by omega) (Nat.le_max_left (width x) (width y)))
  have hy0 : y < 2 ^ k0 := by
    dsimp [k0]
    exact Nat.lt_of_lt_of_le (lt_pow_of_width y)
      (Nat.pow_le_pow_right (by omega) (Nat.le_max_right (width x) (width y)))
  have hxK : x < 2 ^ K := Nat.lt_of_lt_of_le hx0 hKpow
  have hyK : y < 2 ^ K := Nat.lt_of_lt_of_le hy0 hKpow
  have hmx : morton x y = M K x y := morton_eq_M_of_bound hxK hyK
  have hxk0' : x / 2 ^ h < 2 ^ k0 := Nat.lt_of_le_of_lt (Nat.div_le_self x (2 ^ h)) hx0
  have hyk0' : y / 2 ^ h < 2 ^ k0 := Nat.lt_of_le_of_lt (Nat.div_le_self y (2 ^ h)) hy0
  have hmx' : morton (x / 2 ^ h) (y / 2 ^ h) = M k0 (x / 2 ^ h) (y / 2 ^ h) :=
    morton_eq_M_of_bound hxk0' hyk0'
  rw [hmx, hmx', four_pow h]
  have hs : M K x y = M h x y + 2 ^ (2 * h) * M k0 (x / 2 ^ h) (y / 2 ^ h) := by
    dsimp [K]
    rw [Nat.add_comm k0 h]
    exact M_split k0 h x y
  rw [hs]
  rw [Nat.mul_comm (2 ^ (2 * h)) (M k0 (x / 2 ^ h) (y / 2 ^ h))]
  rw [Nat.add_mul_div_right _ _ (Nat.pow_pos (by omega))]
  rw [Nat.div_eq_of_lt (M_lt_pow h x y)]
  simp

end TinylambOpt.ResearchProofs.StorageExecution.Goal_ClusterOrder_Interleaved_QuadrantPrefix
