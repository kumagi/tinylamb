import TinylambOpt.ResearchGoals.JitFrontiers.WordPromises
import TinylambOpt.ResearchProofs.JitFrontiers.WordPromises.BV64

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitFrontiers.WordPromises.Goal_DisjointOr_HoistAcrossPhi

open TinylambOpt.ResearchGoals.JitFrontiers.WordPromises
open TinylambOpt.ResearchGoals.JitFrontiers.WordPromises.DisjointOr

private theorem signed_not_overflow (n : Int) :
    signed n = !((n ≥ (2^63 : Int)) || (n < -(2^63 : Int))) := by
  unfold signed
  generalize hA : decide (-(2^63 : Int) ≤ n) = A
  generalize hB : decide (n < (2^63 : Int)) = B
  generalize hC : decide (n ≥ (2^63 : Int)) = C
  generalize hD : decide (n < -(2^63 : Int)) = D
  cases A <;> cases B <;> cases C <;> cases D <;> simp_all <;> omega

private theorem floorMask_toNat (y : W) (hy : y.toNat < 64) :
    (floorMask y).toNat = 2 ^ y.toNat - 1 := by
  have hpow : 2 ^ y.toNat < 2^64 := Nat.pow_lt_pow_right (by omega) hy
  have hpowpos : 1 ≤ 2 ^ y.toNat := Nat.one_le_two_pow
  unfold floorMask
  rw [BitVec.toNat_sub]
  change (2^64 - 1 + (↑(1 <<< y.toNat) : W).toNat) % 2^64 = _
  have hof : (↑(1 <<< y.toNat) : W).toNat = 2 ^ y.toNat := by
    rw [Nat.shiftLeft_eq]
    simp [Nat.mod_eq_of_lt hpow]
  rw [hof]
  rw [show 2^64 - 1 + 2 ^ y.toNat = 2^64 + (2 ^ y.toNat - 1) by omega]
  rw [Nat.add_mod_left, Nat.mod_eq_of_lt (by omega)]

theorem dropFlag_of_guardExact (o : Op) (h : guardExact o) : dropFlag o := by
  intro p hp
  have hd : o.defined p.1 p.2 = true := by
    simpa [passes, ← h p.1 p.2] using hp
  simp [ir, hd]

theorem speculateSelect_of_guardExact (o : Op) (h : guardExact o) : speculateSelect o := by
  intro active p defval hac
  by_cases ha : active = true
  · subst active
    have hp : passes o p := hac rfl
    have hd := dropFlag_of_guardExact o h p hp
    simp [takeBranch, hd]
  · have hf : active = false := by cases active <;> simp_all
    subst active
    simp [takeBranch]

theorem ir_isNone_of_guardExact (o : Op) (h : guardExact o) (p : Args) :
    (ir o p).isNone = !(passes o p) := by
  unfold ir passes
  rw [← h p.1 p.2]
  cases o.test p.1 p.2 <;> rfl

theorem ir_none_iff_not_passes (o : Op) (h : guardExact o) (p : Args) :
    ir o p = none ↔ passes o p = false := by
  unfold ir passes
  rw [← h p.1 p.2]
  cases o.test p.1 p.2 <;> simp

private theorem findIdx_congr {α : Type} (p q : α → Bool) :
    ∀ (L : List α), (∀ x ∈ L, p x = q x) → L.findIdx p = L.findIdx q := by
  intro L
  induction L with
  | nil => intro _; rfl
  | cons a t ih =>
    intro h
    simp only [List.findIdx_cons]
    rw [h a (by simp)]
    rw [ih (fun x hx => h x (by simp [hx]))]

theorem firstPoison_of_guardExact (o : Op) (h : guardExact o) : firstPoison o := by
  intro ps live
  apply findIdx_congr
  intro e he
  rcases e with ⟨p, i⟩
  simp only [ir_isNone_of_guardExact o h p]

theorem hoistAcrossPhi_of_guardExact (o : Op) (h : guardExact o) : hoistAcrossPhi o := by
  intro pick a b hp
  by_cases hk : pick = true
  · subst pick
    simp [takeBranch] at hp ⊢
    exact dropFlag_of_guardExact o h a hp
  · have hkf : pick = false := by cases pick <;> simp_all
    subst pick
    simp [takeBranch] at hp ⊢
    exact dropFlag_of_guardExact o h b hp

theorem freezeInvariant_of_guardExact (o : Op) (h : guardExact o) : freezeInvariant o := by
  intro p
  by_cases hpass : passes o p = true
  · have hsome : ∃ w, ir o p = some w := by
      cases hir : ir o p with
      | none =>
        have hn := (ir_none_iff_not_passes o h p).mp hir
        simp [hpass] at hn
      | some w => exact ⟨w, rfl⟩
    rcases hsome with ⟨w, hw⟩
    constructor
    · intro _
      exact hpass
    · intro _ a b
      simp [freeze, hw]
  · have hnone : ir o p = none := by
      cases hir : ir o p with
      | none => exact rfl
      | some w =>
        exfalso
        unfold ir at hir
        cases hd : o.defined p.1 p.2 <;> simp [hd] at hir
        have ht : o.test p.1 p.2 = true := by simpa [← h p.1 p.2] using hd
        exact hpass (by simpa [passes] using ht)
    constructor
    · intro hall
      have h01 := hall (0 : W) (1 : W)
      simp [freeze, hnone] at h01
    · intro hp
      exact (hpass hp).elim

theorem deadInputs_of_guardExact (o : Op) (h : guardExact o) : deadInputs o := by
  intro a b ids
  induction ids with
  | nil => intro _ _; rfl
  | cons i t ih =>
    intro hab hpass
    simp only [List.map_cons]
    have hi : a i = b i := hab i (by simp)
    have hp : passes o (a i) := hpass i (by simp)
    have hd := dropFlag_of_guardExact o h (a i) hp
    rw [hi] at hd
    rw [ih (fun j hj => hab j (by simp [hj])) (fun j hj => hpass j (by simp [hj]))]
    rw [hi]
    rw [hd]


private theorem hguard : DisjointOr.GuardExact.Goal :=
  fun x y => BV64.and_eq_zero_iff_beq (x := x) (y := y)

theorem sound : DisjointOr.HoistAcrossPhi.Goal := hoistAcrossPhi_of_guardExact DisjointOr.op hguard

end TinylambOpt.ResearchProofs.JitFrontiers.WordPromises.Goal_DisjointOr_HoistAcrossPhi
