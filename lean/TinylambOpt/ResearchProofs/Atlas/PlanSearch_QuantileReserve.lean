import TinylambOpt.ResearchGoals.Atlas.PlanSearch

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.PlanSearch_QuantileReserve

open TinylambOpt.ResearchGoals.Atlas.PlanSearch
open TinylambOpt.ResearchGoals.Atlas.PlanSearch.RiskEnvelope

private theorem mass_cons (x : Int) (w : Nat) (xs : Costs) (p : Int → Bool) :
    mass ((x,w)::xs) p = (if p x = true then w else 0) + mass xs p := by
  by_cases hp : p x = true <;> simp [mass, hp]

private theorem mass_pred_mono (xs : Costs) (p q : Int → Bool)
    (h : ∀ x, p x = true → q x = true) :
    mass xs p ≤ mass xs q := by
  induction xs with
  | nil => simp [mass]
  | cons hd tl ih =>
      rcases hd with ⟨x,w⟩
      rw [mass_cons, mass_cons]
      by_cases hp : p x = true
      · have hq : q x = true := h x hp
        simp [hp, hq]
        exact ih
      · simp [hp]
        by_cases hq : q x = true <;> simp [hq] <;> omega

private theorem mass_le_mono (xs : Costs) {s t : Int} (hst : s ≤ t) :
    mass xs (fun x => decide (x ≤ s)) ≤ mass xs (fun x => decide (x ≤ t)) := by
  apply mass_pred_mono
  intro x hx
  apply decide_eq_true_eq.mpr
  have hx' : x ≤ s := of_decide_eq_true hx
  omega

private theorem mass_le_of_lt (xs : Costs) {t z : Int} (htz : t < z) :
    mass xs (fun x => decide (x ≤ t)) ≤ mass xs (fun x => decide (x < z)) := by
  apply mass_pred_mono
  intro x hx
  apply decide_eq_true_eq.mpr
  have hx' : x ≤ t := of_decide_eq_true hx
  omega

private theorem mass_split (xs : Costs) (t : Int) :
    mass xs (fun x => decide (x ≤ t)) + mass xs (fun x => decide (t < x)) = mass xs (fun _ => true) := by
  induction xs with
  | nil => simp [mass]
  | cons hd tl ih =>
      rcases hd with ⟨x,w⟩
      by_cases h : x ≤ t
      · have h1 : decide (x ≤ t) = true := decide_eq_true_eq.mpr h
        have hgt : ¬ t < x := by omega
        have h2 : decide (t < x) = false := decide_eq_false_iff_not.mpr hgt
        rw [mass_cons, mass_cons, mass_cons]
        simp [h1, h2]
        omega
      · have h1 : decide (x ≤ t) = false := decide_eq_false_iff_not.mpr h
        have hgt : t < x := by omega
        have h2 : decide (t < x) = true := decide_eq_true_eq.mpr hgt
        rw [mass_cons, mass_cons, mass_cons]
        simp [h1, h2]
        omega

private theorem loss_cons (x : Int) (w : Nat) (xs : Costs) (z : Int) (a b : Nat) :
    loss ((x,w)::xs) z a b = w * (a * (x-z).toNat + b * (z-x).toNat) + loss xs z a b := by
  simp [loss]

private theorem head_le (x t : Int) (a b w : Nat)
    (h1 : (x - t).toNat = 0) (h2 : (x - (t+1)).toNat = 0)
    (h3 : ((t+1) - x).toNat = (t - x).toNat + 1) :
    w * (a * (x-(t+1)).toNat + b * ((t+1)-x).toNat) =
      w * (a * (x-t).toNat + b * (t-x).toNat) + b*w := by
  rw [h1, h2, h3]
  simp [Nat.mul_add, Nat.mul_comm, Nat.add_comm]

private theorem head_gt (x t : Int) (a b w : Nat)
    (h1 : (t - x).toNat = 0) (h2 : ((t+1) - x).toNat = 0)
    (h3 : (x - t).toNat = (x - (t+1)).toNat + 1) :
    w * (a * (x-(t+1)).toNat + b * ((t+1)-x).toNat) + a*w =
      w * (a * (x-t).toNat + b * (t-x).toNat) := by
  rw [h1, h2, h3]
  simp [Nat.mul_add, Nat.mul_comm, Nat.add_comm]

private theorem step_identity (xs : Costs) (t : Int) (a b : Nat) :
    loss xs (t+1) a b + a * mass xs (fun x => decide (t < x)) =
      loss xs t a b + b * mass xs (fun x => decide (x ≤ t)) := by
  induction xs with
  | nil => simp [loss, mass]
  | cons hd tl ih =>
      rcases hd with ⟨x,w⟩
      rw [loss_cons, loss_cons, mass_cons, mass_cons]
      by_cases h : x ≤ t
      · have hle : decide (x ≤ t) = true := decide_eq_true_eq.mpr h
        have hgt : ¬ t < x := by omega
        have hgtf : decide (t < x) = false := decide_eq_false_iff_not.mpr hgt
        simp [hle, hgtf]
        have h1 : (x - t).toNat = 0 := by omega
        have h2 : (x - (t+1)).toNat = 0 := by omega
        have h3 : ((t+1) - x).toNat = (t - x).toNat + 1 := by omega
        calc
          w * (a * (x-(t+1)).toNat + b*((t+1)-x).toNat) + loss tl (t+1) a b + a*mass tl (fun x => decide (t < x))
              = w * (a * (x-(t+1)).toNat + b*((t+1)-x).toNat)
                  + (loss tl (t+1) a b + a*mass tl (fun x => decide (t < x))) := by ac_rfl
          _ = w * (a * (x-(t+1)).toNat + b*((t+1)-x).toNat)
                  + (loss tl t a b + b*mass tl (fun x => decide (x ≤ t))) := by rw [ih]
          _ = (w * (a * (x-t).toNat + b*(t-x).toNat) + b*w)
                  + (loss tl t a b + b*mass tl (fun x => decide (x ≤ t))) := by
                rw [head_le x t a b w h1 h2 h3]
          _ = w * (a * (x-t).toNat + b*(t-x).toNat) + loss tl t a b
                  + b*(w + mass tl (fun x => decide (x ≤ t))) := by
                simp [Nat.mul_add, Nat.add_comm, Nat.add_left_comm, Nat.add_assoc]
      · have hle : decide (x ≤ t) = false := decide_eq_false_iff_not.mpr h
        have hgt : t < x := by omega
        have hgtf : decide (t < x) = true := decide_eq_true_eq.mpr hgt
        simp [hle, hgtf]
        have h1 : (t - x).toNat = 0 := by omega
        have h2 : ((t+1) - x).toNat = 0 := by omega
        have h3 : (x - t).toNat = (x - (t+1)).toNat + 1 := by omega
        calc
          w * (a * (x-(t+1)).toNat + b*((t+1)-x).toNat) + loss tl (t+1) a b + a*(w + mass tl (fun x => decide (t < x)))
              = (w * (a * (x-(t+1)).toNat + b*((t+1)-x).toNat) + a*w)
                  + (loss tl (t+1) a b + a*mass tl (fun x => decide (t < x))) := by
                  simp [Nat.mul_add, Nat.add_comm, Nat.add_left_comm, Nat.add_assoc]
          _ = w * (a * (x-t).toNat + b*(t-x).toNat)
                  + (loss tl (t+1) a b + a*mass tl (fun x => decide (t < x))) := by
                rw [head_gt x t a b w h1 h2 h3]
          _ = w * (a * (x-t).toNat + b*(t-x).toNat)
                  + (loss tl t a b + b*mass tl (fun x => decide (x ≤ t))) := by rw [ih]
          _ = w * (a * (x-t).toNat + b*(t-x).toNat) + loss tl t a b
                  + b * mass tl (fun x => decide (x ≤ t)) := by ac_rfl

private theorem step_up (xs : Costs) (t : Int) (a b : Nat)
    (h : a * mass xs (fun _ => true) ≤ (a+b) * mass xs (fun x => decide (x ≤ t))) :
    loss xs t a b ≤ loss xs (t+1) a b := by
  have hs := mass_split xs t
  have h' := h
  rw [← hs] at h'
  simp [Nat.mul_add, Nat.add_mul] at h'
  have hstep := step_identity xs t a b
  omega

private theorem step_down (xs : Costs) (t : Int) (a b : Nat)
    (h : (a+b) * mass xs (fun x => decide (x ≤ t)) ≤ a * mass xs (fun _ => true)) :
    loss xs (t+1) a b ≤ loss xs t a b := by
  have hs := mass_split xs t
  have h' := h
  rw [← hs] at h'
  simp [Nat.mul_add, Nat.add_mul] at h'
  have hstep := step_identity xs t a b
  omega

theorem sound_QuantileReserve : RiskEnvelope.QuantileReserve.Goal := by
  intro xs z a b hleft hright y
  by_cases hyz : z ≤ y
  · let n : Nat := (y - z).toNat
    have hy_eq : y = z + (n : Int) := by
      dsimp [n]
      have hnonneg : 0 ≤ y - z := by omega
      have hcast : ((y - z).toNat : Int) = y - z := Int.toNat_of_nonneg hnonneg
      omega
    rw [hy_eq]
    suffices hmono : ∀ k : Nat, loss xs z a b ≤ loss xs (z + (k : Int)) a b by
      exact hmono n
    intro k
    induction k with
    | zero => simp
    | succ k ih =>
        have hmk : mass xs (fun x => decide (x ≤ z)) ≤ mass xs (fun x => decide (x ≤ z + (k : Int))) :=
          mass_le_mono xs (by omega)
        have hmul := Nat.mul_le_mul_left (a + b) hmk
        have hcond : a * mass xs (fun _ => true) ≤ (a+b) * mass xs (fun x => decide (x ≤ z + (k : Int))) := by
          omega
        have hstep := step_up xs (z + (k : Int)) a b hcond
        rw [show z + ((k+1 : Nat) : Int) = z + (k : Int) + 1 by omega]
        omega
  · have hylt : y < z := by omega
    let n : Nat := (z - y).toNat
    have hy_eq : y = z - (n : Int) := by
      dsimp [n]
      have hnonneg : 0 ≤ z - y := by omega
      have hcast : ((z - y).toNat : Int) = z - y := Int.toNat_of_nonneg hnonneg
      omega
    rw [hy_eq]
    suffices hmono : ∀ k : Nat, loss xs z a b ≤ loss xs (z - (k : Int)) a b by
      exact hmono n
    intro k
    induction k with
    | zero => simp
    | succ k ih =>
        let t : Int := z - (k : Int) - 1
        have ht : t < z := by dsimp [t]; omega
        have hmk := mass_le_of_lt xs ht
        have hmul := Nat.mul_le_mul_left (a + b) hmk
        have hcond : (a+b) * mass xs (fun x => decide (x ≤ t)) ≤ a * mass xs (fun _ => true) := by
          omega
        have hstep := step_down xs t a b hcond
        dsimp [t] at hstep
        rw [show z - (k : Int) - 1 + 1 = z - (k : Int) by omega] at hstep
        rw [show z - ((k+1 : Nat) : Int) = z - (k : Int) - 1 by omega]
        omega

end TinylambOpt.ResearchProofs.Atlas.PlanSearch_QuantileReserve
