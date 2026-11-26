import TinylambOpt.ResearchGoals.Atlas.JoinPlans

/-!
Join planning can change the order or representation of matching work while preserving
the modeled result and multiplicity.

Proved

- `CountryReturn.FractionalCap` — `∀ n db b weights den, cover pattern weights den →
  (reference pattern n (fun r x y => if db r x y == 0 then 0 else 1) b).toNat ^ den ≤
  ((pattern.atoms.zip weights).map (fun (a,w) => support n db a pattern b ^ w)).prod`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_CountryReturn_FractionalCap

open TinylambOpt.ResearchGoals.Atlas.JoinPlans
open TinylambOpt.ResearchGoals.Atlas.JoinPlans.CountryReturn

private theorem nat_cast_sum {α} (l : List α) (f : α → Nat) :
    ((l.map f).sum : Int) = (l.map (fun x => (f x : Int))).sum := by
  induction l with
  | nil => simp
  | cons a l ih => simp [ih]

private theorem countP_eq_sum {α} (l : List α) (p : α → Bool) :
    l.countP p = (l.map (fun x => if p x then (1 : Nat) else 0)).sum := by
  induction l with
  | nil => simp
  | cons a l ih => by_cases h : p a = true <;> simp [h, ih] <;> omega

private def binN (db : DB) (r x y : Nat) : Nat := if db r x y == 0 then 0 else 1
private def binI (db : DB) : DB := fun r x y => (binN db r x y : Int)

private theorem binN_eq_ind (db : DB) (r x y : Nat) :
    binN db r x y = if db r x y != 0 then 1 else 0 := by
  by_cases h : db r x y = 0 <;> simp [binN, h]

private theorem binN_le_one (db : DB) (r x y : Nat) : binN db r x y ≤ 1 := by
  by_cases h : db r x y = 0 <;> simp [binN, h]

private def rsumN (n : Nat) (f : Nat → Nat) : Nat := ((List.range n).map f).sum

private def s0 (n : Nat) (db : DB) (b : Bind) : Nat := rsumN n (fun y => binN db 0 (b 0) y)
private def s1 (n : Nat) (db : DB) : Nat := rsumN n (fun x => rsumN n (fun y => binN db 1 x y))
private def s2 (n : Nat) (db : DB) : Nat := rsumN n (fun x => rsumN n (fun y => binN db 2 x y))
private def s3 (n : Nat) (db : DB) : Nat := rsumN n (fun x => rsumN n (fun y => binN db 1 x y))
private def s4 (n : Nat) (db : DB) (b : Bind) : Nat := rsumN n (fun y => binN db 2 (b 0) y)

private theorem support0_eq (n : Nat) (db : DB) (b : Bind) :
    support n db (⟨0, 0, 1⟩ : Atom) pattern b = s0 n db b := by
  simp [support, pattern, s0, rsumN, countP_eq_sum, binN_eq_ind]

private theorem support1_eq (n : Nat) (db : DB) (b : Bind) :
    support n db (⟨1, 1, 2⟩ : Atom) pattern b = s1 n db := by
  simp [support, pattern, s1, rsumN, countP_eq_sum, binN_eq_ind]

private theorem support2_eq (n : Nat) (db : DB) (b : Bind) :
    support n db (⟨2, 2, 3⟩ : Atom) pattern b = s2 n db := by
  simp [support, pattern, s2, rsumN, countP_eq_sum, binN_eq_ind]

private theorem support3_eq (n : Nat) (db : DB) (b : Bind) :
    support n db (⟨1, 3, 4⟩ : Atom) pattern b = s3 n db := by
  simp [support, pattern, s3, rsumN, countP_eq_sum, binN_eq_ind]

private theorem support4_eq (n : Nat) (db : DB) (b : Bind) :
    support n db (⟨2, 0, 4⟩ : Atom) pattern b = s4 n db b := by
  simp [support, pattern, s4, rsumN, countP_eq_sum, binN_eq_ind]

private theorem list_sum_congr_nat {α} (l : List α) (f g : α → Nat)
    (h : ∀ x ∈ l, f x = g x) : (l.map f).sum = (l.map g).sum := by
  induction l with
  | nil => simp
  | cons a l ih => simp [ih (fun x hx => h x (by simp [hx])), h a (by simp)]

private theorem rsumN_congr (n : Nat) {f g : Nat → Nat} (h : ∀ i, i < n → f i = g i) :
    rsumN n f = rsumN n g := by
  dsimp only [rsumN]
  apply list_sum_congr_nat
  intro i hi
  exact h i (List.mem_range.mp hi)

private theorem rsumN_succ (n : Nat) (f : Nat → Nat) : rsumN (n+1) f = rsumN n f + f n := by
  simp [rsumN, List.range_succ]

private theorem rsumN_le (n : Nat) {f g : Nat → Nat} (h : ∀ i, i < n → f i ≤ g i) :
    rsumN n f ≤ rsumN n g := by
  induction n with
  | zero => simp [rsumN]
  | succ n ih =>
      rw [rsumN_succ, rsumN_succ]
      exact Nat.add_le_add (ih (fun i hi => h i (Nat.lt_succ_of_lt hi))) (h n (Nat.lt_succ_self n))

private theorem rsumN_const_mul (n : Nat) (c : Nat) (f : Nat → Nat) :
    rsumN n (fun i => c * f i) = c * rsumN n f := by
  induction n with
  | zero => simp [rsumN]
  | succ n ih => rw [rsumN_succ, rsumN_succ, ih, Nat.mul_add]

private theorem rsumN_mul_const (n : Nat) (f : Nat → Nat) (c : Nat) :
    rsumN n (fun i => f i * c) = rsumN n f * c := by
  induction n with
  | zero => simp [rsumN]
  | succ n ih => rw [rsumN_succ, rsumN_succ, ih, Nat.add_mul]

private theorem rsumN_add (n : Nat) (f g : Nat → Nat) :
    rsumN n (fun i => f i + g i) = rsumN n f + rsumN n g := by
  induction n with
  | zero => simp [rsumN]
  | succ n ih => rw [rsumN_succ, rsumN_succ, rsumN_succ, ih]; omega

private theorem rsumN_zero_fun (m : Nat) : rsumN m (fun _ => (0 : Nat)) = 0 := by
  induction m with
  | zero => rfl
  | succ m ih => rw [rsumN_succ, ih]

private theorem rsumN_comm (n m : Nat) (f : Nat → Nat → Nat) :
    rsumN n (fun i => rsumN m (fun j => f i j)) = rsumN m (fun j => rsumN n (fun i => f i j)) := by
  induction n with
  | zero =>
      simp only [rsumN, List.range_zero, List.map_nil, List.sum_nil]
      rw [show (List.map (fun _ => (0 : Nat)) (List.range m)).sum = 0 by
        simpa [rsumN] using rsumN_zero_fun m]
  | succ n ih =>
      rw [rsumN_succ]
      calc
        rsumN n (fun i => rsumN m (fun j => f i j)) + rsumN m (fun j => f n j)
            = rsumN m (fun j => rsumN n (fun i => f i j)) + rsumN m (fun j => f n j) := by rw [ih]
        _ = rsumN m (fun j => rsumN n (fun i => f i j) + f n j) := by rw [rsumN_add]
        _ = rsumN m (fun j => rsumN (n+1) (fun i => f i j)) := by
          apply rsumN_congr
          intro j hj
          rw [rsumN_succ]

private theorem rsumN_mul_le_mul_rsum (n : Nat) (f g : Nat → Nat) :
    rsumN n (fun i => f i * g i) ≤ rsumN n f * rsumN n g := by
  induction n with
  | zero => simp [rsumN]
  | succ n ih =>
      rw [rsumN_succ, rsumN_succ, rsumN_succ]
      calc
        rsumN n (fun i => f i * g i) + f n * g n ≤ rsumN n f * rsumN n g + f n * g n :=
          Nat.add_le_add_right ih _
        _ ≤ rsumN n f * rsumN n g + (rsumN n f * g n + f n * rsumN n g + f n * g n) := by omega
        _ = (rsumN n f + f n) * (rsumN n g + g n) := by
          simp only [Nat.add_mul, Nat.mul_add]
          omega

private theorem rsum4_sep (n : Nat) (f : Nat → Nat) (g : Nat → Nat → Nat) (h : Nat → Nat) :
    (rsumN n (fun x1 => rsumN n (fun x2 => rsumN n (fun x3 => rsumN n (fun x4 =>
      (f x1 * g x2 x3) * h x4))))) =
    rsumN n f * (rsumN n (fun x2 => rsumN n (fun x3 => g x2 x3))) * rsumN n h := by
  simp only [rsumN_mul_const, rsumN_const_mul]

private theorem rsum4_prod (n : Nat) (f g : Nat → Nat → Nat) :
    (rsumN n (fun x1 => rsumN n (fun x2 => rsumN n (fun x3 => rsumN n (fun x4 =>
      f x1 x2 * g x3 x4))))) =
    (rsumN n (fun x1 => rsumN n (fun x2 => f x1 x2))) *
      (rsumN n (fun x3 => rsumN n (fun x4 => g x3 x4))) := by
  simp only [rsumN_const_mul, rsumN_mul_const]

private def refN (n : Nat) (db : DB) (b : Bind) : Nat :=
  rsumN n (fun x1 => rsumN n (fun x2 => rsumN n (fun x3 => rsumN n (fun x4 =>
    binN db 0 (b 0) x1 * binN db 1 x1 x2 * binN db 2 x2 x3 *
      binN db 1 x3 x4 * binN db 2 (b 0) x4))))

private theorem boundC (n : Nat) (db : DB) (b : Bind) :
    refN n db b ≤ s0 n db b * s2 n db * s3 n db := by
  let D : Nat → Nat := fun x3 => rsumN n (fun x4 => binN db 1 x3 x4)
  let Cs : Nat → Nat := fun x2 => rsumN n (fun x3 => binN db 2 x2 x3)
  have hDsum : rsumN n D = s3 n db := by simp [D, s3]
  have hCssum : rsumN n Cs = s2 n db := by simp [Cs, s2]
  have hpoint : ∀ x1 x2 x3 x4,
      binN db 0 (b 0) x1 * binN db 1 x1 x2 * binN db 2 x2 x3 *
          binN db 1 x3 x4 * binN db 2 (b 0) x4
        ≤ (binN db 0 (b 0) x1 * binN db 2 x2 x3) * binN db 1 x3 x4 := by
    intro x1 x2 x3 x4
    have h1 := binN_le_one db 1 x1 x2
    have h4 := binN_le_one db 2 (b 0) x4
    calc
      binN db 0 (b 0) x1 * binN db 1 x1 x2 * binN db 2 x2 x3 *
          binN db 1 x3 x4 * binN db 2 (b 0) x4
          ≤ (binN db 0 (b 0) x1 * 1) * binN db 2 x2 x3 * binN db 1 x3 x4 * 1 := by
            exact Nat.mul_le_mul
              (Nat.mul_le_mul (Nat.mul_le_mul (Nat.mul_le_mul (Nat.le_refl _) h1) (Nat.le_refl _)) (Nat.le_refl _))
              h4
      _ = (binN db 0 (b 0) x1 * binN db 2 x2 x3) * binN db 1 x3 x4 := by simp
  let E1 := rsumN n (fun x1 => rsumN n (fun x2 => rsumN n (fun x3 => rsumN n (fun x4 =>
      (binN db 0 (b 0) x1 * binN db 2 x2 x3) * binN db 1 x3 x4))))
  let E2 := rsumN n (fun x1 => rsumN n (fun x2 => rsumN n (fun x3 =>
      (binN db 0 (b 0) x1 * binN db 2 x2 x3) * D x3)))
  let E3 := rsumN n (fun x1 => rsumN n (fun x2 =>
      binN db 0 (b 0) x1 * rsumN n (fun x3 => binN db 2 x2 x3 * D x3)))
  let E4 := rsumN n (fun x1 => rsumN n (fun x2 =>
      binN db 0 (b 0) x1 * (Cs x2 * s3 n db)))
  have hle1 : refN n db b ≤ E1 := by
    dsimp only [E1, refN]
    apply rsumN_le; intro x1 hx1
    apply rsumN_le; intro x2 hx2
    apply rsumN_le; intro x3 hx3
    apply rsumN_le; intro x4 hx4
    exact hpoint x1 x2 x3 x4
  have hstep1 : E1 = E2 := by
    dsimp only [E1, E2]
    apply rsumN_congr; intro x1 hx1
    apply rsumN_congr; intro x2 hx2
    apply rsumN_congr; intro x3 hx3
    dsimp only [D]
    rw [rsumN_const_mul]
  have hstep2 : E2 = E3 := by
    dsimp only [E2, E3]
    apply rsumN_congr; intro x1 hx1
    apply rsumN_congr; intro x2 hx2
    calc
      rsumN n (fun x3 => (binN db 0 (b 0) x1 * binN db 2 x2 x3) * D x3)
          = rsumN n (fun x3 => binN db 0 (b 0) x1 * (binN db 2 x2 x3 * D x3)) := by
            apply rsumN_congr; intro x3 hx3; ac_rfl
      _ = binN db 0 (b 0) x1 * rsumN n (fun x3 => binN db 2 x2 x3 * D x3) := by
        rw [rsumN_const_mul]
  have hstep3 : E3 ≤ E4 := by
    dsimp only [E3, E4]
    apply rsumN_le; intro x1 hx1
    apply rsumN_le; intro x2 hx2
    apply Nat.mul_le_mul_left
    calc
      rsumN n (fun x3 => binN db 2 x2 x3 * D x3)
          ≤ (rsumN n (fun x3 => binN db 2 x2 x3)) * (rsumN n D) :=
        rsumN_mul_le_mul_rsum n (fun x3 => binN db 2 x2 x3) D
      _ = Cs x2 * s3 n db := by rw [hDsum]
  have hstep4 : E4 = s0 n db b * s2 n db * s3 n db := by
    dsimp only [E4]
    calc
      rsumN n (fun x1 => rsumN n (fun x2 =>
        binN db 0 (b 0) x1 * (Cs x2 * s3 n db)))
          = rsumN n (fun x1 => binN db 0 (b 0) x1 *
              rsumN n (fun x2 => Cs x2 * s3 n db)) := by
            apply rsumN_congr; intro x1 hx1
            rw [rsumN_const_mul]
      _ = (rsumN n (fun x1 => binN db 0 (b 0) x1)) *
            rsumN n (fun x2 => Cs x2 * s3 n db) := by
            rw [rsumN_mul_const]
      _ = s0 n db b * (s2 n db * s3 n db) := by
            rw [rsumN_mul_const, hCssum]
            simp [s0]
      _ = s0 n db b * s2 n db * s3 n db := by rw [Nat.mul_assoc]
  calc
    refN n db b ≤ E1 := hle1
    _ = E2 := hstep1
    _ = E3 := hstep2
    _ ≤ E4 := hstep3
    _ = s0 n db b * s2 n db * s3 n db := hstep4

private theorem boundE (n : Nat) (db : DB) (b : Bind) :
    refN n db b ≤ s1 n db * s2 n db * s4 n db b := by
  let Cs : Nat → Nat := fun x2 => rsumN n (fun x3 => binN db 2 x2 x3)
  let Bs : Nat → Nat := fun x2 => rsumN n (fun x1 => binN db 1 x1 x2)
  have hCssum : rsumN n Cs = s2 n db := by simp [Cs, s2]
  have hBssum : rsumN n Bs = s1 n db := by
    dsimp only [Bs, s1]
    rw [rsumN_comm]
  have hpoint : ∀ x1 x2 x3 x4,
      binN db 0 (b 0) x1 * binN db 1 x1 x2 * binN db 2 x2 x3 *
          binN db 1 x3 x4 * binN db 2 (b 0) x4
        ≤ (binN db 1 x1 x2 * binN db 2 x2 x3) * binN db 2 (b 0) x4 := by
    intro x1 x2 x3 x4
    have h0 := binN_le_one db 0 (b 0) x1
    have h3 := binN_le_one db 1 x3 x4
    calc
      binN db 0 (b 0) x1 * binN db 1 x1 x2 * binN db 2 x2 x3 *
          binN db 1 x3 x4 * binN db 2 (b 0) x4
          ≤ 1 * binN db 1 x1 x2 * binN db 2 x2 x3 * 1 * binN db 2 (b 0) x4 := by
            exact Nat.mul_le_mul
              (Nat.mul_le_mul (Nat.mul_le_mul (Nat.mul_le_mul h0 (Nat.le_refl _)) (Nat.le_refl _)) h3)
              (Nat.le_refl _)
      _ = (binN db 1 x1 x2 * binN db 2 x2 x3) * binN db 2 (b 0) x4 := by simp
  let E1 := rsumN n (fun x1 => rsumN n (fun x2 => rsumN n (fun x3 => rsumN n (fun x4 =>
      (binN db 1 x1 x2 * binN db 2 x2 x3) * binN db 2 (b 0) x4))))
  let E2 := rsumN n (fun x1 => rsumN n (fun x2 => rsumN n (fun x3 =>
      (binN db 1 x1 x2 * binN db 2 x2 x3) * s4 n db b)))
  let E3 := rsumN n (fun x1 => rsumN n (fun x2 =>
      (binN db 1 x1 x2 * Cs x2) * s4 n db b))
  let E4 := rsumN n (fun x2 => rsumN n (fun x1 =>
      (binN db 1 x1 x2 * Cs x2) * s4 n db b))
  let E5 := rsumN n (fun x2 => (Bs x2 * Cs x2) * s4 n db b)
  have hle1 : refN n db b ≤ E1 := by
    dsimp only [E1, refN]
    apply rsumN_le; intro x1 hx1
    apply rsumN_le; intro x2 hx2
    apply rsumN_le; intro x3 hx3
    apply rsumN_le; intro x4 hx4
    exact hpoint x1 x2 x3 x4
  have hstep1 : E1 = E2 := by
    dsimp only [E1, E2]
    apply rsumN_congr; intro x1 hx1
    apply rsumN_congr; intro x2 hx2
    apply rsumN_congr; intro x3 hx3
    dsimp only [s4]
    rw [rsumN_const_mul]
  have hstep2 : E2 = E3 := by
    dsimp only [E2, E3]
    apply rsumN_congr; intro x1 hx1
    apply rsumN_congr; intro x2 hx2
    calc
      rsumN n (fun x3 => (binN db 1 x1 x2 * binN db 2 x2 x3) * s4 n db b)
          = rsumN n (fun x3 => (binN db 1 x1 x2 * s4 n db b) * binN db 2 x2 x3) := by
            apply rsumN_congr; intro x3 hx3; ac_rfl
      _ = (binN db 1 x1 x2 * s4 n db b) * (rsumN n (fun x3 => binN db 2 x2 x3)) := by
        rw [rsumN_const_mul]
      _ = (binN db 1 x1 x2 * Cs x2) * s4 n db b := by
        dsimp only [Cs]; ac_rfl
  have hstep3 : E3 = E4 := by
    dsimp only [E3, E4]
    rw [rsumN_comm]
  have hstep4 : E4 = E5 := by
    dsimp only [E4, E5]
    apply rsumN_congr; intro x2 hx2
    calc
      rsumN n (fun x1 => (binN db 1 x1 x2 * Cs x2) * s4 n db b)
          = rsumN n (fun x1 => (binN db 1 x1 x2 * s4 n db b) * Cs x2) := by
            apply rsumN_congr; intro x1 hx1; ac_rfl
      _ = (rsumN n (fun x1 => binN db 1 x1 x2 * s4 n db b)) * Cs x2 := by
        rw [rsumN_mul_const]
      _ = (rsumN n (fun x1 => binN db 1 x1 x2) * s4 n db b) * Cs x2 := by
        rw [rsumN_mul_const]
      _ = (Bs x2 * Cs x2) * s4 n db b := by
        dsimp only [Bs]; ac_rfl
  have hstep5 : E5 ≤ s1 n db * s2 n db * s4 n db b := by
    dsimp only [E5]
    have hmul : rsumN n (fun x2 => Bs x2 * Cs x2) ≤ rsumN n Bs * rsumN n Cs :=
      rsumN_mul_le_mul_rsum n Bs Cs
    calc
      rsumN n (fun x2 => (Bs x2 * Cs x2) * s4 n db b)
          = rsumN n (fun x2 => Bs x2 * Cs x2) * s4 n db b := by rw [rsumN_mul_const]
      _ ≤ (rsumN n Bs * rsumN n Cs) * s4 n db b := Nat.mul_le_mul_right _ hmul
      _ = s1 n db * s2 n db * s4 n db b := by rw [hBssum, hCssum]
  calc
    refN n db b ≤ E1 := hle1
    _ = E2 := hstep1
    _ = E3 := hstep2
    _ = E4 := hstep3
    _ = E5 := hstep4
    _ ≤ s1 n db * s2 n db * s4 n db b := hstep5

private theorem boundA (n : Nat) (db : DB) (b : Bind) :
    refN n db b ≤ s0 n db b * s2 n db * s4 n db b := by
  have hpoint : ∀ x1 x2 x3 x4,
      binN db 0 (b 0) x1 * binN db 1 x1 x2 * binN db 2 x2 x3 *
          binN db 1 x3 x4 * binN db 2 (b 0) x4
        ≤ (binN db 0 (b 0) x1 * binN db 2 x2 x3) * binN db 2 (b 0) x4 := by
    intro x1 x2 x3 x4
    have h1 := binN_le_one db 1 x1 x2
    have h3 := binN_le_one db 1 x3 x4
    calc
      binN db 0 (b 0) x1 * binN db 1 x1 x2 * binN db 2 x2 x3 *
          binN db 1 x3 x4 * binN db 2 (b 0) x4
          ≤ (binN db 0 (b 0) x1 * 1) * binN db 2 x2 x3 * 1 * binN db 2 (b 0) x4 := by
            exact Nat.mul_le_mul
              (Nat.mul_le_mul (Nat.mul_le_mul (Nat.mul_le_mul (Nat.le_refl _) h1) (Nat.le_refl _)) h3)
              (Nat.le_refl _)
      _ = (binN db 0 (b 0) x1 * binN db 2 x2 x3) * binN db 2 (b 0) x4 := by simp
  calc
    refN n db b ≤ rsumN n (fun x1 => rsumN n (fun x2 => rsumN n (fun x3 => rsumN n (fun x4 =>
        (binN db 0 (b 0) x1 * binN db 2 x2 x3) * binN db 2 (b 0) x4)))) := by
      dsimp only [refN]
      apply rsumN_le; intro x1 hx1
      apply rsumN_le; intro x2 hx2
      apply rsumN_le; intro x3 hx3
      apply rsumN_le; intro x4 hx4
      exact hpoint x1 x2 x3 x4
    _ = s0 n db b * s2 n db * s4 n db b := by
      rw [rsum4_sep]
      simp [s0, s2, s4]

private theorem boundB (n : Nat) (db : DB) (b : Bind) :
    refN n db b ≤ s1 n db * s3 n db := by
  have hpoint : ∀ x1 x2 x3 x4,
      binN db 0 (b 0) x1 * binN db 1 x1 x2 * binN db 2 x2 x3 *
          binN db 1 x3 x4 * binN db 2 (b 0) x4
        ≤ binN db 1 x1 x2 * binN db 1 x3 x4 := by
    intro x1 x2 x3 x4
    have h0 := binN_le_one db 0 (b 0) x1
    have h2 := binN_le_one db 2 x2 x3
    have h4 := binN_le_one db 2 (b 0) x4
    calc
      binN db 0 (b 0) x1 * binN db 1 x1 x2 * binN db 2 x2 x3 *
          binN db 1 x3 x4 * binN db 2 (b 0) x4
          ≤ (1 * binN db 1 x1 x2) * 1 * binN db 1 x3 x4 * 1 := by
            exact Nat.mul_le_mul
              (Nat.mul_le_mul (Nat.mul_le_mul (Nat.mul_le_mul h0 (Nat.le_refl _)) h2) (Nat.le_refl _)) h4
      _ = binN db 1 x1 x2 * binN db 1 x3 x4 := by simp
  calc
    refN n db b ≤ rsumN n (fun x1 => rsumN n (fun x2 => rsumN n (fun x3 => rsumN n (fun x4 =>
        binN db 1 x1 x2 * binN db 1 x3 x4)))) := by
      dsimp only [refN]
      apply rsumN_le; intro x1 hx1
      apply rsumN_le; intro x2 hx2
      apply rsumN_le; intro x3 hx3
      apply rsumN_le; intro x4 hx4
      exact hpoint x1 x2 x3 x4
    _ = s1 n db * s3 n db := by
      rw [rsum4_prod]
      simp [s1, s3]

private theorem rsumN_eq_zero_of_all (n : Nat) {f : Nat → Nat} (h : ∀ i, i < n → f i = 0) :
    rsumN n f = 0 := by
  induction n with
  | zero => rfl
  | succ n ih =>
      rw [rsumN_succ, h n (Nat.lt_succ_self n)]
      exact ih (fun i hi => h i (Nat.lt_succ_of_lt hi))

private theorem rsumN_zero_imp (n : Nat) {f : Nat → Nat} (h : rsumN n f = 0) :
    ∀ i, i < n → f i = 0 := by
  induction n with
  | zero => omega
  | succ n ih =>
      rw [rsumN_succ] at h
      have hs : rsumN n f = 0 := by omega
      have hn : f n = 0 := by omega
      intro i hi
      by_cases hin : i = n
      · subst i
        exact hn
      · exact ih hs i (by omega)

private theorem pow_ge_one (S k : Nat) (hS : 1 ≤ S) : 1 ≤ S ^ k := Nat.one_le_pow k S hS

private theorem weighted_bound (T S0 S1 S2 S3 S4 : Nat)
    (hA : T ≤ S0 * S2 * S4) (hB : T ≤ S1 * S3)
    (hC : T ≤ S0 * S2 * S3) (hE : T ≤ S1 * S2 * S4)
    (hS0 : 1 ≤ S0) (hS1 : 1 ≤ S1) (hS2 : 1 ≤ S2) (hS3 : 1 ≤ S3) (hS4 : 1 ≤ S4) :
    ∀ D : Nat, 0 < D → ∀ a b c d e : Nat,
      D ≤ a + b → D ≤ b + c → D ≤ c + d → D ≤ d + e →
      T ^ D ≤ S0 ^ a * S1 ^ b * S2 ^ c * S3 ^ d * S4 ^ e := by
  intro D
  induction D using Nat.strongRecOn with
  | ind D ih =>
      intro hD a b c d e h1 h2 h3 h4
      by_cases hAred : 0 < a ∧ 0 < c ∧ 0 < e
      · by_cases hDone : D = 1
        · subst D
          have ha : 1 ≤ a := by omega
          have hc : 1 ≤ c := by omega
          have he : 1 ≤ e := by omega
          have hp : T ≤ S0 ^ a * S2 ^ c * S4 ^ e := by
            calc
              T ≤ S0 * S2 * S4 := hA
              _ ≤ S0 ^ a * S2 ^ c * S4 ^ e := by
                exact Nat.mul_le_mul (Nat.mul_le_mul
                  (by simpa using Nat.pow_le_pow_right hS0 ha) (by simpa using Nat.pow_le_pow_right hS2 hc))
                  (by simpa using Nat.pow_le_pow_right hS4 he)
          have hrest : 1 ≤ S1 ^ b * S3 ^ d :=
            Nat.mul_le_mul (pow_ge_one S1 b hS1) (pow_ge_one S3 d hS3)
          calc
            T ^ 1 = T := by simp
            _ ≤ S0 ^ a * S2 ^ c * S4 ^ e := hp
            _ ≤ (S0 ^ a * S2 ^ c * S4 ^ e) * (S1 ^ b * S3 ^ d) :=
              Nat.le_mul_of_pos_right _ (by omega)
            _ = S0 ^ a * S1 ^ b * S2 ^ c * S3 ^ d * S4 ^ e := by ac_rfl
        · have hD2 : 2 ≤ D := by omega
          have hih := ih (D - 1) (by omega) (by omega) (a - 1) b (c - 1) d (e - 1)
            (by omega) (by omega) (by omega) (by omega)
          rw [show D = (D - 1) + 1 by omega, Nat.pow_succ]
          have ha : S0 ^ a = S0 ^ (a - 1) * S0 := by
            calc
              S0 ^ a = S0 ^ ((a - 1) + 1) := by congr 1; omega
              _ = S0 ^ (a - 1) * S0 := by rw [Nat.pow_succ]
          have hc : S2 ^ c = S2 ^ (c - 1) * S2 := by
            calc
              S2 ^ c = S2 ^ ((c - 1) + 1) := by congr 1; omega
              _ = S2 ^ (c - 1) * S2 := by rw [Nat.pow_succ]
          have he : S4 ^ e = S4 ^ (e - 1) * S4 := by
            calc
              S4 ^ e = S4 ^ ((e - 1) + 1) := by congr 1; omega
              _ = S4 ^ (e - 1) * S4 := by rw [Nat.pow_succ]
          calc
            T ^ (D - 1) * T ≤
                (S0 ^ (a - 1) * S1 ^ b * S2 ^ (c - 1) * S3 ^ d * S4 ^ (e - 1)) *
                  (S0 * S2 * S4) := Nat.mul_le_mul hih hA
            _ = S0 ^ a * S1 ^ b * S2 ^ c * S3 ^ d * S4 ^ e := by
              rw [ha, hc, he]
              ac_rfl
      · by_cases hBred : 0 < b ∧ 0 < d
        · by_cases hDone : D = 1
          · subst D
            have hb : 1 ≤ b := by omega
            have hd : 1 ≤ d := by omega
            have hp : T ≤ S1 ^ b * S3 ^ d := by
              calc
                T ≤ S1 * S3 := hB
                _ ≤ S1 ^ b * S3 ^ d :=
                  Nat.mul_le_mul (by simpa using Nat.pow_le_pow_right hS1 hb) (by simpa using Nat.pow_le_pow_right hS3 hd)
            have hrest : 1 ≤ S0 ^ a * S2 ^ c * S4 ^ e :=
              Nat.mul_le_mul (Nat.mul_le_mul (pow_ge_one S0 a hS0) (pow_ge_one S2 c hS2))
                (pow_ge_one S4 e hS4)
            calc
              T ^ 1 = T := by simp
              _ ≤ S1 ^ b * S3 ^ d := hp
              _ ≤ (S1 ^ b * S3 ^ d) * (S0 ^ a * S2 ^ c * S4 ^ e) :=
                Nat.le_mul_of_pos_right _ (by omega)
              _ = S0 ^ a * S1 ^ b * S2 ^ c * S3 ^ d * S4 ^ e := by ac_rfl
          · have hD2 : 2 ≤ D := by omega
            have hih := ih (D - 1) (by omega) (by omega) a (b - 1) c (d - 1) e
              (by omega) (by omega) (by omega) (by omega)
            rw [show D = (D - 1) + 1 by omega, Nat.pow_succ]
            have hb : S1 ^ b = S1 ^ (b - 1) * S1 := by
              calc
                S1 ^ b = S1 ^ ((b - 1) + 1) := by congr 1; omega
                _ = S1 ^ (b - 1) * S1 := by rw [Nat.pow_succ]
            have hd : S3 ^ d = S3 ^ (d - 1) * S3 := by
              calc
                S3 ^ d = S3 ^ ((d - 1) + 1) := by congr 1; omega
                _ = S3 ^ (d - 1) * S3 := by rw [Nat.pow_succ]
            calc
              T ^ (D - 1) * T ≤
                  (S0 ^ a * S1 ^ (b - 1) * S2 ^ c * S3 ^ (d - 1) * S4 ^ e) *
                    (S1 * S3) := Nat.mul_le_mul hih hB
              _ = S0 ^ a * S1 ^ b * S2 ^ c * S3 ^ d * S4 ^ e := by
                rw [hb, hd]
                ac_rfl
        · have hdisj : (D ≤ a ∧ D ≤ c ∧ D ≤ e) ∨ (D ≤ b ∧ D ≤ d) ∨
              (D ≤ a ∧ D ≤ c ∧ D ≤ d) ∨ (D ≤ b ∧ D ≤ c ∧ D ≤ e) := by
            omega
          rcases hdisj with hca | hcb | hcc | hce
          · have hp : T ^ D ≤ S0 ^ a * S2 ^ c * S4 ^ e := by
              calc
                T ^ D ≤ (S0 * S2 * S4) ^ D := Nat.pow_le_pow_left hA D
                _ = S0 ^ D * S2 ^ D * S4 ^ D := by rw [Nat.mul_pow, Nat.mul_pow]
                _ ≤ S0 ^ a * S2 ^ c * S4 ^ e := by
                  exact Nat.mul_le_mul (Nat.mul_le_mul
                    (Nat.pow_le_pow_right hS0 hca.1) (Nat.pow_le_pow_right hS2 hca.2.1))
                    (Nat.pow_le_pow_right hS4 hca.2.2)
            have hrest : 1 ≤ S1 ^ b * S3 ^ d :=
              Nat.mul_le_mul (pow_ge_one S1 b hS1) (pow_ge_one S3 d hS3)
            calc
              T ^ D ≤ S0 ^ a * S2 ^ c * S4 ^ e := hp
              _ ≤ (S0 ^ a * S2 ^ c * S4 ^ e) * (S1 ^ b * S3 ^ d) :=
                Nat.le_mul_of_pos_right _ (by omega)
              _ = S0 ^ a * S1 ^ b * S2 ^ c * S3 ^ d * S4 ^ e := by ac_rfl
          · have hp : T ^ D ≤ S1 ^ b * S3 ^ d := by
              calc
                T ^ D ≤ (S1 * S3) ^ D := Nat.pow_le_pow_left hB D
                _ = S1 ^ D * S3 ^ D := by rw [Nat.mul_pow]
                _ ≤ S1 ^ b * S3 ^ d :=
                  Nat.mul_le_mul (by simpa using Nat.pow_le_pow_right hS1 hcb.1) (by simpa using Nat.pow_le_pow_right hS3 hcb.2)
            have hrest : 1 ≤ S0 ^ a * S2 ^ c * S4 ^ e :=
              Nat.mul_le_mul (Nat.mul_le_mul (pow_ge_one S0 a hS0) (pow_ge_one S2 c hS2))
                (pow_ge_one S4 e hS4)
            calc
              T ^ D ≤ S1 ^ b * S3 ^ d := hp
              _ ≤ (S1 ^ b * S3 ^ d) * (S0 ^ a * S2 ^ c * S4 ^ e) :=
                Nat.le_mul_of_pos_right _ (by omega)
              _ = S0 ^ a * S1 ^ b * S2 ^ c * S3 ^ d * S4 ^ e := by ac_rfl
          · have hp : T ^ D ≤ S0 ^ a * S2 ^ c * S3 ^ d := by
              calc
                T ^ D ≤ (S0 * S2 * S3) ^ D := Nat.pow_le_pow_left hC D
                _ = S0 ^ D * S2 ^ D * S3 ^ D := by rw [Nat.mul_pow, Nat.mul_pow]
                _ ≤ S0 ^ a * S2 ^ c * S3 ^ d := by
                  exact Nat.mul_le_mul (Nat.mul_le_mul
                    (Nat.pow_le_pow_right hS0 hcc.1) (Nat.pow_le_pow_right hS2 hcc.2.1))
                    (Nat.pow_le_pow_right hS3 hcc.2.2)
            have hrest : 1 ≤ S1 ^ b * S4 ^ e :=
              Nat.mul_le_mul (pow_ge_one S1 b hS1) (pow_ge_one S4 e hS4)
            calc
              T ^ D ≤ S0 ^ a * S2 ^ c * S3 ^ d := hp
              _ ≤ (S0 ^ a * S2 ^ c * S3 ^ d) * (S1 ^ b * S4 ^ e) :=
                Nat.le_mul_of_pos_right _ (by omega)
              _ = S0 ^ a * S1 ^ b * S2 ^ c * S3 ^ d * S4 ^ e := by ac_rfl
          · have hp : T ^ D ≤ S1 ^ b * S2 ^ c * S4 ^ e := by
              calc
                T ^ D ≤ (S1 * S2 * S4) ^ D := Nat.pow_le_pow_left hE D
                _ = S1 ^ D * S2 ^ D * S4 ^ D := by rw [Nat.mul_pow, Nat.mul_pow]
                _ ≤ S1 ^ b * S2 ^ c * S4 ^ e := by
                  exact Nat.mul_le_mul (Nat.mul_le_mul
                    (Nat.pow_le_pow_right hS1 hce.1) (Nat.pow_le_pow_right hS2 hce.2.1))
                    (Nat.pow_le_pow_right hS4 hce.2.2)
            have hrest : 1 ≤ S0 ^ a * S3 ^ d :=
              Nat.mul_le_mul (pow_ge_one S0 a hS0) (pow_ge_one S3 d hS3)
            calc
              T ^ D ≤ S1 ^ b * S2 ^ c * S4 ^ e := hp
              _ ≤ (S1 ^ b * S2 ^ c * S4 ^ e) * (S0 ^ a * S3 ^ d) :=
                Nat.le_mul_of_pos_right _ (by omega)
              _ = S0 ^ a * S1 ^ b * S2 ^ c * S3 ^ d * S4 ^ e := by ac_rfl

private theorem reference_bin_count (n : Nat) (db : DB) (b : Bind) :
    (reference pattern n (fun r x y => if db r x y == 0 then 0 else 1) b).toNat = refN n db b := by
  have hbin : (fun r x y => if db r x y == 0 then 0 else 1) = binI db := by
    funext r x y
    by_cases h : db r x y == 0 <;> simp [binI, binN, h]
  rw [hbin]
  have hi : reference pattern n (binI db) b = (refN n db b : Int) := by
    simp [reference, pattern, integrate, value, put, refN, rsumN, binI, binN, nat_cast_sum, Int.mul_assoc]
  rw [hi]
  simp

theorem sound_CountryReturn_FractionalCap : CountryReturn.FractionalCap.Goal := by
  intro n db b weights den hcover
  have hden : 0 < den := hcover.1
  have hwlen : weights.length = 5 := by simpa [pattern] using hcover.2.1
  obtain ⟨w0, w1, w2, w3, w4, hw⟩ : ∃ a b c d e : Nat, weights = [a, b, c, d, e] := by
    cases weights with
    | nil => simp at hwlen
    | cons x xs =>
        cases xs with
        | nil => simp at hwlen
        | cons y ys =>
            cases ys with
            | nil => simp at hwlen
            | cons z zs =>
                cases zs with
                | nil => simp at hwlen
                | cons u us =>
                    cases us with
                    | nil => simp at hwlen
                    | cons v vs =>
                        simp at hwlen
                        subst vs
                        exact ⟨x, y, z, u, v, rfl⟩
  subst weights
  have hc1 : den ≤ w0 + w1 := by
    have h := hcover.2.2 1 (by simp [pattern])
    simp [pattern] at h
    omega
  have hc2 : den ≤ w1 + w2 := by
    have h := hcover.2.2 2 (by simp [pattern])
    simp [pattern] at h
    omega
  have hc3 : den ≤ w2 + w3 := by
    have h := hcover.2.2 3 (by simp [pattern])
    simp [pattern] at h
    omega
  have hc4 : den ≤ w3 + w4 := by
    have h := hcover.2.2 4 (by simp [pattern])
    simp [pattern] at h
    omega
  rw [reference_bin_count n db b]
  rw [show ((pattern.atoms.zip [w0, w1, w2, w3, w4]).map
        (fun (a, w) => support n db a pattern b ^ w)).prod =
      s0 n db b ^ w0 * s1 n db ^ w1 * s2 n db ^ w2 * s3 n db ^ w3 * s4 n db b ^ w4 by
    simp [pattern]
    have h0 : support n db (⟨0, 0, 1⟩ : Atom)
        { atoms := [⟨0, 0, 1⟩, ⟨1, 1, 2⟩, ⟨2, 2, 3⟩, ⟨1, 3, 4⟩, ⟨2, 0, 4⟩],
          freeVars := [1, 2, 3, 4] } b = s0 n db b := by simpa [pattern] using support0_eq n db b
    have h1 : support n db (⟨1, 1, 2⟩ : Atom)
        { atoms := [⟨0, 0, 1⟩, ⟨1, 1, 2⟩, ⟨2, 2, 3⟩, ⟨1, 3, 4⟩, ⟨2, 0, 4⟩],
          freeVars := [1, 2, 3, 4] } b = s1 n db := by simpa [pattern] using support1_eq n db b
    have h2 : support n db (⟨2, 2, 3⟩ : Atom)
        { atoms := [⟨0, 0, 1⟩, ⟨1, 1, 2⟩, ⟨2, 2, 3⟩, ⟨1, 3, 4⟩, ⟨2, 0, 4⟩],
          freeVars := [1, 2, 3, 4] } b = s2 n db := by simpa [pattern] using support2_eq n db b
    have h3 : support n db (⟨1, 3, 4⟩ : Atom)
        { atoms := [⟨0, 0, 1⟩, ⟨1, 1, 2⟩, ⟨2, 2, 3⟩, ⟨1, 3, 4⟩, ⟨2, 0, 4⟩],
          freeVars := [1, 2, 3, 4] } b = s3 n db := by simpa [pattern] using support3_eq n db b
    have h4 : support n db (⟨2, 0, 4⟩ : Atom)
        { atoms := [⟨0, 0, 1⟩, ⟨1, 1, 2⟩, ⟨2, 2, 3⟩, ⟨1, 3, 4⟩, ⟨2, 0, 4⟩],
          freeVars := [1, 2, 3, 4] } b = s4 n db b := by simpa [pattern] using support4_eq n db b
    rw [h0, h1, h2, h3, h4]
    ac_rfl]
  by_cases hz0 : s0 n db b = 0
  · have ht : refN n db b = 0 := by
      dsimp only [refN]
      apply rsumN_eq_zero_of_all
      intro x1 hx1
      have hzero := rsumN_zero_imp n hz0 x1 hx1
      rw [hzero]
      simp [rsumN_zero_fun]
    simp [ht, hden]
  by_cases hz4 : s4 n db b = 0
  · have ht : refN n db b = 0 := by
      dsimp only [refN]
      dsimp only [s4] at hz4
      have hzero4 := rsumN_zero_imp n hz4
      apply rsumN_eq_zero_of_all; intro x1 hx1
      apply rsumN_eq_zero_of_all; intro x2 hx2
      apply rsumN_eq_zero_of_all; intro x3 hx3
      apply rsumN_eq_zero_of_all; intro t4 ht4
      rw [hzero4 t4 ht4]
      simp
    simp [ht, hden]
  by_cases hz1 : s1 n db = 0
  · have ht : refN n db b = 0 := by
      dsimp only [refN]
      dsimp only [s1] at hz1
      have hzero1 := rsumN_zero_imp n hz1
      apply rsumN_eq_zero_of_all
      intro x1 hx1
      have hinner : rsumN n (fun x2 => binN db 1 x1 x2) = 0 := hzero1 x1 hx1
      have hzero2 := rsumN_zero_imp n hinner
      apply rsumN_eq_zero_of_all
      intro x2 hx2
      rw [hzero2 x2 hx2]
      simp [rsumN_zero_fun]
    simp [ht, hden]
  by_cases hz3 : s3 n db = 0
  · have ht : refN n db b = 0 := by
      dsimp only [refN]
      dsimp only [s3] at hz3
      have hzero3 := rsumN_zero_imp n hz3
      apply rsumN_eq_zero_of_all
      intro x1 hx1
      apply rsumN_eq_zero_of_all
      intro x2 hx2
      apply rsumN_eq_zero_of_all
      intro x3 hx3
      have hinner : rsumN n (fun x4 => binN db 1 x3 x4) = 0 := hzero3 x3 hx3
      have hzero4 := rsumN_zero_imp n hinner
      apply rsumN_eq_zero_of_all
      intro x4 hx4
      rw [hzero4 x4 hx4]
      simp
    simp [ht, hden]
  by_cases hz2 : s2 n db = 0
  · have ht : refN n db b = 0 := by
      dsimp only [refN]
      dsimp only [s2] at hz2
      have hzero2 := rsumN_zero_imp n hz2
      apply rsumN_eq_zero_of_all
      intro x1 hx1
      apply rsumN_eq_zero_of_all
      intro x2 hx2
      have hinner : rsumN n (fun x3 => binN db 2 x2 x3) = 0 := hzero2 x2 hx2
      have hzero3 := rsumN_zero_imp n hinner
      apply rsumN_eq_zero_of_all
      intro x3 hx3
      rw [hzero3 x3 hx3]
      simp [rsumN_zero_fun]
    simp [ht, hden]
  have hS0 : 1 ≤ s0 n db b := by omega
  have hS1 : 1 ≤ s1 n db := by omega
  have hS2 : 1 ≤ s2 n db := by omega
  have hS3 : 1 ≤ s3 n db := by omega
  have hS4 : 1 ≤ s4 n db b := by omega
  exact weighted_bound (refN n db b) (s0 n db b) (s1 n db) (s2 n db) (s3 n db) (s4 n db b)
    (boundA n db b) (boundB n db b) (boundC n db b) (boundE n db b)
    hS0 hS1 hS2 hS3 hS4 den hden w0 w1 w2 w3 w4 hc1 hc2 hc3 hc4

end TinylambOpt.ResearchProofs.Atlas.Goal_CountryReturn_FractionalCap
