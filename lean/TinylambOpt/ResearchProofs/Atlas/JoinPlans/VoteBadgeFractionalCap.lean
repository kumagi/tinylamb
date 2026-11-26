import TinylambOpt.ResearchGoals.Atlas.JoinPlans

/-!
Join planning can change the order or representation of matching work while preserving
the modeled result and multiplicity.

Proved

- `VoteBadge.FractionalCap` — `∀ n db b weights den, cover pattern weights den →
  (reference pattern n (fun r x y => if db r x y == 0 then 0 else 1) b).toNat ^ den ≤
  ((pattern.atoms.zip weights).map (fun (a,w) => support n db a pattern b ^ w)).prod`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.JoinPlans.VoteBadgeFractionalCap

open TinylambOpt.ResearchGoals.Atlas.JoinPlans
open TinylambOpt.ResearchGoals.Atlas.JoinPlans.VoteBadge

private def rsumN (n : Nat) (f : Nat → Nat) : Nat := ((List.range n).map f).sum

private theorem rsumN_succ (n : Nat) (f : Nat → Nat) : rsumN (n+1) f = rsumN n f + f n := by
  simp [rsumN, List.range_succ]

private theorem rsumN_congr (n : Nat) {f g : Nat → Nat} (h : ∀ i, i < n → f i = g i) :
    rsumN n f = rsumN n g := by
  induction n with
  | zero => simp [rsumN]
  | succ n ih =>
      rw [rsumN_succ, rsumN_succ]
      rw [ih (fun i hi => h i (Nat.lt_succ_of_lt hi)), h n (Nat.lt_succ_self n)]

private theorem rsumN_add (n : Nat) (f g : Nat → Nat) :
    rsumN n (fun i => f i + g i) = rsumN n f + rsumN n g := by
  induction n with
  | zero => simp [rsumN]
  | succ n ih =>
      rw [rsumN_succ, rsumN_succ, rsumN_succ, ih]
      omega

private theorem rsumN_zero (n : Nat) : rsumN n (fun _ => (0 : Nat)) = 0 := by
  induction n with
  | zero => simp [rsumN]
  | succ n ih => rw [rsumN_succ, ih]

private theorem list_sum_zero (l : List Nat) : (l.map (fun _ => (0 : Nat))).sum = 0 := by
  induction l with
  | nil => simp
  | cons a l ih => simp [ih]

private theorem rsumN_comm (n m : Nat) (f : Nat → Nat → Nat) :
    rsumN n (fun i => rsumN m (fun j => f i j)) = rsumN m (fun j => rsumN n (fun i => f i j)) := by
  induction n with
  | zero =>
      simp [rsumN, list_sum_zero]
  | succ n ih =>
      rw [rsumN_succ]
      rw [ih]
      rw [show (fun j => rsumN (n+1) (fun i => f i j)) =
          (fun j => rsumN n (fun i => f i j) + f n j) by
        funext j; rw [rsumN_succ]]
      rw [rsumN_add]

private theorem rsumN_mul_left (n : Nat) (c : Nat) (f : Nat → Nat) :
    rsumN n (fun i => c * f i) = c * rsumN n f := by
  induction n with
  | zero => simp [rsumN]
  | succ n ih =>
      rw [rsumN_succ, rsumN_succ, ih, Nat.mul_add]

private theorem rsumN_mul_right (n : Nat) (f : Nat → Nat) (c : Nat) :
    rsumN n (fun i => f i * c) = rsumN n f * c := by
  induction n with
  | zero => simp [rsumN]
  | succ n ih =>
      rw [rsumN_succ, rsumN_succ, ih, Nat.add_mul]

private theorem rsumN_le (n : Nat) (f g : Nat → Nat) (h : ∀ i, i < n → f i ≤ g i) :
    rsumN n f ≤ rsumN n g := by
  induction n with
  | zero => simp [rsumN]
  | succ n ih =>
      rw [rsumN_succ, rsumN_succ]
      exact Nat.add_le_add (ih (fun i hi => h i (Nat.lt_succ_of_lt hi))) (h n (Nat.lt_succ_self n))

private theorem rsumN_prod (n m : Nat) (f g : Nat → Nat) :
    rsumN n (fun i => rsumN m (fun j => f i * g j)) = rsumN n f * rsumN m g := by
  calc
    rsumN n (fun i => rsumN m (fun j => f i * g j))
        = rsumN n (fun i => f i * rsumN m g) := by
            apply rsumN_congr
            intro i hi
            rw [rsumN_mul_left]
    _ = rsumN n f * rsumN m g := by rw [rsumN_mul_right]

private theorem le_rsumN (n x : Nat) (f : Nat → Nat) (hx : x < n) : f x ≤ rsumN n f := by
  induction n with
  | zero => omega
  | succ n ih =>
      rw [rsumN_succ]
      by_cases hxn : x == n
      · have hx' : x = n := Nat.eq_of_beq_eq_true hxn
        subst x
        omega
      · have hxne : x ≠ n := by intro he; subst x; simp at hxn
        have hx' : x < n := by omega
        exact Nat.le_trans (ih hx') (Nat.le_add_right _ _)

private def indN (db : DB) (r x y : Nat) : Nat := if db r x y == 0 then 0 else 1

private theorem countP_eq_sum {α} (p : α → Bool) (l : List α) :
    l.countP p = (l.map (fun x => if p x then 1 else 0)).sum := by
  induction l with
  | nil => simp
  | cons a l ih => by_cases h : p a = true <;> simp [h, ih] <;> omega

private theorem ind_count (db : DB) (r x y : Nat) :
    (if db r x y != 0 then 1 else 0) = (if db r x y == 0 then 0 else 1) := by
  by_cases hz : db r x y = 0 <;> simp [hz]

private theorem support_a0 (n : Nat) (db : DB) (b : Bind) :
    support n db (⟨0,0,1⟩ : Atom) pattern b = rsumN n (fun y => indN db 0 (b 0) y) := by
  simp [support, pattern, rsumN, countP_eq_sum, indN]

private theorem support_a1 (n : Nat) (db : DB) (b : Bind) :
    support n db (⟨1,1,2⟩ : Atom) pattern b = rsumN n (fun x => rsumN n (fun y => indN db 1 x y)) := by
  simp [support, pattern, rsumN, countP_eq_sum, indN]

private theorem support_a2 (n : Nat) (db : DB) (b : Bind) :
    support n db (⟨2,1,3⟩ : Atom) pattern b = rsumN n (fun x => rsumN n (fun y => indN db 2 x y)) := by
  simp [support, pattern, rsumN, countP_eq_sum, indN]

private theorem support_a3 (n : Nat) (db : DB) (b : Bind) :
    support n db (⟨3,0,4⟩ : Atom) pattern b = rsumN n (fun y => indN db 3 (b 0) y) := by
  simp [support, pattern, rsumN, countP_eq_sum, indN]

private theorem support_a4 (n : Nat) (db : DB) (b : Bind) :
    support n db (⟨3,4,5⟩ : Atom) pattern b = rsumN n (fun x => rsumN n (fun y => indN db 3 x y)) := by
  simp [support, pattern, rsumN, countP_eq_sum, indN]

private def rsumI (n : Nat) (f : Nat → Int) : Int := ((List.range n).map f).sum

private theorem int_sum_of_nat (l : List Nat) : ((l.sum : Nat) : Int) = (l.map (fun x : Nat => (x : Int))).sum := by
  induction l with
  | nil => simp
  | cons a l ih => simp [ih, Int.natCast_add]

private theorem rsumN_cast (n : Nat) (f : Nat → Nat) :
    ((rsumN n f : Nat) : Int) = rsumI n (fun i => (f i : Int)) := by
  simp [rsumN, rsumI, int_sum_of_nat, Function.comp_def]

private def refN (n : Nat) (db : DB) (b : Bind) : Nat :=
  rsumN n (fun x2 => rsumN n (fun x3 => rsumN n (fun x5 => rsumN n (fun x4 =>
    rsumN n (fun x1 => indN db 0 (b 0) x1 * indN db 1 x1 x2 * indN db 2 x1 x3 *
      indN db 3 (b 0) x4 * indN db 3 x4 x5)))))

private def refI (n : Nat) (db : DB) (b : Bind) : Int :=
  rsumI n (fun x2 => rsumI n (fun x3 => rsumI n (fun x5 => rsumI n (fun x4 =>
    rsumI n (fun x1 => (indN db 0 (b 0) x1 : Int) * (indN db 1 x1 x2 : Int) *
      (indN db 2 x1 x3 : Int) * (indN db 3 (b 0) x4 : Int) * (indN db 3 x4 x5 : Int))))))

private theorem refN_cast (n : Nat) (db : DB) (b : Bind) : (refN n db b : Int) = refI n db b := by
  simp [refN, refI, rsumN_cast, Int.natCast_mul]

private theorem reference_refI (n : Nat) (db : DB) (b : Bind) :
    reference pattern n (fun r x y => (indN db r x y : Int)) b = refI n db b := by
  simp [refI, reference, integrate, pattern, value, rsumI, indN, put, Int.mul_assoc]

private theorem reference_nat (n : Nat) (db : DB) (b : Bind) :
    reference pattern n (fun r x y => (indN db r x y : Int)) b = (refN n db b : Int) := by
  rw [reference_refI, ← refN_cast]

private theorem indN_le_one (db : DB) (r x y : Nat) : indN db r x y ≤ 1 := by
  by_cases h : db r x y == 0 <;> simp [indN, h]

private theorem ref_le_bound (n : Nat) (db : DB) (b : Bind) :
    refN n db b ≤
      rsumN n (fun x => rsumN n (fun y => indN db 1 x y)) *
      rsumN n (fun x => rsumN n (fun y => indN db 2 x y)) *
      rsumN n (fun x => rsumN n (fun y => indN db 3 x y)) := by
  let A : Nat → Nat := fun x => indN db 0 (b 0) x
  let B : Nat → Nat → Nat := fun x y => indN db 1 x y
  let C : Nat → Nat → Nat := fun x y => indN db 2 x y
  let D : Nat → Nat := fun x => indN db 3 (b 0) x
  let E : Nat → Nat → Nat := fun x y => indN db 3 x y
  let SB : Nat → Nat := fun x => rsumN n (fun y => B x y)
  let SC : Nat → Nat := fun x => rsumN n (fun y => C x y)
  let SE : Nat → Nat := fun x => rsumN n (fun y => E x y)
  let f : Nat → Nat → Nat → Nat → Nat → Nat := fun a b c d e => A a * B a b * C a c * D d * E d e
  let S1 : Nat := rsumN n (fun a => A a * SB a * SC a)
  let S2 : Nat := rsumN n (fun d => D d * SE d)
  have hS1 : S1 ≤ rsumN n SB * rsumN n SC := by
    calc
      S1 = rsumN n (fun a => A a * SB a * SC a) := rfl
      _ ≤ rsumN n (fun a => SB a * SC a) := by
            apply rsumN_le; intro a ha
            have hA : A a ≤ 1 := indN_le_one db 0 (b 0) a
            have h1 : A a * SB a ≤ 1 * SB a := Nat.mul_le_mul_right (SB a) hA
            exact Nat.le_trans (Nat.mul_le_mul_right (SC a) h1) (by simp)
      _ ≤ rsumN n (fun a => SB a * rsumN n SC) := by
            apply rsumN_le; intro a ha
            have hc : SC a ≤ rsumN n SC := le_rsumN n a SC ha
            exact Nat.mul_le_mul_left (SB a) hc
      _ = rsumN n SB * rsumN n SC := rsumN_mul_right n SB (rsumN n SC)
  have hS2 : S2 ≤ rsumN n SE := by
    calc
      S2 = rsumN n (fun d => D d * SE d) := rfl
      _ ≤ rsumN n (fun d => SE d) := by
            apply rsumN_le; intro d hd
            have hD : D d ≤ 1 := indN_le_one db 3 (b 0) d
            have h1 : D d * SE d ≤ 1 * SE d := Nat.mul_le_mul_right (SE d) hD
            simpa using h1
  have hprod : S1 * S2 =
      rsumN n (fun a => rsumN n (fun d => rsumN n (fun b => rsumN n (fun c =>
        rsumN n (fun e => f a b c d e))))) := by
    calc
      S1 * S2 = rsumN n (fun a => rsumN n (fun d => (A a * SB a * SC a) * (D d * SE d))) := by
        dsimp [S1, S2]
        rw [← rsumN_prod]
      _ = rsumN n (fun a => rsumN n (fun d =>
            rsumN n (fun b => rsumN n (fun c => rsumN n (fun e => f a b c d e))))) := by
        apply rsumN_congr; intro a ha
        apply rsumN_congr; intro d hd
        dsimp [SB, SC, SE, f]
        rw [show (A a * rsumN n (fun y => B a y) * rsumN n (fun y => C a y)) *
              (D d * rsumN n (fun y => E d y)) =
            (rsumN n (fun y => A a * B a y) * rsumN n (fun y => C a y)) *
              D d * rsumN n (fun y => E d y) by
              rw [rsumN_mul_left]; ac_rfl]
        rw [show rsumN n (fun y => A a * B a y) * rsumN n (fun y => C a y) =
            rsumN n (fun b => rsumN n (fun c => (A a * B a b) * C a c)) by
              rw [rsumN_prod]]
        rw [show (rsumN n (fun b => rsumN n (fun c => (A a * B a b) * C a c))) * D d =
            rsumN n (fun b => rsumN n (fun c => ((A a * B a b) * C a c) * D d)) by
              rw [← rsumN_mul_right]
              apply rsumN_congr; intro b hb
              rw [← rsumN_mul_right]]
        rw [show (rsumN n (fun b => rsumN n (fun c => ((A a * B a b) * C a c) * D d))) *
              rsumN n (fun e => E d e) =
            rsumN n (fun b => rsumN n (fun c => rsumN n (fun e =>
              ((A a * B a b) * C a c) * D d * E d e))) by
              rw [← rsumN_mul_right]
              apply rsumN_congr; intro b hb
              rw [← rsumN_mul_right]
              apply rsumN_congr; intro c hc
              rw [← rsumN_mul_left]]
  have hperm : rsumN n (fun a => rsumN n (fun d => rsumN n (fun b => rsumN n (fun c =>
        rsumN n (fun e => f a b c d e))))) =
      rsumN n (fun b => rsumN n (fun c => rsumN n (fun e => rsumN n (fun d =>
        rsumN n (fun a => f a b c d e))))) := by
    calc
      rsumN n (fun a => rsumN n (fun d => rsumN n (fun b => rsumN n (fun c =>
        rsumN n (fun e => f a b c d e)))))
          = rsumN n (fun d => rsumN n (fun a => rsumN n (fun b => rsumN n (fun c =>
              rsumN n (fun e => f a b c d e))))) := rsumN_comm n n (fun a d =>
                rsumN n (fun b => rsumN n (fun c => rsumN n (fun e => f a b c d e))))
      _ = rsumN n (fun d => rsumN n (fun b => rsumN n (fun a => rsumN n (fun c =>
              rsumN n (fun e => f a b c d e))))) := by
            apply rsumN_congr; intro d hd
            exact rsumN_comm n n (fun a b => rsumN n (fun c => rsumN n (fun e => f a b c d e)))
      _ = rsumN n (fun d => rsumN n (fun b => rsumN n (fun c => rsumN n (fun a =>
              rsumN n (fun e => f a b c d e))))) := by
            apply rsumN_congr; intro d hd; apply rsumN_congr; intro b hb
            exact rsumN_comm n n (fun a c => rsumN n (fun e => f a b c d e))
      _ = rsumN n (fun d => rsumN n (fun b => rsumN n (fun c => rsumN n (fun e =>
              rsumN n (fun a => f a b c d e))))) := by
            apply rsumN_congr; intro d hd; apply rsumN_congr; intro b hb
            apply rsumN_congr; intro c hc
            exact rsumN_comm n n (fun a e => f a b c d e)
      _ = rsumN n (fun b => rsumN n (fun d => rsumN n (fun c => rsumN n (fun e =>
              rsumN n (fun a => f a b c d e))))) := by
            exact rsumN_comm n n (fun d b => rsumN n (fun c => rsumN n (fun e =>
              rsumN n (fun a => f a b c d e))))
      _ = rsumN n (fun b => rsumN n (fun c => rsumN n (fun d => rsumN n (fun e =>
              rsumN n (fun a => f a b c d e))))) := by
            apply rsumN_congr; intro b hb
            exact rsumN_comm n n (fun d c => rsumN n (fun e => rsumN n (fun a => f a b c d e)))
      _ = rsumN n (fun b => rsumN n (fun c => rsumN n (fun e => rsumN n (fun d =>
              rsumN n (fun a => f a b c d e))))) := by
            apply rsumN_congr; intro b hb; apply rsumN_congr; intro c hc
            exact rsumN_comm n n (fun d e => rsumN n (fun a => f a b c d e))
  have hN_eq : refN n db b = S1 * S2 := by
    rw [hprod, hperm]
    dsimp [refN, A, B, C, D, E, f]
  rw [hN_eq]
  have hm := Nat.mul_le_mul hS1 hS2
  dsimp [SB, SC, SE] at hm
  have hassoc : rsumN n (fun x => rsumN n (fun y => indN db 1 x y)) *
      rsumN n (fun x => rsumN n (fun y => indN db 2 x y)) *
      rsumN n (fun x => rsumN n (fun y => indN db 3 x y)) =
    (rsumN n (fun x => rsumN n (fun y => indN db 1 x y)) *
      rsumN n (fun x => rsumN n (fun y => indN db 2 x y))) *
      rsumN n (fun x => rsumN n (fun y => indN db 3 x y)) := rfl
  rw [← hassoc] at hm
  exact hm

private theorem rsumN_eq_zero_of_all_zero {n : Nat} {f : Nat → Nat}
    (h : ∀ i, i < n → f i = 0) : rsumN n f = 0 := by
  rw [show rsumN n f = rsumN n (fun _ => 0) by
    apply rsumN_congr
    intro i hi
    simp [h i hi]]
  exact rsumN_zero n

private theorem refN_zero_of_s0 (n : Nat) (db : DB) (b : Bind)
    (hs : rsumN n (fun y => indN db 0 (b 0) y) = 0) : refN n db b = 0 := by
  have hA : ∀ x, x < n → indN db 0 (b 0) x = 0 := by
    intro x hx
    have hle := le_rsumN n x (fun y => indN db 0 (b 0) y) hx
    rw [hs] at hle
    omega
  unfold refN
  apply rsumN_eq_zero_of_all_zero
  intro x2 hx2
  apply rsumN_eq_zero_of_all_zero
  intro x3 hx3
  apply rsumN_eq_zero_of_all_zero
  intro x5 hx5
  apply rsumN_eq_zero_of_all_zero
  intro x4 hx4
  apply rsumN_eq_zero_of_all_zero
  intro x1 hx1
  simp [hA x1 hx1]

private theorem refN_zero_of_s3 (n : Nat) (db : DB) (b : Bind)
    (hs : rsumN n (fun y => indN db 3 (b 0) y) = 0) : refN n db b = 0 := by
  have hA : ∀ x, x < n → indN db 3 (b 0) x = 0 := by
    intro x hx
    have hle := le_rsumN n x (fun y => indN db 3 (b 0) y) hx
    rw [hs] at hle
    omega
  unfold refN
  apply rsumN_eq_zero_of_all_zero
  intro x2 hx2
  apply rsumN_eq_zero_of_all_zero
  intro x3 hx3
  apply rsumN_eq_zero_of_all_zero
  intro x5 hx5
  apply rsumN_eq_zero_of_all_zero
  intro x4 hx4
  simp [hA x4 hx4]
  exact rsumN_zero n

private theorem pow_le_pow_exp {a i j : Nat} (hi : 0 < i) (hij : i ≤ j) : a ^ i ≤ a ^ j := by
  by_cases ha : a = 0
  · subst a
    have hj : 0 < j := by omega
    rw [Nat.zero_pow hi, Nat.zero_pow hj]
    omega
  · exact Nat.pow_le_pow_right (Nat.pos_of_ne_zero ha) hij

private theorem exists_five (l : List Nat) (h : l.length = 5) :
    ∃ a b c d e, l = [a,b,c,d,e] := by
  match l with
  | [a,b,c,d,e] => exact ⟨a,b,c,d,e,rfl⟩
  | [] => simp at h
  | [a] => simp at h
  | [a,b] => simp at h
  | [a,b,c] => simp at h
  | [a,b,c,d] => simp at h
  | a::b::c::d::e::f::rest => simp at h

private theorem reference_nat_ind (n : Nat) (db : DB) (b : Bind) :
    reference pattern n (fun r x y => if db r x y == 0 then 0 else 1) b = (refN n db b : Int) := by
  have hf : (fun r x y => if db r x y == 0 then (0 : Int) else 1) =
      (fun r x y => (indN db r x y : Int)) := by
    funext r x y
    by_cases h : db r x y == 0 <;> simp [indN, h]
  rw [hf]
  exact reference_nat n db b

theorem sound_FractionalCap :
    TinylambOpt.ResearchGoals.Atlas.JoinPlans.VoteBadge.FractionalCap.Goal := by
  intro n db b weights den hcov
  have hden : 0 < den := hcov.1
  have hlen : weights.length = 5 := by
    simpa [pattern] using hcov.2.1
  rcases exists_five weights hlen with ⟨w0, w1, w2, w3, w4, rfl⟩
  have hc2 : den ≤ w1 := by
    have hh := hcov.2.2 2 (by simp [pattern])
    simpa [pattern] using hh
  have hc3 : den ≤ w2 := by
    have hh := hcov.2.2 3 (by simp [pattern])
    simpa [pattern] using hh
  have hc4 : den ≤ w4 := by
    have hh := hcov.2.2 5 (by simp [pattern])
    simpa [pattern] using hh
  let s0 := rsumN n (fun y => indN db 0 (b 0) y)
  let s1 := rsumN n (fun x => rsumN n (fun y => indN db 1 x y))
  let s2 := rsumN n (fun x => rsumN n (fun y => indN db 2 x y))
  let s3 := rsumN n (fun y => indN db 3 (b 0) y)
  let s4 := rsumN n (fun x => rsumN n (fun y => indN db 3 x y))
  have href : (reference pattern n (fun r x y => if db r x y == 0 then 0 else 1) b).toNat =
      refN n db b := by
    have hnon : 0 ≤ reference pattern n (fun r x y => if db r x y == 0 then 0 else 1) b := by
      rw [reference_nat_ind]
      exact Int.natCast_nonneg _
    have hc : ((reference pattern n (fun r x y => if db r x y == 0 then 0 else 1) b).toNat : Int) =
        (refN n db b : Int) := by
      calc
        ((reference pattern n (fun r x y => if db r x y == 0 then 0 else 1) b).toNat : Int) =
            reference pattern n (fun r x y => if db r x y == 0 then 0 else 1) b :=
              Int.toNat_of_nonneg hnon
        _ = (refN n db b : Int) := reference_nat_ind n db b
    exact Int.ofNat.inj hc
  have hbound := ref_le_bound n db b
  have hmain : refN n db b ^ den ≤
      s1 ^ w1 * s2 ^ w2 * s4 ^ w4 := by
    have hb := Nat.pow_le_pow_left hbound den
    rw [Nat.mul_pow, Nat.mul_pow] at hb
    have h1 : (rsumN n (fun x => rsumN n (fun y => indN db 1 x y))) ^ den ≤
        (rsumN n (fun x => rsumN n (fun y => indN db 1 x y))) ^ w1 :=
      pow_le_pow_exp hden hc2
    have h2 : (rsumN n (fun x => rsumN n (fun y => indN db 2 x y))) ^ den ≤
        (rsumN n (fun x => rsumN n (fun y => indN db 2 x y))) ^ w2 :=
      pow_le_pow_exp hden hc3
    have h4 : (rsumN n (fun x => rsumN n (fun y => indN db 3 x y))) ^ den ≤
        (rsumN n (fun x => rsumN n (fun y => indN db 3 x y))) ^ w4 :=
      pow_le_pow_exp hden hc4
    exact Nat.le_trans hb (Nat.mul_le_mul (Nat.mul_le_mul h1 h2) h4)
  have hrhs : refN n db b ^ den ≤
      (support n db (⟨0,0,1⟩ : Atom) pattern b ^ w0) *
      ((support n db (⟨1,1,2⟩ : Atom) pattern b ^ w1) *
      ((support n db (⟨2,1,3⟩ : Atom) pattern b ^ w2) *
      ((support n db (⟨3,0,4⟩ : Atom) pattern b ^ w3) *
      (support n db (⟨3,4,5⟩ : Atom) pattern b ^ w4)))) := by
    by_cases hs0 : s0 = 0
    · have hz := refN_zero_of_s0 n db b (by simpa [s0] using hs0)
      rw [hz, Nat.zero_pow hden]
      exact Nat.zero_le _
    · by_cases hs3 : s3 = 0
      · have hz := refN_zero_of_s3 n db b (by simpa [s3] using hs3)
        rw [hz, Nat.zero_pow hden]
        exact Nat.zero_le _
      · have hs0pos : 0 < s0 := Nat.pos_of_ne_zero hs0
        have hs3pos : 0 < s3 := Nat.pos_of_ne_zero hs3
        have hp0 : 1 ≤ s0 ^ w0 := Nat.one_le_pow w0 s0 hs0pos
        have hp3 : 1 ≤ s3 ^ w3 := Nat.one_le_pow w3 s3 hs3pos
        have htmp : s1 ^ w1 * s2 ^ w2 * s4 ^ w4 ≤
            s0 ^ w0 * (s1 ^ w1 * (s2 ^ w2 * (s3 ^ w3 * s4 ^ w4))) := by
          have hmid : s1 ^ w1 * s2 ^ w2 * s4 ^ w4 ≤
              (s0 ^ w0 * s1 ^ w1) * (s2 ^ w2 * (s3 ^ w3 * s4 ^ w4)) := by
            have hA : s1 ^ w1 * s2 ^ w2 * s4 ^ w4 ≤
                (s0 ^ w0 * s1 ^ w1) * s2 ^ w2 * s4 ^ w4 := by
              exact Nat.mul_le_mul_right (s4 ^ w4)
                (Nat.mul_le_mul_right (s2 ^ w2) (Nat.le_mul_of_pos_left (s1 ^ w1) hp0))
            have hB : (s0 ^ w0 * s1 ^ w1) * s2 ^ w2 * s4 ^ w4 ≤
                (s0 ^ w0 * s1 ^ w1) * s2 ^ w2 * (s3 ^ w3 * s4 ^ w4) := by
              exact Nat.mul_le_mul_left ((s0 ^ w0 * s1 ^ w1) * s2 ^ w2)
                (Nat.le_mul_of_pos_left (s4 ^ w4) hp3)
            exact Nat.le_trans hA (by simpa [Nat.mul_assoc] using hB)
          calc
            s1 ^ w1 * s2 ^ w2 * s4 ^ w4 ≤
                (s0 ^ w0 * s1 ^ w1) * (s2 ^ w2 * (s3 ^ w3 * s4 ^ w4)) := hmid
            _ = s0 ^ w0 * (s1 ^ w1 * (s2 ^ w2 * (s3 ^ w3 * s4 ^ w4))) := by ac_rfl
        have hchain := Nat.le_trans hmain htmp
        dsimp [s0, s1, s2, s3, s4] at hchain
        simpa [support_a0, support_a1, support_a2, support_a3, support_a4] using hchain
  rw [href]
  simpa [pattern] using hrhs

end TinylambOpt.ResearchProofs.Atlas.JoinPlans.VoteBadgeFractionalCap
