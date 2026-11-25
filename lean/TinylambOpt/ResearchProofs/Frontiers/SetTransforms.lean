import TinylambOpt.ResearchGoals.Frontiers.SetTransforms

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Frontiers.SetTransforms

open TinylambOpt.ResearchGoals.Frontiers.Contractions (total)
open TinylambOpt.ResearchGoals.Frontiers.SetTransforms

private theorem total_zero (f : Nat → Int) : total 0 f = 0 := by rfl

private theorem total_succ (n : Nat) (f : Nat → Int) :
    total (Nat.succ n) f = total n f + f n := by
  unfold total
  rw [List.range_succ, List.map_append, List.sum_append]
  simp

private theorem total_const_zero (n : Nat) : total n (fun _ => (0 : Int)) = 0 := by
  induction n with
  | zero => rfl
  | succ n ih => rw [total_succ, ih]; simp

private theorem total_add_split (n m : Nat) (f : Nat → Int) :
    total (n+m) f = total n f + total m (fun i => f (n+i)) := by
  unfold total
  rw [List.range_add, List.map_append, List.map_map, List.sum_append]
  rfl

private theorem total_congr {n : Nat} {f g : Nat → Int}
    (h : ∀ i, i < n → f i = g i) : total n f = total n g := by
  unfold total
  induction n with
  | zero => simp
  | succ n ih =>
      rw [List.range_succ, List.map_append, List.sum_append]
      simp only [List.map_singleton, List.sum_singleton]
      rw [ih (fun i hi => h i (Nat.lt_trans hi (Nat.lt_succ_self n))), h n (Nat.lt_succ_self n)]
      simp

private theorem total_eq_zero {n : Nat} {f : Nat → Int}
    (h : ∀ i, i < n → f i = 0) : total n f = 0 := by
  calc
    total n f = total n (fun _ => 0) := by
      apply total_congr
      intro i hi
      exact h i hi
    _ = 0 := total_const_zero n

private theorem total_total_zero (n m : Nat) :
    total n (fun _ => total m (fun _ => (0 : Int))) = 0 := by
  apply total_eq_zero
  intro i hi
  exact total_const_zero m

private theorem total_add (n : Nat) (f g : Nat → Int) :
    total n (fun i => f i + g i) = total n f + total n g := by
  induction n with
  | zero => simp [total]
  | succ n ih =>
      rw [total_succ, total_succ, total_succ, ih]
      grind

private theorem total_sub (n : Nat) (f g : Nat → Int) :
    total n (fun i => f i - g i) = total n f - total n g := by
  induction n with
  | zero => simp [total]
  | succ n ih =>
      rw [total_succ, total_succ, total_succ, ih]
      grind

private theorem total_mul_left (n : Nat) (c : Int) (f : Nat → Int) :
    total n (fun i => c * f i) = c * total n f := by
  induction n with
  | zero => simp [total]
  | succ n ih =>
      rw [total_succ, total_succ, ih]
      grind

private theorem total_mul_right (n : Nat) (f : Nat → Int) (c : Int) :
    total n (fun i => f i * c) = total n f * c := by
  induction n with
  | zero => simp [total]
  | succ n ih =>
      rw [total_succ, total_succ, ih]
      grind

private theorem total_neg (n : Nat) (f : Nat → Int) :
    total n (fun i => - f i) = - total n f := by
  simpa using total_mul_left n (-1) f

private theorem total_comm (n m : Nat) (f : Nat → Nat → Int) :
    total n (fun i => total m (fun j => f i j)) =
    total m (fun j => total n (fun i => f i j)) := by
  induction n with
  | zero =>
      symm
      apply total_eq_zero
      intro i hi
      rfl
  | succ n ih =>
      rw [total_succ]
      rw [ih]
      rw [← total_add]
      apply total_congr
      intro j hj
      rw [total_succ]

private theorem total_le {n} {f g : Nat → Int} (h : ∀ i, i < n → f i ≤ g i) :
    total n f ≤ total n g := by
  induction n with
  | zero => simp [total]
  | succ n ih =>
      rw [total_succ, total_succ]
      exact Int.add_le_add (ih (fun i hi => h i (Nat.lt_trans hi (Nat.lt_succ_self n))))
        (h n (Nat.lt_succ_self n))

private theorem total_eq_single (n x : Nat) (v : Int) (hx : x < n) :
    total n (fun z => if z == x then v else 0) = v := by
  induction n with
  | zero => omega
  | succ n ih =>
      rw [total_succ]
      by_cases hxn : x < n
      · rw [ih hxn]
        have hxne : n ≠ x := by omega
        simp [hxne]
      · have hxe : x = n := by omega
        subst x
        have hz : ∀ i, i < n → (if i == n then v else 0) = 0 := by
          intro i hi
          simp [Nat.ne_of_lt hi]
        rw [total_eq_zero hz]
        simp

private theorem beq_true_eq {a b : Nat} (h : (a == b) = true) : a = b := by
  rw [Nat.beq_eq_true_eq] at h
  exact h

private theorem land_pow_zero {d i : Nat} (hi : i < 2^d) : i &&& 2^d = 0 := by
  apply Nat.eq_of_testBit_eq
  intro t
  rw [Nat.testBit_and, Nat.testBit_two_pow]
  by_cases htd : d = t
  · subst t
    simp [Nat.testBit_lt_two_pow hi]
  · simp [htd]

private theorem testBit_lt_of_le {i d t : Nat} (hi : i < 2^d) (hdt : d ≤ t) :
    i.testBit t = false := by
  apply Nat.testBit_lt_two_pow
  have hp : 2^d ≤ 2^t := Nat.pow_le_pow_right (by omega) hdt
  omega

private theorem testBit_pow_add {d x t : Nat} (hx : x < 2^d) :
    (2^d + x).testBit t = if t < d then x.testBit t else decide (t = d) := by
  have h := Nat.testBit_two_pow_mul_add (a := 1) (b := x) (i := d) (by simpa using hx) t
  rw [Nat.mul_one] at h
  by_cases htd : t < d
  · simp [htd, h]
  · have hge : d ≤ t := Nat.le_of_not_lt htd
    by_cases hte : t = d
    · subst t
      simp [h]
    · have hsub : t - d ≠ 0 := by omega
      have hf : Nat.testBit 1 (t - d) = false := by
        have hiff := Nat.testBit_one_eq_true_iff_self_eq_zero (i := t - d)
        cases hb : Nat.testBit 1 (t - d) <;> simp [hb, hsub] at hiff ⊢
      simp [h, hte, hf]

private theorem sub_or_left (i j : Nat) : sub i (i ||| j) := by
  unfold sub
  rw [Nat.beq_eq_true_eq]
  apply Nat.eq_of_testBit_eq
  intro t
  cases hi : i.testBit t <;> cases hj : j.testBit t <;>
    simp [Nat.testBit_and, Nat.testBit_or, hi, hj]

private theorem sub_or_right (i j : Nat) : sub j (i ||| j) := by
  unfold sub
  rw [Nat.beq_eq_true_eq]
  apply Nat.eq_of_testBit_eq
  intro t
  cases hi : i.testBit t <;> cases hj : j.testBit t <;>
    simp [Nat.testBit_and, Nat.testBit_or, hi, hj]

private theorem sub_trans {a b c : Nat} (hab : sub a b) (hbc : sub b c) : sub a c := by
  unfold sub at hab hbc ⊢
  rw [Nat.beq_eq_true_eq] at hab hbc ⊢
  apply Nat.eq_of_testBit_eq
  intro t
  have h1 := congrArg (fun x : Nat => x.testBit t) hab
  have h2 := congrArg (fun x : Nat => x.testBit t) hbc
  simp [Nat.testBit_and] at h1 h2 ⊢
  cases ha : a.testBit t <;> cases hb : b.testBit t <;> cases hc : c.testBit t <;>
    simp [ha, hb, hc] at h1 h2 ⊢

private theorem sub_or_iff (i j k : Nat) : sub (i ||| j) k ↔ sub i k ∧ sub j k := by
  constructor
  · intro h
    exact ⟨sub_trans (sub_or_left i j) h, sub_trans (sub_or_right i j) h⟩
  · rintro ⟨hi, hj⟩
    unfold sub at hi hj ⊢
    rw [Nat.beq_eq_true_eq] at hi hj ⊢
    apply Nat.eq_of_testBit_eq
    intro t
    have h1 := congrArg (fun x : Nat => x.testBit t) hi
    have h2 := congrArg (fun x : Nat => x.testBit t) hj
    simp [Nat.testBit_and, Nat.testBit_or] at h1 h2 ⊢
    cases hti : i.testBit t <;> cases htj : j.testBit t <;> cases htk : k.testBit t <;>
      simp [hti, htj, htk] at h1 h2 ⊢

private theorem sub_high_false {d i k : Nat} (_hi : i < 2^d) (hk : k < 2^d) :
    ¬ sub (2^d + i) k := by
  intro h
  unfold sub at h
  have h' := beq_true_eq h
  have hle : (2^d + i) &&& k ≤ k := Nat.and_le_right
  omega

private theorem sub_low_add {d i k : Nat} (hi : i < 2^d) (hk : k < 2^d) :
    sub i k ↔ sub i (2^d + k) := by
  unfold sub
  rw [Nat.beq_eq_true_eq]
  rw [Nat.beq_eq_true_eq]
  constructor
  · intro h
    apply Nat.eq_of_testBit_eq
    intro t
    have ht := congrArg (fun x : Nat => x.testBit t) h
    simp [Nat.testBit_and] at ht ⊢
    by_cases htd : t < d
    · rw [testBit_pow_add hk]
      simp [htd]
      exact ht
    · have hit : i.testBit t = false := testBit_lt_of_le hi (Nat.le_of_not_lt htd)
      simp [hit]
  · intro h
    apply Nat.eq_of_testBit_eq
    intro t
    have ht := congrArg (fun x : Nat => x.testBit t) h
    simp [Nat.testBit_and] at ht ⊢
    by_cases htd : t < d
    · rw [testBit_pow_add hk] at ht
      simp [htd] at ht
      exact ht
    · have hit : i.testBit t = false := testBit_lt_of_le hi (Nat.le_of_not_lt htd)
      simp [hit]

private theorem sub_high_add {d i k : Nat} (hi : i < 2^d) (hk : k < 2^d) :
    sub (2^d + i) (2^d + k) ↔ sub i k := by
  unfold sub
  rw [Nat.beq_eq_true_eq]
  rw [Nat.beq_eq_true_eq]
  constructor
  · intro h
    apply Nat.eq_of_testBit_eq
    intro t
    have h1 := congrArg (fun x : Nat => x.testBit t) h
    simp [Nat.testBit_and] at h1
    by_cases htd : t < d
    · rw [testBit_pow_add hi, testBit_pow_add hk] at h1
      simpa [htd] using h1
    · have hit : i.testBit t = false := testBit_lt_of_le hi (Nat.le_of_not_lt htd)
      simp [hit]
  · intro h
    apply Nat.eq_of_testBit_eq
    intro t
    have ht := congrArg (fun x : Nat => x.testBit t) h
    simp [Nat.testBit_and] at ht
    rw [Nat.testBit_and]
    rw [testBit_pow_add hi, testBit_pow_add hk]
    by_cases htd : t < d
    · simp [htd]
      exact ht
    · have hit : i.testBit t = false := testBit_lt_of_le hi (Nat.le_of_not_lt htd)
      have hkt : k.testBit t = false := testBit_lt_of_le hk (Nat.le_of_not_lt htd)
      by_cases hte : t = d
      · subst t
        simp
      · simp [htd, hte]

private theorem below_low {d : Nat} {f : Hist} {k : Nat} (hk : k < 2^d) :
    below (d+1) f k = below d f k := by
  unfold below
  rw [Nat.two_pow_succ, total_add_split]
  have hz : total (2^d) (fun i => if sub (2^d + i) k then f (2^d + i) else 0) = 0 := by
    apply total_eq_zero
    intro i hi
    simp [sub_high_false hi hk]
  rw [hz]
  simp

private theorem below_high {d : Nat} {f : Hist} {k : Nat} (hk : k < 2^d) :
    below (d+1) f (2^d + k) = below d f k + below d (fun i => f (2^d + i)) k := by
  unfold below
  rw [Nat.two_pow_succ, total_add_split]
  congr 1
  · apply total_congr
    intro i hi
    simp [sub_low_add hi hk]
  · apply total_congr
    intro i hi
    simp [sub_high_add hi hk]

private theorem pow_add_eq_or {d x : Nat} (hx : x < 2^d) :
    2^d + x = 2^d ||| x := by
  have h := Nat.two_pow_add_eq_or_of_lt hx 1
  simpa using h

private theorem and_pow_add_right {d k i : Nat} (hk : k < 2^d) (hi : i < 2^d) :
    (2^d + k) &&& i = k &&& i := by
  rw [pow_add_eq_or hk, Nat.and_or_distrib_right, Nat.and_comm (2^d) i]
  rw [land_pow_zero hi]
  simp

private theorem and_pow_add_left {d k i : Nat} (hk : k < 2^d) (hi : i < 2^d) :
    k &&& (2^d + i) = k &&& i := by
  rw [pow_add_eq_or hi, Nat.and_or_distrib_left]
  rw [land_pow_zero hk]
  simp

private theorem and_pow_add_both {d k i : Nat} (hk : k < 2^d) (hi : i < 2^d) :
    (2^d + k) &&& (2^d + i) = 2^d + (k &&& i) := by
  rw [pow_add_eq_or hk, pow_add_eq_or hi, Nat.and_or_distrib_right]
  rw [Nat.and_or_distrib_left, Nat.and_or_distrib_left]
  rw [Nat.and_comm (2^d) i, land_pow_zero hi, land_pow_zero hk]
  have hki : k &&& i < 2^d := Nat.and_lt_two_pow k hi
  rw [pow_add_eq_or hki]
  simp

private theorem countP_range_succ (p : Nat → Bool) (d : Nat) :
    (List.range (d+1)).countP p = (List.range d).countP p + (if p d then 1 else 0) := by
  rw [show d + 1 = Nat.succ d by omega, List.range_succ, List.countP_append]
  simp [List.countP_cons]

private theorem countP_range_congr {d : Nat} {p q : Nat → Bool}
    (h : ∀ i, i < d → p i = q i) :
    (List.range d).countP p = (List.range d).countP q := by
  induction d with
  | zero => simp
  | succ d ih =>
      rw [countP_range_succ, countP_range_succ]
      rw [ih (fun i hi => h i (by omega)), h d (by omega)]

private theorem count_flip (c : Nat) :
    (if (c+1)%2 == 0 then (1:Int) else -1) = - (if c%2 == 0 then (1:Int) else -1) := by
  have hc : c % 2 = 0 ∨ c % 2 = 1 := by omega
  rcases hc with h0 | h1
  · have hn : (c+1)%2 ≠ 0 := by omega
    simp [h0, hn]
  · have hy : (c+1)%2 = 0 := by omega
    simp [h1, hy]

private theorem paritySign_congr {d x y : Nat}
    (h : ∀ i, i < d → x.testBit i = y.testBit i) : paritySign d x = paritySign d y := by
  unfold paritySign
  rw [countP_range_congr]
  intro i hi
  exact h i hi

private theorem paritySign_low {d k : Nat} (hk : k < 2^d) :
    paritySign (d+1) k = paritySign d k := by
  unfold paritySign
  rw [countP_range_succ]
  have hkd : k.testBit d = false := Nat.testBit_lt_two_pow hk
  simp [hkd]

private theorem paritySign_pow_add {d k : Nat} (hk : k < 2^d) :
    paritySign (d+1) (2^d + k) = - paritySign d k := by
  unfold paritySign
  rw [countP_range_succ]
  have hcnt : (List.range d).countP (fun i => (2^d + k).testBit i) =
      (List.range d).countP (fun i => k.testBit i) := by
    apply countP_range_congr
    intro i hi
    rw [testBit_pow_add hk]
    simp [hi]
  have hbitd : (2^d + k).testBit d = true := by
    rw [testBit_pow_add hk]
    simp
  rw [hcnt, hbitd]
  simp only [ite_true]
  exact count_flip ((List.range d).countP fun i => k.testBit i)

private theorem chi_low {d k i : Nat} (_hk : k < 2^d) (hi : i < 2^d) :
    chi (d+1) k i = chi d k i := by
  unfold chi
  have hki : k &&& i < 2^d := Nat.and_lt_two_pow k hi
  exact paritySign_low hki

private theorem chi_high_index {d k i : Nat} (hk : k < 2^d) (hi : i < 2^d) :
    chi (d+1) k (2^d + i) = chi d k i := by
  unfold chi
  rw [and_pow_add_left hk hi]
  have hki : k &&& i < 2^d := Nat.and_lt_two_pow k hi
  exact paritySign_low hki

private theorem chi_high_key_low_index {d k i : Nat} (hk : k < 2^d) (hi : i < 2^d) :
    chi (d+1) (2^d + k) i = chi d k i := by
  unfold chi
  rw [and_pow_add_right hk hi]
  have hki : k &&& i < 2^d := Nat.and_lt_two_pow k hi
  exact paritySign_low hki

private theorem chi_high_both {d k i : Nat} (hk : k < 2^d) (hi : i < 2^d) :
    chi (d+1) (2^d + k) (2^d + i) = - chi d k i := by
  unfold chi
  rw [and_pow_add_both hk hi]
  have hki : k &&& i < 2^d := Nat.and_lt_two_pow k hi
  rw [paritySign_pow_add hki]

private theorem spectrum_low {d : Nat} {f : Hist} {k : Nat} (hk : k < 2^d) :
    spectrum (d+1) f k = spectrum d f k + spectrum d (fun i => f (2^d + i)) k := by
  unfold spectrum
  rw [Nat.two_pow_succ, total_add_split]
  congr 1
  · apply total_congr
    intro i hi
    rw [chi_low hk hi]
  · apply total_congr
    intro i hi
    rw [chi_high_index hk hi]

private theorem spectrum_high {d : Nat} {f : Hist} {k : Nat} (hk : k < 2^d) :
    spectrum (d+1) f (2^d + k) = spectrum d f k - spectrum d (fun i => f (2^d + i)) k := by
  unfold spectrum
  rw [Nat.two_pow_succ, total_add_split]
  rw [← total_add, ← total_sub]
  apply total_congr
  intro i hi
  rw [chi_high_key_low_index hk hi, chi_high_both hk hi]
  grind

private theorem xor_pow_add_right {d k i : Nat} (hk : k < 2^d) (hi : i < 2^d) :
    (2^d + k) ^^^ i = 2^d + (k ^^^ i) := by
  apply Nat.eq_of_testBit_eq
  intro t
  rw [Nat.testBit_xor]
  rw [testBit_pow_add hk, testBit_pow_add (Nat.xor_lt_two_pow hk hi)]
  by_cases ht : t < d
  · simp [ht, Nat.testBit_xor]
  · have hit : i.testBit t = false := testBit_lt_of_le hi (Nat.le_of_not_lt ht)
    simp [ht, hit]

private theorem xor_pow_add_both {d k i : Nat} (hk : k < 2^d) (hi : i < 2^d) :
    (2^d + k) ^^^ (2^d + i) = k ^^^ i := by
  apply Nat.eq_of_testBit_eq
  intro t
  rw [Nat.testBit_xor, Nat.testBit_xor]
  rw [testBit_pow_add hk, testBit_pow_add hi]
  by_cases ht : t < d
  · simp [ht]
  · have hkt : k.testBit t = false := testBit_lt_of_le hk (Nat.le_of_not_lt ht)
    have hit : i.testBit t = false := testBit_lt_of_le hi (Nat.le_of_not_lt ht)
    by_cases hte : t = d
    · subst t
      simp [hkt, hit]
    · simp [ht, hte, hkt, hit]

private theorem sgn_mul (a b : Nat) :
    (if (a+b)%2 == 0 then (1:Int) else -1) =
    (if a%2 == 0 then (1:Int) else -1) * (if b%2 == 0 then (1:Int) else -1) := by
  have ha : a % 2 = 0 ∨ a % 2 = 1 := by omega
  have hb : b % 2 = 0 ∨ b % 2 = 1 := by omega
  rcases ha with ha | ha <;> rcases hb with hb | hb
  · have hs : (a+b)%2 = 0 := by omega
    simp [ha, hb, hs]
  · have hs : (a+b)%2 ≠ 0 := by omega
    simp [ha, hb, hs]
  · have hs : (a+b)%2 ≠ 0 := by omega
    simp [ha, hb, hs]
  · have hs : (a+b)%2 = 0 := by omega
    simp [ha, hb, hs]

private theorem countP_xor_mod_two (d x y : Nat) :
    ((List.range d).countP (fun i => (x ^^^ y).testBit i)) % 2 =
    ((List.range d).countP (fun i => x.testBit i) +
     (List.range d).countP (fun i => y.testBit i)) % 2 := by
  induction d with
  | zero => simp
  | succ d ih =>
      rw [countP_range_succ, countP_range_succ, countP_range_succ]
      have hcnt : (List.range d).countP (fun i => (x ^^^ y).testBit i) =
          (List.range d).countP (fun i => x.testBit i ^^ y.testBit i) := by
        apply countP_range_congr
        intro i hi
        rw [Nat.testBit_xor]
      rw [hcnt]
      simp only [Nat.testBit_xor]
      have hbit : (if (x.testBit d ^^ y.testBit d) then 1 else 0) =
          ((if x.testBit d then 1 else 0) + (if y.testBit d then 1 else 0)) % 2 := by
        cases hx : x.testBit d <;> cases hy : y.testBit d <;> simp
      rw [hbit]
      omega

private theorem paritySign_xor (d x y : Nat) :
    paritySign d (x ^^^ y) = paritySign d x * paritySign d y := by
  unfold paritySign
  have hmod := countP_xor_mod_two d x y
  rw [hmod]
  exact sgn_mul ((List.range d).countP fun i => x.testBit i)
                ((List.range d).countP fun i => y.testBit i)

private theorem chi_mul (d k i j : Nat) :
    chi d k (i ^^^ j) = chi d k i * chi d k j := by
  unfold chi
  have h : k &&& (i ^^^ j) = (k &&& i) ^^^ (k &&& j) :=
    Nat.and_xor_distrib_left (a := k) (b := i) (c := j)
  rw [h, paritySign_xor]

private theorem total_mul_total (n m : Nat) (f g : Nat → Int) :
    total n f * total m g = total n (fun i => total m (fun j => f i * g j)) := by
  induction n with
  | zero => simp [total]
  | succ n ih =>
      rw [total_succ, total_succ]
      rw [Int.add_mul, ih, ← total_mul_left]

private theorem sub_compl_iff {d j x : Nat} (hj : j < 2^d) (hx : x < 2^d) :
    sub j ((2^d - 1) ^^^ x) ↔ (x &&& j == 0) := by
  unfold sub
  rw [Nat.beq_eq_true_eq, Nat.beq_eq_true_eq]
  constructor
  · intro h
    apply Nat.eq_of_testBit_eq
    intro t
    rw [Nat.testBit_and]
    by_cases ht : t < d
    · have hb := congrArg (fun n : Nat => n.testBit t) h
      simp [Nat.testBit_and, Nat.testBit_xor, Nat.testBit_two_pow_sub_one] at hb
      cases hxt : x.testBit t <;> cases hjt : j.testBit t <;>
        simp [ht, hxt, hjt] at hb ⊢
    · have hjt : j.testBit t = false := testBit_lt_of_le hj (Nat.le_of_not_lt ht)
      simp [hjt]
  · intro h
    apply Nat.eq_of_testBit_eq
    intro t
    have hb := congrArg (fun n : Nat => n.testBit t) h
    simp [Nat.testBit_and] at hb
    by_cases ht : t < d
    · cases hxt : x.testBit t <;> cases hjt : j.testBit t <;>
        simp [ht, hxt, hjt, Nat.testBit_xor, Nat.testBit_two_pow_sub_one] at hb ⊢
    · have hjt : j.testBit t = false := testBit_lt_of_le hj (Nat.le_of_not_lt ht)
      have hmask : ((2^d - 1) ^^^ x).testBit t = false := by
        rw [Nat.testBit_xor, Nat.testBit_two_pow_sub_one]
        simp [ht, testBit_lt_of_le hx (Nat.le_of_not_lt ht)]
      simp [hjt, hmask]

private theorem countP_shift (p : Nat → Bool) (d : Nat) :
    (List.range (d+1)).countP p =
    (List.range d).countP (fun i => p (i+1)) + (if p 0 then 1 else 0) := by
  induction d with
  | zero => simp [List.countP_cons]
  | succ d ih =>
      rw [countP_range_succ, ih, countP_range_succ]
      grind

private theorem paritySign_double {d z : Nat} (_hz : z < 2^d) :
    paritySign (d+1) (2*z) = paritySign d z := by
  unfold paritySign
  rw [countP_shift]
  have h0 : (2*z).testBit 0 = false := by
    rw [Nat.testBit_zero]
    have heven : (2*z) % 2 = 0 := by omega
    simp [heven]
  rw [h0]
  simp
  have hcnt : (List.range d).countP (fun i => (2*z).testBit (i+1)) =
      (List.range d).countP (fun i => z.testBit i) := by
    apply countP_range_congr
    intro i hi
    rw [Nat.testBit_add_one]
    have hdiv : (2*z) / 2 = z := by omega
    rw [hdiv]
  rw [hcnt]

private theorem mul_two_and_mul_two (k j : Nat) :
    (2*k) &&& (2*j) = 2*(k &&& j) := by
  have h2k : 2*k = k <<< 1 := by
    rw [Nat.shiftLeft_eq]
    simp [Nat.mul_comm]
  have h2j : 2*j = j <<< 1 := by
    rw [Nat.shiftLeft_eq]
    simp [Nat.mul_comm]
  have h2kj : 2*(k &&& j) = (k &&& j) <<< 1 := by
    rw [Nat.shiftLeft_eq]
    simp [Nat.mul_comm]
  rw [h2k, h2j, h2kj, Nat.shiftLeft_and_distrib]

private theorem mul_two_and_odd (k j : Nat) :
    (2*k) &&& (2*j+1) = 2*(k &&& j) := by
  have h2k : 2*k = k <<< 1 := by
    rw [Nat.shiftLeft_eq]
    simp [Nat.mul_comm]
  have h2j : 2*j+1 = j <<< 1 ||| 1 := by
    have h := Nat.two_pow_add_eq_or_of_lt (i := 1) (b := 1) (by omega) j
    simp at h
    rw [h, show 2*j = j <<< 1 by rw [Nat.shiftLeft_eq]; simp [Nat.mul_comm]]
  have hzero : (2*k) &&& 1 = 0 := by
    rw [Nat.and_one_is_mod]
    have heven : (2*k) % 2 = 0 := by omega
    simp [heven]
  rw [h2j, Nat.and_or_distrib_left, hzero]
  simp
  rw [h2k]
  rw [← Nat.shiftLeft_and_distrib]
  rw [Nat.shiftLeft_eq]
  simp [Nat.mul_comm]

private theorem chi_even_even {d k j : Nat} (_hk : k < 2^d) (hj : j < 2^d) :
    chi (d+1) (2*k) (2*j) = chi d k j := by
  unfold chi
  rw [mul_two_and_mul_two]
  have hkj : k &&& j < 2^d := Nat.and_lt_two_pow k hj
  rw [paritySign_double hkj]

private theorem chi_even_odd {d k j : Nat} (_hk : k < 2^d) (hj : j < 2^d) :
    chi (d+1) (2*k) (2*j+1) = chi d k j := by
  unfold chi
  rw [mul_two_and_odd]
  have hkj : k &&& j < 2^d := Nat.and_lt_two_pow k hj
  rw [paritySign_double hkj]

private theorem total_double (n : Nat) (f : Nat → Int) :
    total (2*n) f = total n (fun i => f (2*i) + f (2*i+1)) := by
  induction n with
  | zero => rfl
  | succ n ih =>
      rw [show 2*(n+1) = Nat.succ (Nat.succ (2*n)) by omega]
      rw [total_succ, total_succ, ih]
      rw [total_succ]
      grind

private def evenHist (x : Nat) : Int := if paritySign 3 x == 1 then 1 else 0

private def oddHist (x : Nat) : Int := 1 - evenHist x

theorem sound_OrJoin :
    TinylambOpt.ResearchGoals.Frontiers.SetTransforms.OrJoin.Goal := by
  intro d a b k hk
  let N := 2^d
  unfold below
  unfold combine
  have hpush :
      total N (fun z => if sub z k then total N (fun i => total N (fun j =>
        if i ||| j == z then a i * b j else 0)) else 0) =
      total N (fun z => total N (fun i => total N (fun j =>
        if sub z k && (i ||| j == z) then a i * b j else 0))) := by
    apply total_congr
    intro z hz
    by_cases hsz : sub z k
    · simp [hsz]
    · simp [hsz, total_total_zero]
  rw [hpush]
  rw [total_comm]
  have hcomm2 :
      total N (fun i => total N (fun z => total N (fun j =>
        if sub z k && (i ||| j == z) then a i * b j else 0))) =
      total N (fun i => total N (fun j => total N (fun z =>
        if sub z k && (i ||| j == z) then a i * b j else 0))) := by
    apply total_congr
    intro i hi
    rw [total_comm]
  rw [hcomm2]
  have hcollapse :
      total N (fun i => total N (fun j => total N (fun z =>
        if sub z k && (i ||| j == z) then a i * b j else 0))) =
      total N (fun i => total N (fun j =>
        if sub (i ||| j) k then a i * b j else 0)) := by
    apply total_congr
    intro i hi
    apply total_congr
    intro j hj
    have hij : i ||| j < N := Nat.or_lt_two_pow hi hj
    calc
      total N (fun z => if sub z k && (i ||| j == z) then a i * b j else 0) =
          total N (fun z => if z == i ||| j then
            (if sub (i ||| j) k then a i * b j else 0) else 0) := by
        apply total_congr
        intro z hz
        by_cases heq : z = i ||| j
        · subst z
          simp
        · have heq' : ¬ i ||| j = z := by intro h; exact heq h.symm
          have hb : (i ||| j == z) = false := by
            simp [heq']
          have hb' : (z == i ||| j) = false := by
            simp [heq]
          simp [hb, hb']
      _ = if sub (i ||| j) k then a i * b j else 0 := total_eq_single N (i ||| j) _ hij
  rw [hcollapse]
  have hsplit :
      total N (fun i => total N (fun j =>
        if sub (i ||| j) k then a i * b j else 0)) =
      total N (fun i => if sub i k then
        a i * total N (fun j => if sub j k then b j else 0) else 0) := by
    apply total_congr
    intro i hi
    by_cases hik : sub i k
    · simp [hik]
      rw [← total_mul_left]
      apply total_congr
      intro j hj
      by_cases hjk : sub j k
      · have hor : sub (i ||| j) k := (sub_or_iff i j k).mpr ⟨hik, hjk⟩
        simp [hjk, hor]
      · have hor : ¬ sub (i ||| j) k := by
          intro hs
          exact hjk ((sub_or_iff i j k).mp hs).2
        simp [hjk, hor]
    · simp [hik]
      apply total_eq_zero
      intro j hj
      have hnot : ¬ sub (i ||| j) k := by
        intro hs
        exact hik ((sub_or_iff i j k).mp hs).1
      simp [hnot]
  rw [hsplit]
  have hfactor :
      total N (fun i => if sub i k then
        a i * total N (fun j => if sub j k then b j else 0) else 0) =
      total N (fun i => (if sub i k then a i else 0) *
        total N (fun j => if sub j k then b j else 0)) := by
    apply total_congr
    intro i hi
    by_cases hik : sub i k <;> simp [hik]
  rw [hfactor, total_mul_right]

theorem sound_RecoverCell :
    TinylambOpt.ResearchGoals.Frontiers.SetTransforms.RecoverCell.Goal := by
  intro d
  induction d with
  | zero =>
      intro f k hk
      have hk0 : k = 0 := by omega
      subst k
      simp [below, total, paritySign, sub]
  | succ d ih =>
      intro f k hk
      let N := 2^d
      have hpow : 2^(d+1) = N + N := by simpa [N] using Nat.two_pow_succ d
      by_cases hklow : k < N
      · have hmain :
            total N (fun i => if sub i k then paritySign (d+1) (k ^^^ i) * below (d+1) f i else 0) =
            total N (fun i => if sub i k then paritySign d (k ^^^ i) * below d f i else 0) := by
          apply total_congr
          intro i hi
          rw [below_low hi]
          have hki : k ^^^ i < N := by simpa [N] using Nat.xor_lt_two_pow hklow hi
          rw [paritySign_low (by simpa [N] using hki)]
        rw [hpow, total_add_split]
        have hz : total N (fun i =>
            if sub (N+i) k then paritySign (d+1) (k ^^^ (N+i)) * below (d+1) f (N+i) else 0) = 0 := by
          apply total_eq_zero
          intro i hi
          by_cases hsf : sub (N+i) k
          · exact False.elim (sub_high_false hi (by simpa [N] using hklow) hsf)
          · simp [hsf]
        rw [hz, hmain]
        simpa [N] using ih f k (by simpa [N] using hklow)
      · have hkge : N ≤ k := by omega
        let k' := k - N
        have hk'lt : k' < N := by omega
        have hkeq : k = N + k' := by omega
        rw [hkeq]
        have hlowsplit :
            total N (fun i => if sub i (N+k') then
              paritySign (d+1) ((N+k') ^^^ i) * below (d+1) f i else 0) =
            total N (fun i => if sub i k' then
              (-paritySign d (k' ^^^ i)) * below d f i else 0) := by
          apply total_congr
          intro i hi
          have hk'lt' : k' < 2^d := by simpa [N] using hk'lt
          have hi' : i < 2^d := by simpa [N] using hi
          have hsubiff : sub i k' ↔ sub i (N+k') := by
            simpa [N] using sub_low_add hi' hk'lt'
          by_cases hs : sub i k'
          · have hs' : sub i (N+k') := hsubiff.mp hs
            simp [hs, hs']
            rw [below_low hi']
            rw [xor_pow_add_right hk'lt' hi']
            have hki : k' ^^^ i < 2^d := Nat.xor_lt_two_pow hk'lt' hi'
            rw [paritySign_pow_add hki]
          · have hs' : ¬ sub i (N+k') := fun h => hs (hsubiff.mpr h)
            simp [hs, hs']
        have hhighsplit :
            total N (fun i => if sub (N+i) (N+k') then
              paritySign (d+1) ((N+k') ^^^ (N+i)) * below (d+1) f (N+i) else 0) =
            total N (fun i => if sub i k' then
              paritySign d (k' ^^^ i) * (below d f i + below d (fun j => f (N+j)) i) else 0) := by
          apply total_congr
          intro i hi
          have hk'lt' : k' < 2^d := by simpa [N] using hk'lt
          have hi' : i < 2^d := by simpa [N] using hi
          have hsubiff : sub (N+i) (N+k') ↔ sub i k' := by
            simpa [N] using sub_high_add hi' hk'lt'
          by_cases hs : sub i k'
          · have hs' : sub (N+i) (N+k') := hsubiff.mpr hs
            simp [hs, hs']
            rw [below_high hi']
            rw [xor_pow_add_both hk'lt' hi']
            have hki : k' ^^^ i < 2^d := Nat.xor_lt_two_pow hk'lt' hi'
            rw [paritySign_low hki]
          · have hs' : ¬ sub (N+i) (N+k') := fun h => hs (hsubiff.mp h)
            simp [hs, hs']
        rw [hpow, total_add_split, hlowsplit, hhighsplit]
        rw [← total_add]
        have hmerge :
            total N (fun i =>
              (if sub i k' then (-paritySign d (k' ^^^ i)) * below d f i else 0) +
              (if sub i k' then paritySign d (k' ^^^ i) *
                  (below d f i + below d (fun j => f (N+j)) i) else 0)) =
            total N (fun i => if sub i k' then
              (-paritySign d (k' ^^^ i)) * below d f i +
              paritySign d (k' ^^^ i) *
                  (below d f i + below d (fun j => f (N+j)) i) else 0) := by
          apply total_congr
          intro i hi
          by_cases hs : sub i k' <;> simp [hs]
        rw [hmerge]
        have hcancel :
            total N (fun i => if sub i k' then
              (-paritySign d (k' ^^^ i)) * below d f i +
              paritySign d (k' ^^^ i) *
                  (below d f i + below d (fun j => f (N+j)) i) else 0) =
            total N (fun i => if sub i k' then
              paritySign d (k' ^^^ i) * below d (fun j => f (N+j)) i else 0) := by
          apply total_congr
          intro i hi
          by_cases hs : sub i k'
          · simp [hs]
            grind
          · simp [hs]
        rw [hcancel]
        exact ih (fun i => f (N+i)) k' (by simpa [N] using hk'lt)

theorem sound_XorJoin :
    TinylambOpt.ResearchGoals.Frontiers.SetTransforms.XorJoin.Goal := by
  intro d a b k
  let N := 2^d
  unfold spectrum
  unfold combine
  have hpush :
      total N (fun x => chi d k x * total N (fun i => total N (fun j =>
        if i ^^^ j == x then a i * b j else 0))) =
      total N (fun x => total N (fun i => total N (fun j =>
        if i ^^^ j == x then chi d k x * (a i * b j) else 0))) := by
    apply total_congr
    intro x hx
    rw [← total_mul_left]
    apply total_congr
    intro i hi
    rw [← total_mul_left]
    apply total_congr
    intro j hj
    by_cases h : i ^^^ j == x <;> simp [h]
  rw [hpush]
  rw [total_comm]
  have hswap :
      total N (fun i => total N (fun x => total N (fun j =>
        if i ^^^ j == x then chi d k x * (a i * b j) else 0))) =
      total N (fun i => total N (fun j => total N (fun x =>
        if i ^^^ j == x then chi d k x * (a i * b j) else 0))) := by
    apply total_congr
    intro i hi
    rw [total_comm]
  rw [hswap]
  have hcollapse :
      total N (fun i => total N (fun j => total N (fun x =>
        if i ^^^ j == x then chi d k x * (a i * b j) else 0))) =
      total N (fun i => total N (fun j => chi d k (i ^^^ j) * (a i * b j))) := by
    apply total_congr
    intro i hi
    apply total_congr
    intro j hj
    have hxlt : i ^^^ j < N := Nat.xor_lt_two_pow hi hj
    have hs := total_eq_single N (i ^^^ j) (chi d k (i ^^^ j) * (a i * b j)) hxlt
    calc
      total N (fun x => if i ^^^ j == x then chi d k x * (a i * b j) else 0) =
          total N (fun x => if x == i ^^^ j then chi d k (i ^^^ j) * (a i * b j) else 0) := by
        apply total_congr
        intro x hx
        by_cases heq : x = i ^^^ j
        · subst x
          simp
        · have heq' : ¬ i ^^^ j = x := by intro h; exact heq h.symm
          have hb1 : (i ^^^ j == x) = false := by
            simp [heq']
          have hb2 : (x == i ^^^ j) = false := by
            simp [heq]
          simp [hb1, hb2]
      _ = chi d k (i ^^^ j) * (a i * b j) := hs
  rw [hcollapse]
  change total N (fun i => total N (fun j => chi d k (i ^^^ j) * (a i * b j))) =
      (total N (fun i => chi d k i * a i)) * (total N (fun i => chi d k i * b i))
  calc
    total N (fun i => total N (fun j => chi d k (i ^^^ j) * (a i * b j))) =
        total N (fun i => total N (fun j =>
          (chi d k i * a i) * (chi d k j * b j))) := by
      apply total_congr
      intro i hi
      apply total_congr
      intro j hj
      rw [chi_mul]
      grind
    _ = (total N (fun i => chi d k i * a i)) * (total N (fun i => chi d k i * b i)) := by
      rw [total_mul_total]

theorem sound_SpectralDot :
    TinylambOpt.ResearchGoals.Frontiers.SetTransforms.SpectralDot.Goal := by
  intro d
  induction d with
  | zero =>
      intro a b
      simp [total, spectrum, paritySign, chi]
  | succ d ih =>
      intro a b
      let N := 2^d
      have hlow : total N (fun k => spectrum (d+1) a k * spectrum (d+1) b k) =
          total N (fun k => (spectrum d a k + spectrum d (fun i => a (N+i)) k) *
                             (spectrum d b k + spectrum d (fun i => b (N+i)) k)) := by
        apply total_congr
        intro k hk
        rw [spectrum_low hk, spectrum_low hk]
      have hhigh : total N (fun k => spectrum (d+1) a (N+k) * spectrum (d+1) b (N+k)) =
          total N (fun k => (spectrum d a k - spectrum d (fun i => a (N+i)) k) *
                             (spectrum d b k - spectrum d (fun i => b (N+i)) k)) := by
        apply total_congr
        intro k hk
        rw [spectrum_high hk, spectrum_high hk]
      have hsum :
          total N (fun k => (spectrum d a k + spectrum d (fun i => a (N+i)) k) *
                             (spectrum d b k + spectrum d (fun i => b (N+i)) k)) +
          total N (fun k => (spectrum d a k - spectrum d (fun i => a (N+i)) k) *
                             (spectrum d b k - spectrum d (fun i => b (N+i)) k)) =
          2 * total N (fun k => spectrum d a k * spectrum d b k) +
          2 * total N (fun k => spectrum d (fun i => a (N+i)) k * spectrum d (fun i => b (N+i)) k) := by
        rw [← total_add, ← total_mul_left, ← total_mul_left, ← total_add]
        apply total_congr
        intro k hk
        grind
      rw [Nat.two_pow_succ]
      rw [total_add_split]
      rw [total_add_split]
      rw [hlow, hhigh, hsum]
      rw [← ih a b, ← ih (fun i => a (N+i)) (fun i => b (N+i))]
      grind

theorem sound_InvisibleRefresh :
    TinylambOpt.ResearchGoals.Frontiers.SetTransforms.InvisibleRefresh.Goal := by
  intro d query delta h
  let N := 2^d
  have hzero : total N (fun k => spectrum d query k * spectrum d delta k) = 0 := by
    apply total_eq_zero
    intro k hk
    rcases h k hk with hq | hd
    · simp [hq]
    · simp [hd]
  have hparseval := sound_SpectralDot d query delta
  rw [hzero] at hparseval
  have hNint : (0 : Int) < (N : Int) := by
    have hNnat : 0 < N := Nat.two_pow_pos d
    omega
  have hzero := Int.mul_eq_zero.mp hparseval
  rcases hzero with hzeroN | hzeroS
  · omega
  · exact hzeroS

theorem sound_TruncatedHaving :
    TinylambOpt.ResearchGoals.Frontiers.SetTransforms.TruncatedHaving.Goal := by
  intro d a b keep cap cut prod hcap hbound
  let N := 2^d
  have hQR :
      total N (fun k => if keep k then 0 else prod k) ≤
      total N (fun k => if keep k then 0 else (prod k).natAbs) := by
    apply total_le
    intro k hk
    by_cases hkkeep : keep k
    · simp [hkkeep]
    · simp [hkkeep]
      exact Int.le_natAbs
  have hRcap :
      total N (fun k => if keep k then 0 else (prod k).natAbs) ≤ (cap : Int) := by
    simpa [N] using hcap
  have hPQ :
      total N (fun k => if keep k then prod k else 0) +
      total N (fun k => if keep k then 0 else prod k) =
      (N : Int) * total N (fun i => a i * b i) := by
    have hpq :
        total N (fun k => if keep k then prod k else 0) +
        total N (fun k => if keep k then 0 else prod k) = total N prod := by
      rw [← total_add]
      apply total_congr
      intro k hk
      by_cases h : keep k <;> simp [h]
    rw [hpq]
    simpa [N, prod] using (sound_SpectralDot d a b).symm
  have hsmall :
      total N (fun k => if keep k then prod k else 0) +
      total N (fun k => if keep k then 0 else prod k) < (N : Int) * cut := by
    have hb' : total N (fun k => if keep k then prod k else 0) + (cap : Int) <
        (N : Int) * cut := by
      simpa [N] using hbound
    omega
  have hmul : (N : Int) * total N (fun i => a i * b i) < (N : Int) * cut := by
    rw [← hPQ]
    exact hsmall
  have hNnonneg : (0 : Int) ≤ (N : Int) := by
    have hNnat : 0 < N := Nat.two_pow_pos d
    omega
  have hfinal : total N (fun i => a i * b i) < cut :=
    Int.lt_of_mul_lt_mul_left hmul hNnonneg
  simpa [N] using hfinal

theorem sound_SplitButterfly :
    TinylambOpt.ResearchGoals.Frontiers.SetTransforms.SplitButterfly.Goal := by
  intro d f k hk
  dsimp only
  rw [spectrum_low hk, spectrum_high hk]

theorem sound_ProjectAxis :
    TinylambOpt.ResearchGoals.Frontiers.SetTransforms.ProjectAxis.Goal := by
  intro d f k hk
  unfold spectrum
  rw [show 2^(d+1) = 2 * 2^d by rw [Nat.two_pow_succ]; omega]
  rw [total_double]
  have hlow :
      total (2^d) (fun j => chi (d+1) (2*k) (2*j) * f (2*j)) =
      total (2^d) (fun j => chi d k j * f (2*j)) := by
    apply total_congr
    intro j hj
    rw [chi_even_even hk hj]
  have hhigh :
      total (2^d) (fun j => chi (d+1) (2*k) (2*j+1) * f (2*j+1)) =
      total (2^d) (fun j => chi d k j * f (2*j+1)) := by
    apply total_congr
    intro j hj
    rw [chi_even_odd hk hj]
  change total (2^d) (fun j => chi d k j * (f (2*j) + f (2*j+1))) =
      total (2^d) (fun i => chi (d+1) (2*k) (2*i) * f (2*i) +
        chi (d+1) (2*k) (2*i+1) * f (2*i+1))
  calc
    total (2^d) (fun j => chi d k j * (f (2*j) + f (2*j+1))) =
        total (2^d) (fun j => chi d k j * f (2*j)) +
        total (2^d) (fun j => chi d k j * f (2*j+1)) := by
      rw [← total_add]
      apply total_congr
      intro j hj
      rw [Int.mul_add]
    _ = total (2^d) (fun i => chi (d+1) (2*k) (2*i) * f (2*i)) +
        total (2^d) (fun i => chi (d+1) (2*k) (2*i+1) * f (2*i+1)) := by
      rw [hlow, hhigh]
    _ = total (2^d) (fun i => chi (d+1) (2*k) (2*i) * f (2*i) +
        chi (d+1) (2*k) (2*i+1) * f (2*i+1)) := by
      rw [← total_add]

/-- Counterexample for `PairStats`: with `a = evenHist`, `b = oddHist`
    (even/odd parity of the three bits), every two-attribute marginal agrees:
    for `m ∈ [3,5,6]` and `v < 8`, `marginal m v a = marginal m v b = 1`.
    But the full cell differs: `a 0 = 1` while `b 0 = 0`, contradicting
    the required `a 0 = b 0`. -/
theorem refutation_PairStats :
    ¬ TinylambOpt.ResearchGoals.Frontiers.SetTransforms.PairStats.Goal := by
  intro h
  have hprem : ∀ m ∈ [3,5,6], ∀ v < 8,
      marginal m v evenHist = marginal m v oddHist := by
    decide
  have h01 : evenHist 0 = oddHist 0 := h evenHist oddHist hprem 0 (by decide)
  have hneq : evenHist 0 ≠ oddHist 0 := by decide
  exact hneq h01

theorem sound_DisjointJoin :
    TinylambOpt.ResearchGoals.Frontiers.SetTransforms.DisjointJoin.Goal := by
  intro d a b
  let N := 2^d
  unfold combine
  unfold below
  have hrhs :
      total N (fun x => a x * total N (fun j =>
        if sub j ((N-1) ^^^ x) then b j else 0)) =
      total N (fun x => total N (fun j =>
        if x &&& j == 0 then a x * b j else 0)) := by
    apply total_congr
    intro x hx
    rw [← total_mul_left]
    apply total_congr
    intro j hj
    by_cases hcond : x &&& j == 0
    · have hs : sub j ((N-1) ^^^ x) := by
        simpa [N] using (sub_compl_iff hj (by simpa [N] using hx)).mpr hcond
      simp [hcond, hs]
    · have hs : ¬ sub j ((N-1) ^^^ x) := by
        intro hs
        apply hcond
        exact (sub_compl_iff hj (by simpa [N] using hx)).mp (by simpa [N] using hs)
      simp [hcond, hs]
  rw [hrhs]

end TinylambOpt.ResearchProofs.Frontiers.SetTransforms
