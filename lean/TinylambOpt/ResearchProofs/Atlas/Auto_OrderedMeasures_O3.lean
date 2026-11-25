import TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Auto_OrderedMeasures_O3

open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures
open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.TripleRise
open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.AlternatingBalance
open TinylambOpt.ResearchGoals.PairAggregates (choose2)

/-! Proofs for the eight assigned Atlas goals:
    TripleRise.MiddlePivot / StrictPage / StableExpand and
    AlternatingBalance.ParityCarry / BackwardPhase / PeriodicParity /
    DuplicateCancellation / ShiftResidue.
    Brute-force checks over all lists of length <= 4 over [-2,2] (and n <= 4)
    found no counterexample for any of the eight statements. -/

/-! ### Generic list helpers (TripleRise) -/

private theorem countP_flatMap {α β : Type} (f : α → List β) (P : β → Bool) (xs : List α) :
    (xs.flatMap f).countP P = (xs.map (fun a => (f a).countP P)).sum := by
  induction xs with
  | nil => rfl
  | cons a as ih => simp [ih]

private theorem countP_map_comp {α β : Type} (P : β → Bool) (g : α → β) (xs : List α) :
    (xs.map g).countP P = xs.countP (fun x => P (g x)) := by
  induction xs with
  | nil => rfl
  | cons x xs ih => by_cases h : P (g x) = true <;> simp [h, ih]

private theorem countP_false {α : Type} (l : List α) (p : α → Bool)
    (h : ∀ a ∈ l, p a = false) : l.countP p = 0 := by
  induction l with
  | nil => rfl
  | cons x xs ih =>
      simp only [List.countP_cons]
      rw [h x (by simp)]
      simp [ih (fun y hy => h y (by simp [hy]))]

private theorem sum_map_add {α : Type} (f g : α → Nat) (l : List α) :
    (l.map (fun x => f x + g x)).sum = (l.map f).sum + (l.map g).sum := by
  induction l with
  | nil => rfl
  | cons x xs ih => simp [ih, Nat.add_assoc, Nat.add_left_comm]

private theorem zipIdx_add {α : Type} (xs : List α) (n m : Nat) :
    xs.zipIdx (n + m) = (xs.zipIdx n).map (fun (v, i) => (v, i + m)) := by
  induction xs generalizing n with
  | nil => simp
  | cons a as ih =>
      simp only [List.zipIdx_cons, List.map_cons]
      congr 1
      simpa [Nat.add_assoc, Nat.add_comm, Nat.add_left_comm] using ih (n + 1)

private theorem countP_repeated (n : Nat) (a : Int) (Q : Int → Bool) :
    (repeated n a).countP Q = if Q a then n else 0 := by
  induction n with
  | zero => simp [repeated]
  | succ n ih =>
      change List.countP Q (a :: List.replicate n a) = if Q a then n + 1 else 0
      simp only [List.countP_cons]
      by_cases h : Q a = true
      · simp [h]
        simpa [repeated, h] using ih
      · simp [h]

private theorem sum_replicate_nat (n k : Nat) : (List.replicate n k).sum = n * k := by
  induction n with
  | zero => simp
  | succ n ih => rw [List.replicate_succ, List.sum_cons, ih]; grind

private theorem countP_flatMap_repeated (n : Nat) (xs : Series) (Q : Int → Bool) :
    (xs.flatMap (repeated n)).countP Q = n * xs.countP Q := by
  induction xs with
  | nil => simp
  | cons a xs ih =>
      simp only [List.flatMap_cons, List.countP_append]
      rw [countP_repeated, ih]
      by_cases h : Q a = true
      · simp [h]; grind
      · simp [h]

/-! ### Pair / triple enumeration helpers -/

private theorem pairs_cons (a : Int) (xs : Series) :
    pairs (a :: xs) = xs.map (fun b => (a, b)) ++ pairs xs := by
  unfold pairs
  rw [List.zipIdx_cons]
  simp only [List.flatMap_cons, List.drop]
  rw [show (1 : Nat) = 0 + 1 by rfl, zipIdx_add]
  rw [List.flatMap_map]

private theorem cart_cons (a : Int) (xs y : Series) :
    cart (a :: xs) y = y.map (fun b => (a, b)) ++ cart xs y := by
  simp [cart]

private theorem pairs_countP_append (P : Int × Int → Bool) (x y : Series) :
    (pairs (x ++ y)).countP P =
      (pairs x).countP P + (pairs y).countP P + (cart x y).countP P := by
  induction x with
  | nil => simp [pairs, cart]
  | cons a xs ih =>
      rw [List.cons_append, pairs_cons, pairs_cons, cart_cons]
      simp only [List.countP_append]
      rw [List.map_append, List.countP_append]
      rw [countP_map_comp, countP_map_comp]
      rw [ih]
      omega

private theorem triples_cons (a : Int) (xs : Series) :
    triples (a :: xs) = (pairs xs).map (fun (b, c) => (a, b, c)) ++ triples xs := by
  unfold triples
  rw [List.zipIdx_cons]
  simp only [List.flatMap_cons, List.drop]
  rw [show (1 : Nat) = 0 + 1 by rfl, zipIdx_add]
  rw [List.flatMap_map]

private theorem pairs_countP_eq (xs : Series) (P : Int × Int → Bool) :
    (pairs xs).countP P =
      (xs.zipIdx.map (fun (b, j) => (xs.drop (j + 1)).countP (fun c => P (b, c)))).sum := by
  unfold pairs
  rw [countP_flatMap]
  apply congrArg List.sum
  apply List.map_congr_left
  intro p hp
  rcases p with ⟨b, j⟩
  exact countP_map_comp P (fun c => (b, c)) (xs.drop (j + 1))

private theorem pairs_fst_mem {xs : Series} {p : Int × Int} (h : p ∈ pairs xs) : p.1 ∈ xs := by
  induction xs with
  | nil => simp [pairs] at h
  | cons a ys ih =>
      rw [pairs_cons] at h
      rcases List.mem_append.mp h with h | h
      · rcases List.mem_map.mp h with ⟨b, hb, hp⟩
        rw [← hp]
        simp
      · exact List.mem_cons_of_mem a (ih h)

private theorem pairs_mem_rising (xs : Series) (h : xs.Pairwise (· < ·)) :
    ∀ p ∈ pairs xs, rising p = true := by
  induction xs with
  | nil => simp [pairs]
  | cons a ys ih =>
      intro p hp
      rw [List.pairwise_cons] at h
      rw [pairs_cons] at hp
      rcases List.mem_append.mp hp with hp | hp
      · rcases List.mem_map.mp hp with ⟨b, hb, rfl⟩
        exact decide_eq_true (h.1 b hb)
      · exact ih h.2 p hp

/-! ### Binomial coefficient recurrences -/

private theorem choose2_step (n : Nat) :
    choose2 (n + 2) = choose2 (n + 1) + (n + 1) := by
  unfold choose2
  simp only [show n + 2 - 1 = n + 1 by omega, show n + 1 - 1 = n by omega]
  rw [show (n + 2) * (n + 1) = (n + 1) * n + 2 * (n + 1) by grind,
    Nat.mul_comm 2 (n + 1), Nat.add_mul_div_right _ _ (by decide)]

private theorem choose2_succ (n : Nat) : choose2 (n + 1) = choose2 n + n := by
  cases n with
  | zero => rfl
  | succ n => exact choose2_step n

private theorem pairs_length (x : Series) : (pairs x).length = choose2 x.length := by
  induction x with
  | nil => rfl
  | cons a xs ih =>
      rw [pairs_cons, List.length_append, List.length_map, ih]
      simp only [List.length_cons]
      rw [choose2_succ]
      omega

private theorem choose3_step (n : Nat) :
    choose3 (n + 3) = choose3 (n + 2) + choose2 (n + 2) := by
  unfold choose3 choose2
  simp only [show n + 3 - 1 = n + 2 by omega,
    show n + 3 - 2 = n + 1 by omega, show n + 2 - 1 = n + 1 by omega,
    show n + 2 - 2 = n by omega]
  have hb : 2 ∣ (n + 2) * (n + 1) := by
    rcases Nat.mod_two_eq_zero_or_one n with h | h
    · have hd : 2 ∣ n + 2 := by omega
      exact Nat.dvd_mul_right_of_dvd hd (n + 1)
    · have hd : 2 ∣ n + 1 := by omega
      exact Nat.dvd_mul_left_of_dvd hd (n + 2)
  have h2 : 2 ∣ (n + 2) * (n + 1) * n := Nat.dvd_mul_right_of_dvd hb n
  have h3 : 3 ∣ (n + 2) * (n + 1) * n := by
    have hm := Nat.mod_lt n (by decide : 0 < 3)
    rcases (show n % 3 = 0 ∨ n % 3 = 1 ∨ n % 3 = 2 by omega) with h | h | h
    · have hd : 3 ∣ n := Nat.dvd_of_mod_eq_zero h
      simpa [Nat.mul_assoc, Nat.mul_comm] using
        Nat.dvd_mul_right_of_dvd hd ((n + 2) * (n + 1))
    · have hd : 3 ∣ n + 2 := by
        apply Nat.dvd_iff_mod_eq_zero.mpr
        omega
      simpa [Nat.mul_assoc, Nat.mul_comm] using
        Nat.dvd_mul_right_of_dvd hd ((n + 1) * n)
    · have hd : 3 ∣ n + 1 := by
        apply Nat.dvd_iff_mod_eq_zero.mpr
        omega
      simpa [Nat.mul_assoc, Nat.mul_comm, Nat.mul_left_comm] using
        Nat.dvd_mul_left_of_dvd hd ((n + 2) * n)
  have hc : 6 ∣ (n + 2) * (n + 1) * n :=
    (by decide : Nat.Coprime 2 3).mul_dvd_of_dvd_of_dvd h2 h3
  have h3B : 6 ∣ 3 * ((n + 2) * (n + 1)) := by
    rcases hb with ⟨k, hk⟩
    refine ⟨k, ?_⟩
    rw [hk]
    simp [Nat.mul_assoc, Nat.mul_comm]
  have hAC : (n + 3) * (n + 2) * (n + 1) =
      (n + 2) * (n + 1) * n + 3 * ((n + 2) * (n + 1)) := by grind
  rw [hAC, Nat.add_div (by decide),
    Nat.mod_eq_zero_of_dvd hc, Nat.mod_eq_zero_of_dvd h3B]
  simp only [Nat.add_zero]
  rcases hb with ⟨k, hk⟩
  rw [hk]
  simp [Nat.mul_assoc, Nat.mul_comm]

private theorem choose3_succ (n : Nat) : choose3 (n + 1) = choose3 n + choose2 n := by
  cases n with
  | zero => rfl
  | succ n =>
      cases n with
      | zero => rfl
      | succ n => exact choose3_step n

/-! ### MiddlePivot -/

private def midSum (x : Series) : Nat :=
  (x.zipIdx.map (fun (v, i) =>
    (x.take i).countP (fun a => a < v) * (x.drop (i + 1)).countP (fun c => v < c))).sum

private theorem natCast_sum_map {α : Type} (f : α → Nat) (l : List α) :
    ((l.map f).sum : Int) = (l.map (fun a => (f a : Int))).sum := by
  induction l with
  | nil => rfl
  | cons a as ih => simp [ih]

private theorem natCast_prod_sum_map {α : Type} (f g : α → Nat) (l : List α) :
    ((l.map (fun a => f a * g a)).sum : Int) =
      (l.map (fun a => (f a : Int) * (g a : Int))).sum := by
  induction l with
  | nil => rfl
  | cons a as ih => simp [ih, Int.natCast_mul]

private def measureNat (x : Series) : Nat := (triples x).countP strict3

private theorem midSum_cons (a : Int) (xs : Series) :
    midSum (a :: xs) =
      (xs.zipIdx.map (fun (v, j) =>
        (if a < v then 1 else 0) * (xs.drop (j + 1)).countP (fun c => v < c))).sum +
        midSum xs := by
  unfold midSum
  rw [List.zipIdx_cons]
  simp only [List.map_cons, List.sum_cons, List.take_zero, List.countP_nil,
    Nat.zero_mul, Nat.zero_add]
  rw [show (1 : Nat) = 0 + 1 by rfl, zipIdx_add, List.map_map]
  simp only [Function.comp_def, Nat.zero_add]
  simp only [List.take_succ_cons, List.drop_succ_cons, Nat.add_assoc]
  simp only [List.countP_cons, Nat.add_mul, sum_map_add]
  simp
  omega

private theorem pairs_countP_left (a : Int) (xs : Series) :
    (pairs xs).countP (fun (b, c) => a < b && b < c) =
      (xs.zipIdx.map (fun (v, j) =>
        (if a < v then 1 else 0) * (xs.drop (j + 1)).countP (fun c => v < c))).sum := by
  rw [pairs_countP_eq]
  apply congrArg List.sum
  apply List.map_congr_left
  intro p hp
  rcases p with ⟨v, j⟩
  by_cases h : a < v
  · simp [h]
  · simp [h]

private theorem measureNat_cons (a : Int) (xs : Series) :
    measureNat (a :: xs) =
      (pairs xs).countP (fun (b, c) => a < b && b < c) + measureNat xs := by
  unfold measureNat
  rw [triples_cons, List.countP_append, countP_map_comp]
  rfl

private theorem measureNat_eq_midSum (x : Series) : measureNat x = midSum x := by
  induction x with
  | nil => rfl
  | cons a xs ih =>
      rw [measureNat_cons, ih, pairs_countP_left, midSum_cons]

theorem sound_TripleRise_MiddlePivot_O3 :
    TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.TripleRise.MiddlePivot.Goal := by
  intro x
  unfold TripleRise.measure
  rw [← natCast_prod_sum_map
    (fun p : Int × Nat => (x.take p.2).countP (fun a => a < p.1))
    (fun p : Int × Nat => (x.drop (p.2 + 1)).countP (fun c => p.1 < c)) x.zipIdx]
  exact_mod_cast measureNat_eq_midSum x

/-! ### StrictPage -/

private theorem pairs_countP_rising_pairwise (xs : Series) (h : xs.Pairwise (· < ·)) :
    (pairs xs).countP rising = choose2 xs.length := by
  rw [← pairs_length xs]
  rw [List.countP_eq_length_filter]
  apply congrArg List.length
  apply List.filter_eq_self.mpr
  intro p hp
  exact pairs_mem_rising xs h p hp

theorem sound_TripleRise_StrictPage_O3 :
    TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.TripleRise.StrictPage.Goal := by
  intro x hx
  induction x with
  | nil => simp [TripleRise.measure, triples, choose3]
  | cons a xs ih =>
      rw [List.pairwise_cons] at hx
      have hxs : xs.Pairwise (· < ·) := hx.2
      have ha : ∀ b ∈ xs, a < b := hx.1
      have hfirst : (pairs xs).countP (fun p : Int × Int => a < p.1 && p.1 < p.2) =
          (pairs xs).countP rising := by
        rw [List.countP_eq_length_filter, List.countP_eq_length_filter]
        apply congrArg List.length
        apply List.filter_congr
        intro p hp
        rcases p with ⟨b, c⟩
        have hbmem : b ∈ xs := pairs_fst_mem hp
        have hab : a < b := ha b hbmem
        simp [rising, hab]
      have hm : measureNat (a :: xs) = (pairs xs).countP rising + measureNat xs := by
        rw [measureNat_cons, hfirst]
      have ih' : (measureNat xs : Int) = (choose3 xs.length : Int) := by
        simpa [TripleRise.measure, measureNat] using ih hxs
      change (measureNat (a :: xs) : Int) = (choose3 (xs.length + 1) : Int)
      rw [hm, Int.natCast_add, ih', pairs_countP_rising_pairwise xs hxs]
      have hc : (choose3 (xs.length + 1) : Int) =
          (choose3 xs.length : Int) + (choose2 xs.length : Int) := by
        exact_mod_cast (choose3_succ xs.length)
      rw [hc]
      grind

/-! ### StableExpand -/

private theorem cart_countP (x y : Series) (P : Int × Int → Bool) :
    (cart x y).countP P = (x.map (fun a => y.countP (fun b => P (a, b)))).sum := by
  induction x with
  | nil => simp [cart]
  | cons a xs ih =>
      rw [show cart (a :: xs) y = y.map (fun b => (a, b)) ++ cart xs y by simp [cart]]
      rw [List.countP_append, countP_map_comp, ih]
      simp only [List.map_cons, List.sum_cons]

private theorem cart_countP_repeated (n : Nat) (a : Int) (z : Series) (P : Int × Int → Bool) :
    (cart (repeated n a) z).countP P = n * z.countP (fun b => P (a, b)) := by
  rw [cart_countP]
  rw [repeated, List.map_replicate, sum_replicate_nat]

private theorem pairs_repeated_countP (n : Nat) (a : Int) (P : Int × Int → Bool)
    (hdiag : P (a, a) = false) : (pairs (repeated n a)).countP P = 0 := by
  induction n with
  | zero => simp [repeated, pairs]
  | succ n ih =>
      rw [show repeated (n + 1) a = a :: repeated n a by
        change List.replicate (n + 1) a = a :: List.replicate n a
        rw [List.replicate_succ]]
      rw [pairs_cons, List.countP_append]
      have hmap : ((repeated n a).map (fun b => (a, b))).countP P = 0 := by
        rw [countP_map_comp, countP_repeated, hdiag]
        simp
      rw [hmap, Nat.zero_add, ih]

private theorem pairs_expand_countP (n : Nat) (xs : Series) (P : Int × Int → Bool)
    (hdiag : ∀ v, P (v, v) = false) :
    (pairs (xs.flatMap (repeated n))).countP P = n * n * (pairs xs).countP P := by
  induction xs with
  | nil => simp [pairs]
  | cons a xs ih =>
      rw [show (a :: xs).flatMap (repeated n) = repeated n a ++ xs.flatMap (repeated n) by rfl]
      rw [pairs_countP_append]
      rw [ih]
      rw [pairs_repeated_countP n a P (hdiag a)]
      rw [cart_countP_repeated n a (xs.flatMap (repeated n)) P]
      rw [countP_flatMap_repeated n xs (fun b => P (a, b))]
      simp only [Nat.zero_add]
      rw [pairs_cons, List.countP_append, countP_map_comp]
      grind

private theorem pairs_prepend_block_countP (n : Nat) (a : Int) (z : Series) :
    (pairs (repeated n a ++ z)).countP (fun (b, c) => a < b && b < c) =
      (pairs z).countP (fun (b, c) => a < b && b < c) := by
  induction n with
  | zero => simp [repeated]
  | succ n ih =>
      rw [show repeated (n + 1) a ++ z = a :: (repeated n a ++ z) by
        change List.replicate (n + 1) a ++ z = a :: (List.replicate n a ++ z)
        rw [List.replicate_succ]
        simp]
      rw [pairs_cons, List.countP_append]
      have hmap : ((repeated n a ++ z).map (fun b => (a, b))).countP
          (fun (b, c) => a < b && b < c) = 0 := by
        rw [countP_map_comp]
        apply countP_false
        intro b hb
        simp
      rw [hmap, Nat.zero_add, ih]

private theorem prepend_block_measureNat (n : Nat) (a : Int) (z : Series) :
    measureNat (repeated n a ++ z) =
      n * (pairs z).countP (fun (b, c) => a < b && b < c) + measureNat z := by
  induction n with
  | zero => simp [repeated, measureNat]
  | succ n ih =>
      rw [show repeated (n + 1) a ++ z = a :: (repeated n a ++ z) by
        change List.replicate (n + 1) a ++ z = a :: (List.replicate n a ++ z)
        rw [List.replicate_succ]
        simp]
      rw [measureNat_cons]
      change (pairs (repeated n a ++ z)).countP (fun (b, c) => a < b && b < c) +
          measureNat (repeated n a ++ z) =
        (n + 1) * (pairs z).countP (fun (b, c) => a < b && b < c) + measureNat z
      rw [pairs_prepend_block_countP, ih]
      grind

private theorem measureNat_expand (x : Series) (n : Nat) :
    measureNat (x.flatMap (repeated n)) = n ^ 3 * measureNat x := by
  induction x with
  | nil => simp [measureNat, triples]
  | cons a xs ih =>
      rw [show (a :: xs).flatMap (repeated n) = repeated n a ++ xs.flatMap (repeated n) by rfl]
      rw [prepend_block_measureNat]
      rw [pairs_expand_countP n xs (fun (b, c) => a < b && b < c) (by intro v; simp)]
      rw [ih]
      rw [measureNat_cons]
      grind

theorem sound_TripleRise_StableExpand_O3 :
    TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.TripleRise.StableExpand.Goal := by
  intro x n
  unfold TripleRise.measure
  change (measureNat (x.flatMap (repeated n)) : Int) = (n : Int) ^ 3 * (measureNat x : Int)
  rw [measureNat_expand]
  rw [Int.natCast_mul, Int.natCast_pow]


/-! ### Alternating-balance helpers and goals (AlternatingBalance) -/

private def alt (k : Nat) (x : Series) : Int :=
  ((x.zipIdx k).map (fun (v, i) => (-1 : Int) ^ i * v)).sum

private theorem measure_alt (x : Series) : AlternatingBalance.measure x = alt 0 x := by
  rfl

private theorem alt_cons (k : Nat) (a : Int) (xs : Series) :
    alt k (a :: xs) = (-1 : Int) ^ k * a + alt (k + 1) xs := by
  simp [alt, List.zipIdx_cons]

private theorem neg_one_pow_succ (n : Nat) : (-1 : Int) ^ (n + 1) = -((-1 : Int) ^ n) := by
  rw [Int.pow_succ]
  omega

private theorem alt_shift (k : Nat) (x : Series) : alt k x = (-1 : Int) ^ k * alt 0 x := by
  induction x generalizing k with
  | nil => simp [alt]
  | cons a xs ih =>
      rw [alt_cons, ih (k + 1)]
      rw [alt_cons, ih 1]
      rw [neg_one_pow_succ]
      simp only [Int.pow_zero]
      grind

private theorem alt_append (k : Nat) (x y : Series) :
    alt k (x ++ y) = alt k x + alt (k + x.length) y := by
  induction x generalizing k with
  | nil => simp [alt]
  | cons a xs ih =>
      rw [List.cons_append, alt_cons, alt_cons, ih (k + 1)]
      rw [show k + 1 + xs.length = k + (xs.length + 1) by omega]
      simp only [List.length_cons]
      omega

private theorem measure_cons (a : Int) (xs : Series) :
    AlternatingBalance.measure (a :: xs) = a - AlternatingBalance.measure xs := by
  rw [measure_alt, measure_alt, alt_cons, alt_shift 1]
  rw [neg_one_pow_succ]
  simp only [Int.pow_zero]
  grind

private theorem neg_one_pow_even (k : Nat) : (-1 : Int) ^ (2 * k) = 1 := by
  induction k with
  | zero => decide
  | succ k ih =>
      rw [show 2 * (k + 1) = 2 * k + 2 by omega]
      rw [Int.pow_add, ih]
      rw [show (-1 : Int) ^ 2 = 1 by decide]
      omega

private theorem neg_one_pow_odd (k : Nat) : (-1 : Int) ^ (2 * k + 1) = -1 := by
  rw [Int.pow_succ, neg_one_pow_even]
  omega

private theorem neg_one_pow_mod (n : Nat) :
    (-1 : Int) ^ n = if n % 2 == 0 then 1 else -1 := by
  by_cases h : n % 2 == 0
  · have h0 : n % 2 = 0 := by simpa using h
    have hm := Nat.mod_add_div n 2
    have hn : n = 2 * (n / 2) := by omega
    rw [ite_eq_left h, hn]
    exact neg_one_pow_even (n / 2)
  · have hlt : n % 2 < 2 := Nat.mod_lt _ (by decide)
    have hn0 : ¬ n % 2 = 0 := by simpa using h
    have h0 : n % 2 = 1 := by omega
    have hm := Nat.mod_add_div n 2
    have hn : n = 2 * (n / 2) + 1 := by omega
    rw [ite_eq_right h, hn]
    exact neg_one_pow_odd (n / 2)

private theorem measure_singleton (a : Int) : AlternatingBalance.measure [a] = a := by
  simp [AlternatingBalance.measure]

private theorem measure_repeated (n : Nat) (a : Int) :
    AlternatingBalance.measure (repeated n a) = ((n % 2 : Nat) : Int) * a := by
  induction n with
  | zero => simp [repeated, AlternatingBalance.measure]
  | succ n ih =>
      rw [show repeated (n + 1) a = a :: repeated n a by
        simp [repeated, List.replicate_succ], measure_cons, ih]
      have hlt : n % 2 < 2 := Nat.mod_lt _ (by decide)
      by_cases h : n % 2 == 0
      · have h0 : n % 2 = 0 := by simpa using h
        have h1 : (n + 1) % 2 = 1 := by omega
        rw [h0, h1]
        grind
      · have hn0 : ¬ n % 2 = 0 := by simpa using h
        have h0 : n % 2 = 1 := by omega
        have h1 : (n + 1) % 2 = 0 := by omega
        rw [h0, h1]
        grind

/-! #### Goals -/

theorem sound_AlternatingBalance_ParityCarry_O3 :
    TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.AlternatingBalance.ParityCarry.Goal := by
  intro x y
  rw [measure_alt, measure_alt, measure_alt]
  rw [alt_append]
  simp only [Nat.zero_add]
  rw [alt_shift (x.length) y]

theorem sound_AlternatingBalance_BackwardPhase_O3 :
    TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.AlternatingBalance.BackwardPhase.Goal := by
  intro x
  induction x with
  | nil => simp [AlternatingBalance.measure]
  | cons a xs ih =>
      rw [List.reverse_cons]
      rw [sound_AlternatingBalance_ParityCarry_O3]
      rw [measure_singleton]
      rw [List.length_reverse]
      rw [ih]
      rw [measure_cons]
      cases xs with
      | nil => simp [AlternatingBalance.measure]
      | cons b bs =>
          simp only [List.length_cons, Nat.add_sub_cancel_right]
          rw [measure_cons]
          rw [neg_one_pow_succ]
          grind

theorem sound_AlternatingBalance_PeriodicParity_O3 :
    TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.AlternatingBalance.PeriodicParity.Goal := by
  intro x n
  induction n with
  | zero => simp [AlternatingBalance.measure]
  | succ n ih =>
      rw [List.replicate_succ, List.flatten_cons]
      rw [sound_AlternatingBalance_ParityCarry_O3, ih]
      by_cases hl : x.length % 2 == 0
      · have hpow : (-1 : Int) ^ x.length = 1 := by
          rw [neg_one_pow_mod, ite_eq_left hl]
        rw [ite_eq_left hl] at ih ⊢
        rw [hpow]
        grind
      · have hpow : (-1 : Int) ^ x.length = -1 := by
          rw [neg_one_pow_mod, ite_eq_right hl]
        rw [ite_eq_right hl] at ih ⊢
        rw [hpow]
        by_cases hn : n % 2 == 0
        · have hlt : n % 2 < 2 := Nat.mod_lt _ (by decide)
          have h0 : n % 2 = 0 := by simpa using hn
          have h1 : (n + 1) % 2 = 1 := by omega
          rw [h0, h1]
          grind
        · have hlt : n % 2 < 2 := Nat.mod_lt _ (by decide)
          have hn0 : ¬ n % 2 = 0 := by simpa using hn
          have h0 : n % 2 = 1 := by omega
          have h1 : (n + 1) % 2 = 0 := by omega
          rw [h0, h1]
          grind

theorem sound_AlternatingBalance_DuplicateCancellation_O3 :
    TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.AlternatingBalance.DuplicateCancellation.Goal := by
  intro x n
  induction x with
  | nil => simp [AlternatingBalance.measure]
  | cons a xs ih =>
      rw [List.flatMap_cons]
      rw [sound_AlternatingBalance_ParityCarry_O3]
      rw [measure_repeated]
      rw [ih]
      rw [measure_cons]
      rw [show (repeated n a).length = n by simp [repeated]]
      by_cases h : n % 2 == 0
      · have hpow : (-1 : Int) ^ n = 1 := by rw [neg_one_pow_mod, ite_eq_left h]
        rw [ite_eq_left h, hpow]
        have h0 : n % 2 = 0 := by simpa using h
        rw [h0]
        grind
      · have hpow : (-1 : Int) ^ n = -1 := by rw [neg_one_pow_mod, ite_eq_right h]
        rw [ite_eq_right h, hpow]
        have hlt : n % 2 < 2 := Nat.mod_lt _ (by decide)
        have hn0 : ¬ n % 2 = 0 := by simpa using h
        have h0 : n % 2 = 1 := by omega
        rw [h0]
        grind

theorem sound_AlternatingBalance_ShiftResidue_O3 :
    TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.AlternatingBalance.ShiftResidue.Goal := by
  intro b x
  induction x with
  | nil => simp [AlternatingBalance.measure, shifted]
  | cons a xs ih =>
      change AlternatingBalance.measure ((a + b) :: shifted b xs) =
        AlternatingBalance.measure (a :: xs) + (((xs.length + 1) % 2 : Nat) : Int) * b
      rw [measure_cons, measure_cons, ih]
      by_cases h : xs.length % 2 == 0
      · have hlt : xs.length % 2 < 2 := Nat.mod_lt _ (by decide)
        have h0 : xs.length % 2 = 0 := by simpa using h
        have h1 : (xs.length + 1) % 2 = 1 := by omega
        rw [h0, h1]
        grind
      · have hlt : xs.length % 2 < 2 := Nat.mod_lt _ (by decide)
        have hn0 : ¬ xs.length % 2 = 0 := by simpa using h
        have h0 : xs.length % 2 = 1 := by omega
        have h1 : (xs.length + 1) % 2 = 0 := by omega
        rw [h0, h1]
        grind


end TinylambOpt.ResearchProofs.Atlas.Auto_OrderedMeasures_O3
