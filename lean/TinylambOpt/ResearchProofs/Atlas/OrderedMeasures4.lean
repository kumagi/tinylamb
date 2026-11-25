import TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.OrderedMeasures4

open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures
open TinylambOpt.ResearchGoals.PairAggregates (choose2)

/-! Shared list algebra.  All sums are over physical positions, so equal values
in different positions remain different pairs. -/

private theorem sq_nonneg (a : Int) : 0 ≤ a ^ 2 := by
  have h : a ^ 2 = a * a := by grind
  rw [h, ← Int.natAbs_mul_self (a := a)]
  exact Int.natCast_nonneg _

private theorem sum_map_nonneg {α : Type} (xs : List α) (f : α → Int)
    (h : ∀ x ∈ xs, 0 ≤ f x) : 0 ≤ (xs.map f).sum := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      simp only [List.map_cons, List.sum_cons]
      have hx := h x (by simp)
      have ht : ∀ y ∈ xs, 0 ≤ f y := by intro y hy; exact h y (by simp [hy])
      have := ih ht
      omega

private theorem sum_map_le_sum {α : Type} (xs : List α) (f g : α → Int)
    (h : ∀ x ∈ xs, f x ≤ g x) : (xs.map f).sum ≤ (xs.map g).sum := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      simp only [List.map_cons, List.sum_cons]
      have hx := h x (by simp)
      have ht : ∀ y ∈ xs, f y ≤ g y := by intro y hy; exact h y (by simp [hy])
      have := ih ht
      omega

private theorem sum_map_add {α : Type} (xs : List α) (f g : α → Int) :
    (xs.map fun x => f x + g x).sum = (xs.map f).sum + (xs.map g).sum := by
  induction xs with
  | nil => simp
  | cons x xs ih => simp only [List.map_cons, List.sum_cons, ih]; omega

private theorem sum_map_sub {α : Type} (xs : List α) (f g : α → Int) :
    (xs.map fun x => f x - g x).sum = (xs.map f).sum - (xs.map g).sum := by
  induction xs with
  | nil => simp
  | cons x xs ih => simp only [List.map_cons, List.sum_cons, ih]; omega

private theorem perm_sum : ∀ {xs ys : List Int}, xs.Perm ys → xs.sum = ys.sum
  | _, _, .nil => rfl
  | _, _, .cons a p => by simp [perm_sum p]
  | _, _, .swap a b xs => by simp; grind
  | _, _, .trans p q => (perm_sum p).trans (perm_sum q)

private theorem perm_map_sum (f : Int → Int) {xs ys : List Int}
    (p : xs.Perm ys) : (xs.map f).sum = (ys.map f).sum :=
  perm_sum (p.map f)

private theorem flatMap_map {α β γ : Type} (f : β → List γ) (g : α → β)
    (xs : List α) : (xs.map g).flatMap f = xs.flatMap (fun x => f (g x)) := by
  induction xs with
  | nil => rfl
  | cons x xs ih => simp [ih]

private theorem zipIdx_add {α : Type} (xs : List α) (n m : Nat) :
    xs.zipIdx (n + m) = (xs.zipIdx n).map (fun (v, i) => (v, i + m)) := by
  induction xs generalizing n with
  | nil => simp
  | cons a xs ih =>
      simp only [List.zipIdx_cons, List.map_cons]
      congr 1
      simpa [Nat.add_assoc, Nat.add_comm, Nat.add_left_comm] using ih (n + 1)

private theorem pairs_cons (a : Int) (xs : Series) :
    pairs (a :: xs) = xs.map (fun b => (a, b)) ++ pairs xs := by
  unfold pairs
  rw [List.zipIdx_cons]
  simp only [List.flatMap_cons, List.drop]
  rw [show (1 : Nat) = 0 + 1 by omega, zipIdx_add]
  rw [flatMap_map]

private theorem cart_cons (a : Int) (xs ys : Series) :
    cart (a :: xs) ys = ys.map (fun b => (a, b)) ++ cart xs ys := by
  simp [cart]

private theorem cart_sum_cons_right (f : Int × Int → Int) (xs : Series)
    (a : Int) (ys : Series) :
    ((cart xs (a :: ys)).map f).sum =
      ((xs.map fun b => (b, a)).map f).sum + ((cart xs ys).map f).sum := by
  induction xs with
  | nil => simp [cart]
  | cons b xs ih =>
      have hleft : ((cart (b :: xs) (a :: ys)).map f).sum =
          ((a :: ys).map fun c => f (b, c)).sum +
            ((cart xs (a :: ys)).map f).sum := by
        rw [cart_cons]
        simp only [List.map_append, List.map_map, List.sum_append, Function.comp_def]
      rw [hleft, ih, cart_cons]
      simp only [List.map_cons, List.map_append, List.map_map, List.sum_cons,
        List.sum_append, Function.comp_def]
      grind

private theorem pairs_map (f : Int → Int) (xs : Series) :
    pairs (xs.map f) = (pairs xs).map (fun t => (f t.1, f t.2)) := by
  induction xs with
  | nil => simp [pairs]
  | cons a xs ih =>
      rw [List.map_cons, pairs_cons, pairs_cons, ih]
      simp only [List.map_append, List.map_map, Function.comp_def]

private theorem pairs_sum_append (f : Int × Int → Int) (xs ys : Series) :
    ((pairs (xs ++ ys)).map f).sum =
      ((pairs xs).map f).sum + ((pairs ys).map f).sum + ((cart xs ys).map f).sum := by
  induction xs with
  | nil => simp [pairs, cart]
  | cons a xs ih =>
      rw [List.cons_append, pairs_cons, pairs_cons, cart_cons]
      simp only [List.map_append, List.map_map, List.sum_append]
      rw [ih]
      simp only [Function.comp_def]
      grind

private theorem pairs_countP_append (P : Int × Int → Bool) (xs ys : Series) :
    (pairs (xs ++ ys)).countP P =
      (pairs xs).countP P + (pairs ys).countP P + (cart xs ys).countP P := by
  induction xs with
  | nil => simp [pairs, cart]
  | cons a xs ih =>
      rw [List.cons_append, pairs_cons, pairs_cons, cart_cons]
      simp only [List.countP_append, List.countP_map, Function.comp_def]
      rw [ih]
      omega

private theorem countP_map_comp {α β : Type} (P : β → Bool) (f : α → β)
    (xs : List α) : (xs.map f).countP P = xs.countP (fun x => P (f x)) := by
  induction xs with
  | nil => rfl
  | cons x xs ih =>
      by_cases h : P (f x) = true <;> simp [List.countP_cons, h, ih]

private theorem sum_map_sub_sq (a : Int) (xs : Series) :
    (xs.map fun b => (a - b) ^ 2).sum =
      (xs.length : Int) * a ^ 2 - 2 * a * xs.sum + (xs.map fun b => b ^ 2).sum := by
  induction xs with
  | nil => simp
  | cons b xs ih => simp only [List.map_cons, List.sum_cons, List.length_cons, ih]; grind

private theorem sum_map_rev_sub_sq (b : Int) (xs : Series) :
    (xs.map fun a => (a - b) ^ 2).sum =
      (xs.map fun a => a ^ 2).sum - 2 * b * xs.sum + (xs.length : Int) * b ^ 2 := by
  induction xs with
  | nil => simp
  | cons a xs ih => simp only [List.map_cons, List.sum_cons, List.length_cons, ih]; grind

private theorem cart_sq_sum (xs ys : Series) :
    ((cart xs ys).map fun t => (t.1 - t.2) ^ 2).sum =
      (ys.length : Int) * (xs.map fun v => v ^ 2).sum +
        (xs.length : Int) * (ys.map fun v => v ^ 2).sum - 2 * xs.sum * ys.sum := by
  induction xs with
  | nil => simp [cart]
  | cons a xs ih =>
      rw [cart_cons]
      simp only [List.map_append, List.map_map, List.sum_append, List.length_cons,
        List.map_cons, List.sum_cons, Function.comp_def] at ih ⊢
      rw [sum_map_sub_sq, ih]
      grind

/-! ### CenteredScatter -/

theorem sound_CenteredScatter_MomentCollapse : CenteredScatter.MomentCollapse.Goal := by
  intro x
  induction x with
  | nil => simp [CenteredScatter.measure, pairs, size]
  | cons a xs ih =>
      simp only [CenteredScatter.measure, size] at ih ⊢
      rw [pairs_cons]
      simp only [List.map_append, List.map_map, List.sum_append, List.length_cons,
        List.sum_cons, List.map_cons] at ih ⊢
      change (xs.map fun b => (a - b) ^ 2).sum +
          ((pairs xs).map (fun p : Int × Int => (p.1 - p.2) ^ 2)).sum = _
      rw [sum_map_sub_sq, ih]
      grind

theorem sound_CenteredScatter_PairMerge : CenteredScatter.PairMerge.Goal := by
  intro x y
  simp only [CenteredScatter.measure]
  rw [pairs_sum_append, cart_sq_sum]
  simp only [size]
  grind

theorem sound_CenteredScatter_Translation : CenteredScatter.Translation.Goal := by
  intro b x
  simp only [CenteredScatter.measure, shifted]
  rw [pairs_map]
  simp only [List.map_map, Function.comp_def]
  apply congrArg List.sum
  apply List.map_congr_left
  intro t _
  grind

private theorem cart_self_sq (xs : Series) :
    ((cart xs xs).map fun t => (t.1 - t.2) ^ 2).sum =
      2 * CenteredScatter.measure xs := by
  induction xs with
  | nil => simp [cart, CenteredScatter.measure, pairs]
  | cons a xs ih =>
      simp only [CenteredScatter.measure]
      rw [cart_cons, pairs_cons]
      simp only [List.map_append, List.map_map, List.sum_append,
        List.map_cons, List.sum_cons, Function.comp_def, CenteredScatter.measure] at ih ⊢
      rw [cart_sum_cons_right (fun t : Int × Int => (t.1 - t.2) ^ 2)]
      simp only [List.map_append, List.map_map, List.sum_append, Function.comp_def]
      rw [sum_map_sub_sq (a := a), sum_map_rev_sub_sq (b := a), ih]
      grind

private theorem replicate_flatten_succ (n : Nat) (xs : Series) :
    (List.replicate (n + 1) xs).flatten = xs ++ (List.replicate n xs).flatten := by
  rfl

private theorem replicate_flatten_length (n : Nat) (xs : Series) :
    (List.replicate n xs).flatten.length = n * xs.length := by
  induction n with
  | zero => simp
  | succ n ih => rw [replicate_flatten_succ, List.length_append, ih]; grind

private theorem replicate_flatten_sum (n : Nat) (xs : Series) :
    (List.replicate n xs).flatten.sum = (n : Int) * xs.sum := by
  induction n with
  | zero => simp
  | succ n ih => rw [replicate_flatten_succ, List.sum_append, ih]; push_cast; grind

private theorem replicate_flatten_sq_sum (n : Nat) (xs : Series) :
    ((List.replicate n xs).flatten.map fun v => v ^ 2).sum =
      (n : Int) * (xs.map fun v => v ^ 2).sum := by
  induction n with
  | zero => simp
  | succ n ih =>
      rw [replicate_flatten_succ, List.map_append, List.sum_append, ih]
      push_cast
      grind

private theorem centered_measure_append (xs ys : Series) :
    CenteredScatter.measure (xs ++ ys) = CenteredScatter.measure xs +
      CenteredScatter.measure ys +
        ((cart xs ys).map fun t => (t.1 - t.2) ^ 2).sum := by
  simpa only [CenteredScatter.measure] using pairs_sum_append
    (fun t : Int × Int => (t.1 - t.2) ^ 2) xs ys

private theorem cart_sq_replicate (n : Nat) (xs : Series) :
    ((cart xs (List.replicate n xs).flatten).map fun t => (t.1 - t.2) ^ 2).sum =
      2 * (n : Int) * CenteredScatter.measure xs := by
  have hm := sound_CenteredScatter_MomentCollapse xs
  simp only [size] at hm
  rw [cart_sq_sum, replicate_flatten_length, replicate_flatten_sum,
    replicate_flatten_sq_sum]
  push_cast
  rw [hm]
  grind

theorem sound_CenteredScatter_ReplicatedGroup : CenteredScatter.ReplicatedGroup.Goal := by
  intro n x
  induction n with
  | zero => simp [CenteredScatter.measure, pairs]
  | succ n ih =>
      rw [replicate_flatten_succ, centered_measure_append, cart_sq_replicate, ih]
      simp only [CenteredScatter.measure]
      push_cast
      grind

private theorem even_pred_mul (n : Nat) : ∃ k : Nat, n * (n - 1) = 2 * k := by
  rcases Nat.mod_two_eq_zero_or_one n with h | h
  · have hn : n = 2 * (n / 2) := by
      have hd := Nat.mod_add_div n 2
      omega
    refine ⟨(n / 2) * (n - 1), ?_⟩
    calc
      n * (n - 1) = (2 * (n / 2)) * (n - 1) :=
        congrArg (fun k => k * (n - 1)) hn
      _ = 2 * ((n / 2) * (n - 1)) := by grind
  · have hn : n = 2 * ((n - 1) / 2) + 1 := by
      have hd := Nat.mod_add_div n 2
      omega
    have hpred : n - 1 = 2 * ((n - 1) / 2) := by omega
    refine ⟨n * ((n - 1) / 2), ?_⟩
    calc
      n * (n - 1) = n * (2 * ((n - 1) / 2)) :=
        congrArg (fun k => n * k) hpred
      _ = 2 * (n * ((n - 1) / 2)) := by grind

private theorem choose2_double (n : Nat) : 2 * choose2 n = n * (n - 1) := by
  rcases even_pred_mul n with ⟨k, hk⟩
  unfold choose2
  rw [hk]
  simp

private theorem pairs_length (xs : Series) : (pairs xs).length = choose2 xs.length := by
  induction xs with
  | nil => rfl
  | cons a xs ih =>
      rw [pairs_cons, List.length_append, List.length_map, ih]
      apply Nat.eq_of_mul_eq_mul_left (by decide : 0 < 2)
      have hm := choose2_double xs.length
      have hs := choose2_double (xs.length + 1)
      simp only [Nat.add_sub_cancel] at hs
      have hsub : xs.length * (xs.length - 1) + xs.length = xs.length * xs.length := by
        rw [Nat.mul_sub_left_distrib, Nat.mul_one]
        apply Nat.sub_add_cancel
        by_cases hz : xs.length = 0
        · simp [hz]
        · exact Nat.le_mul_of_pos_left xs.length (Nat.pos_of_ne_zero hz)
      simp only [List.length_cons]
      have hdistr : 2 * (xs.length + choose2 xs.length) =
          2 * xs.length + 2 * choose2 xs.length := by grind
      rw [hdistr, hm, hs]
      simp only [Nat.add_mul, Nat.one_mul]
      omega

private theorem max_getD_of_mem (xs : Series) (a : Int) (ha : a ∈ xs) :
    a ≤ xs.max?.getD 0 := by
  obtain ⟨m, hm⟩ := Option.isSome_iff_exists.mp (List.isSome_max?_of_mem ha)
  rw [hm]
  exact (List.max?_eq_some_iff.mp hm).2 a ha

private theorem min_getD_le_of_mem (xs : Series) (a : Int) (ha : a ∈ xs) :
    xs.min?.getD 0 ≤ a := by
  obtain ⟨m, hm⟩ := Option.isSome_iff_exists.mp (List.isSome_min?_of_mem ha)
  rw [hm]
  exact (List.min?_eq_some_iff.mp hm).2 a ha

private theorem sum_map_const_int {α : Type} (xs : List α) (c : Int) :
    (xs.map fun _ => c).sum = (xs.length : Int) * c := by
  induction xs with
  | nil => simp
  | cons x xs ih => simp [ih]; grind

private theorem row_endpoint_sum (a : Int) (xs : Series) (f : Int → Int) :
    ((xs.map fun b => (a, b)).map fun t => f t.1 + f t.2).sum =
      (xs.length : Int) * f a + (xs.map f).sum := by
  rw [List.map_map]
  simp only [Function.comp_def]
  rw [sum_map_add]
  simp [sum_map_const_int]

private theorem pairs_endpoint_sum (xs : Series) (f : Int → Int) :
    ((pairs xs).map fun t => f t.1 + f t.2).sum =
      (xs.length : Int) * (xs.map f).sum - (xs.map f).sum := by
  induction xs with
  | nil => simp [pairs]
  | cons a xs ih =>
      rw [pairs_cons, List.map_append, List.sum_append, row_endpoint_sum, ih]
      simp only [List.length_cons, List.map_cons, List.sum_cons]
      push_cast
      grind

private theorem pair_sq_le_interval_cross (m M a b : Int)
    (hma : m ≤ a) (haM : a ≤ M) (hmb : m ≤ b) (hbM : b ≤ M) :
    (a - b) ^ 2 ≤ (a - m) * (M - b) + (b - m) * (M - a) := by
  have h1 : 0 ≤ (a - m) * (M - b) :=
    Int.mul_nonneg (by omega) (by omega)
  have h2 : 0 ≤ (b - m) * (M - a) :=
    Int.mul_nonneg (by omega) (by omega)
  have h3 : 0 ≤ (a - m) * (M - a) :=
    Int.mul_nonneg (by omega) (by omega)
  have h4 : 0 ≤ (b - m) * (M - b) :=
    Int.mul_nonneg (by omega) (by omega)
  have hd : (a - m) * (M - b) + (b - m) * (M - a) - (a - b) * (a - b) =
      (a - m) * (M - a) + (b - m) * (M - b) := by
    grind
  have hpow : (a - b) ^ 2 = (a - b) * (a - b) := by grind
  rw [hpow]
  omega

private theorem sum_map_mul_left {α : Type} (xs : List α) (f : α → Int) (c : Int) :
    (xs.map fun x => c * f x).sum = c * (xs.map f).sum := by
  induction xs with
  | nil => simp
  | cons x xs ih => simp [ih]; grind

private theorem row_product_sum (a : Int) (xs : Series) :
    ((xs.map fun b => (a, b)).map fun t => t.1 * t.2).sum = a * xs.sum := by
  rw [List.map_map]
  simp only [Function.comp_def]
  simpa using sum_map_mul_left xs (fun b => b) a

private theorem pairs_product_double (xs : Series) :
    2 * ((pairs xs).map fun t => t.1 * t.2).sum =
      xs.sum ^ 2 - (xs.map fun v => v ^ 2).sum := by
  induction xs with
  | nil => simp [pairs]
  | cons a xs ih =>
      rw [pairs_cons, List.map_append, List.sum_append, row_product_sum]
      have hdistr : 2 * (a * xs.sum +
          ((pairs xs).map fun t => t.1 * t.2).sum) =
          2 * a * xs.sum + 2 * ((pairs xs).map fun t => t.1 * t.2).sum := by grind
      rw [hdistr, ih]
      simp only [List.sum_cons, List.map_cons]
      grind

private theorem sum_map_shift (b : Int) (xs : Series) :
    (xs.map fun v => v - b).sum = xs.sum - (xs.length : Int) * b := by
  induction xs with
  | nil => simp
  | cons v xs ih => simp only [List.map_cons, List.sum_cons, List.length_cons, ih]; grind

theorem sound_CenteredScatter_OutlierCap : CenteredScatter.OutlierCap.Goal := by
  intro x
  let m := x.min?.getD 0
  let M := x.max?.getD 0
  have hbounds : ∀ v ∈ x, m ≤ v ∧ v ≤ M := by
    intro v hv
    exact ⟨min_getD_le_of_mem x v hv, max_getD_of_mem x v hv⟩
  have hpoint : ∀ t ∈ pairs x,
      (t.1 - t.2) ^ 2 ≤ (t.1 - m) * (M - t.2) + (t.2 - m) * (M - t.1) := by
    intro t ht
    unfold pairs at ht
    rw [List.mem_flatMap] at ht
    rcases ht with ⟨⟨a, i⟩, hai, ht⟩
    rcases List.mem_map.mp ht with ⟨b, hb, rfl⟩
    have ha : a ∈ x := List.mem_of_getElem? (List.mk_mem_zipIdx_iff_getElem?.mp hai)
    have hb' : b ∈ x := List.mem_of_mem_drop hb
    exact pair_sq_le_interval_cross m M a b (hbounds a ha).1 (hbounds a ha).2
      (hbounds b hb').1 (hbounds b hb').2
  have hsum := sum_map_le_sum (pairs x) (fun t => (t.1 - t.2) ^ 2)
    (fun t => (t.1 - m) * (M - t.2) + (t.2 - m) * (M - t.1)) hpoint
  let R := M - m
  let T := (x.map fun v => v - m).sum
  let Q := (x.map fun v => (v - m) ^ 2).sum
  let P := ((pairs x).map fun t => (t.1 - m) * (t.2 - m)).sum
  have hcrossMap : (pairs x).map
      (fun t => (t.1 - m) * (M - t.2) + (t.2 - m) * (M - t.1)) =
      (pairs x).map
        (fun t => R * ((t.1 - m) + (t.2 - m)) - 2 * ((t.1 - m) * (t.2 - m))) := by
    apply List.map_congr_left
    intro t _
    dsimp [R]
    grind
  have hcross : ((pairs x).map
      (fun t => (t.1 - m) * (M - t.2) + (t.2 - m) * (M - t.1))).sum =
      (x.length : Int) * R * T - R * T - 2 * P := by
    have hend := pairs_endpoint_sum x (fun v => v - m)
    change ((pairs x).map fun t => (t.1 - m) + (t.2 - m)).sum = _ at hend
    rw [hcrossMap, sum_map_sub, sum_map_mul_left, sum_map_mul_left, hend]
    change R * ((x.length : Int) * T - T) - 2 * P =
      (x.length : Int) * R * T - R * T - 2 * P
    grind
  have hsumCross := hsum
  rw [hcross] at hsum
  have hp := pairs_product_double (x.map fun v => v - m)
  rw [pairs_map] at hp
  simp only [List.map_map, Function.comp_def] at hp
  have hprod : 2 * P = T ^ 2 - Q := hp
  have hQle : Q ≤ R * T := by
    rw [← sum_map_mul_left x (fun v => v - m) R]
    apply sum_map_le_sum
    intro v hv
    have ht0 : 0 ≤ v - m := by have := (hbounds v hv).1; omega
    have htR : v - m ≤ R := by dsimp [R]; have := (hbounds v hv).2; omega
    have hpow : (v - m) ^ 2 = (v - m) * (v - m) := by grind
    rw [hpow]
    exact Int.mul_le_mul_of_nonneg_right htR ht0
  have hT0 : 0 ≤ T := by
    apply sum_map_nonneg
    intro v hv
    have := (hbounds v hv).1
    omega
  have hTle : T ≤ (x.length : Int) * R := by
    rw [← sum_map_const_int x R]
    apply sum_map_le_sum
    intro v hv
    have := (hbounds v hv).1
    have := (hbounds v hv).2
    dsimp [R]
    omega
  have hscalar : 4 * T * ((x.length : Int) * R - T) ≤
      (x.length : Int) ^ 2 * R ^ 2 := by
    have heq : 4 * T * ((x.length : Int) * R - T) =
        (x.length : Int) ^ 2 * R ^ 2 - (2 * T - (x.length : Int) * R) ^ 2 := by grind
    have hn := sq_nonneg (2 * T - (x.length : Int) * R)
    rw [heq]
    omega
  have hCbound : 4 *
      ((pairs x).map (fun t => (t.1 - m) * (M - t.2) + (t.2 - m) * (M - t.1))).sum ≤
      4 * T * ((x.length : Int) * R - T) := by
    have hCeq : ((pairs x).map
        (fun t => (t.1 - m) * (M - t.2) + (t.2 - m) * (M - t.1))).sum =
        (x.length : Int) * R * T - R * T - T ^ 2 + Q := by grind
    have hQeq : Q = T ^ 2 - 2 * P := by omega
    have hPQ : T ^ 2 - R * T ≤ 2 * P := by omega
    rw [hCeq, hQeq]
    have h1 : 4 * ((x.length : Int) * R * T - R * T - T ^ 2 + (T ^ 2 - 2 * P)) =
        4 * (x.length : Int) * R * T - 4 * R * T - 8 * P := by grind
    have h2 : 4 * T * ((x.length : Int) * R - T) =
        4 * (x.length : Int) * R * T - 4 * T ^ 2 := by grind
    have hp4 : 4 * T ^ 2 - 4 * R * T ≤ 8 * P := by grind
    rw [h1, h2]
    omega
  have hfinal : 4 * CenteredScatter.measure x ≤
      (x.length : Int) ^ 2 * (M - m) ^ 2 := by
    calc
      4 * CenteredScatter.measure x ≤
          4 * ((pairs x).map
            (fun t => (t.1 - m) * (M - t.2) + (t.2 - m) * (M - t.1))).sum :=
        Int.mul_le_mul_of_nonneg_left
          (by simpa only [CenteredScatter.measure] using hsumCross)
          (by decide : (0 : Int) ≤ 4)
      _ ≤ 4 * T * ((x.length : Int) * R - T) := hCbound
      _ ≤ (x.length : Int) ^ 2 * R ^ 2 := hscalar
      _ = (x.length : Int) ^ 2 * (M - m) ^ 2 := by rfl
  simpa only [CenteredScatter.measure, size, spread, m, M] using hfinal

/-! ### GiniDistance -/

private theorem distance_symm (a b : Int) : distance a b = distance b a := by
  unfold distance
  rw [show b - a = -(a - b) by grind, Int.natAbs_neg]

private theorem distance_triangle (a b c : Int) :
    distance a c ≤ distance a b + distance b c := by
  unfold distance
  have h := Int.natAbs_add_le (c - b) (b - a)
  have heq : c - a = (c - b) + (b - a) := by grind
  rw [heq]
  have h' : (-(b - c) + -(a - b)).natAbs ≤
      (a - b).natAbs + (b - c).natAbs := by
    simpa only [show c - b = -(b - c) by grind, show b - a = -(a - b) by grind,
      Int.natAbs_neg, Nat.add_comm] using h
  exact_mod_cast h'

private theorem cart_self_distance (xs : Series) :
    ((cart xs xs).map fun t => distance t.1 t.2).sum = 2 * GiniDistance.measure xs := by
  induction xs with
  | nil => simp [cart, GiniDistance.measure, pairs]
  | cons a xs ih =>
      rw [cart_cons, GiniDistance.measure, pairs_cons]
      simp only [List.map_append, List.map_map, List.sum_append, List.map_cons,
        List.sum_cons, Function.comp_def, GiniDistance.measure] at ih ⊢
      rw [cart_sum_cons_right (fun t => distance t.1 t.2)]
      simp only [List.map_append, List.map_map, List.sum_append, Function.comp_def]
      rw [ih]
      have hsym : (xs.map fun b => distance b a).sum = (xs.map fun b => distance a b).sum := by
        apply congrArg List.sum
        apply List.map_congr_left
        intro b _
        exact (distance_symm a b).symm
      rw [hsym]
      have hzero : distance a a = 0 := by simp [distance]
      grind

private theorem cart_sum_nested (f : Int × Int → Int) (xs ys : Series) :
    ((cart xs ys).map f).sum =
      (xs.map fun a => (ys.map fun b => f (a, b)).sum).sum := by
  induction xs with
  | nil => simp [cart]
  | cons a xs ih => simp only [cart_cons, List.map_append, List.map_map,
      List.sum_append, List.map_cons, List.sum_cons, Function.comp_def, ih]

private theorem cart_sum_perm (f : Int × Int → Int) {xs xs' ys ys' : Series}
    (px : xs.Perm xs') (py : ys.Perm ys') :
    ((cart xs ys).map f).sum = ((cart xs' ys').map f).sum := by
  rw [cart_sum_nested, cart_sum_nested]
  calc
    (xs.map fun a => (ys.map fun b => f (a, b)).sum).sum =
      (xs.map fun a => (ys'.map fun b => f (a, b)).sum).sum := by
        apply congrArg List.sum
        apply List.map_congr_left
        intro a _
        exact perm_map_sum (fun b => f (a, b)) py
    _ = (xs'.map fun a => (ys'.map fun b => f (a, b)).sum).sum :=
      perm_map_sum (fun a => (ys'.map fun b => f (a, b)).sum) px

private theorem gini_measure_perm {xs ys : Series} (p : xs.Perm ys) :
    GiniDistance.measure xs = GiniDistance.measure ys := by
  have h := cart_sum_perm (fun t => distance t.1 t.2) p p
  rw [cart_self_distance, cart_self_distance] at h
  omega

private def sortedWeight (xs : Series) : Int :=
  (xs.zipIdx.map fun (v, i) => (2 * (i : Int) + 1 - (xs.length : Int)) * v).sum

private theorem zipIdx_weight_shift_from (xs : Series) (k : Nat) (n : Int) :
    ((xs.zipIdx (k + 1)).map fun (v, j) => (2 * (j : Int) + 1 - n) * v).sum =
      ((xs.zipIdx k).map fun (v, i) => (2 * (i : Int) + 1 - (n - 1)) * v).sum + xs.sum := by
  induction xs generalizing k with
  | nil => simp
  | cons a xs ih =>
      simp only [List.zipIdx_cons, List.map_cons, List.sum_cons]
      rw [ih (k + 1)]
      push_cast
      grind

private theorem zipIdx_weight_shift (xs : Series) (n : Int) :
    ((xs.zipIdx 1).map fun (v, j) => (2 * (j : Int) + 1 - n) * v).sum =
      (xs.zipIdx.map fun (v, i) => (2 * (i : Int) + 1 - (n - 1)) * v).sum + xs.sum := by
  simpa using zipIdx_weight_shift_from xs 0 n

private theorem sortedWeight_cons (a : Int) (xs : Series) :
    sortedWeight (a :: xs) = sortedWeight xs + xs.sum - (xs.length : Int) * a := by
  unfold sortedWeight
  rw [List.zipIdx_cons]
  simp only [List.length_cons, List.map_cons, List.sum_cons]
  rw [zipIdx_weight_shift]
  push_cast
  grind

private theorem sum_distance_from (a : Int) (xs : Series)
    (h : ∀ b ∈ xs, a ≤ b) :
    (xs.map fun b => distance a b).sum = xs.sum - (xs.length : Int) * a := by
  have hm : xs.map (fun b => distance a b) = xs.map (fun b => b - a) := by
    apply List.map_congr_left
    intro b hb
    unfold distance
    rw [Int.natAbs_of_nonneg (by have := h b hb; omega : 0 ≤ b - a)]
  rw [hm]
  simpa [sum_map_const_int] using sum_map_sub xs (fun b => b) (fun _ => a)

private theorem sortedWeight_eq_gini (xs : Series) (hs : xs.Pairwise (· ≤ ·)) :
    sortedWeight xs = GiniDistance.measure xs := by
  induction xs with
  | nil => simp [sortedWeight, GiniDistance.measure, pairs]
  | cons a xs ih =>
      have hp : xs.Pairwise (· ≤ ·) := (List.pairwise_cons.mp hs).2
      have ha : ∀ b ∈ xs, a ≤ b := (List.pairwise_cons.mp hs).1
      rw [sortedWeight_cons, ih hp]
      simp only [GiniDistance.measure, pairs_cons, List.map_append, List.map_map,
        List.sum_append, Function.comp_def]
      have hrow := sum_distance_from a xs ha
      rw [hrow]
      grind

theorem sound_GiniDistance_SortedWeights : GiniDistance.SortedWeights.Goal := by
  intro x
  let s := x.mergeSort (fun a b => a ≤ b)
  have hp0 := List.pairwise_mergeSort (le := fun a b => decide (a ≤ b))
    (l := x) (by
      intro a b c hab hbc
      exact decide_eq_true (Int.le_trans (of_decide_eq_true hab) (of_decide_eq_true hbc)))
    (by
      intro a b
      by_cases h : a ≤ b
      · simp [h]
      · have hba : b ≤ a := by omega
        simp [h, hba])
  have hp : s.Pairwise (· ≤ ·) := by
    simpa only [decide_eq_true_eq] using hp0
  have hperm := List.mergeSort_perm x (fun a b => decide (a ≤ b))
  have hsort : GiniDistance.measure x = GiniDistance.measure s :=
    gini_measure_perm hperm.symm
  have hw := sortedWeight_eq_gini s hp
  have hlen : s.length = x.length := hperm.length_eq
  simpa only [sortedWeight, s, hlen, size] using hsort.trans hw.symm

private theorem cart_distance_separated (x y : Series)
    (h : ∀ a ∈ x, ∀ b ∈ y, a ≤ b) :
    ((cart x y).map fun t => distance t.1 t.2).sum =
      (x.length : Int) * y.sum - (y.length : Int) * x.sum := by
  induction x with
  | nil => simp [cart]
  | cons a xs ih =>
      rw [cart_cons]
      simp only [List.map_append, List.map_map, List.sum_append, List.length_cons,
        List.map_cons, List.sum_cons, Function.comp_def]
      have hrow := sum_distance_from a y (fun b hb => h a (by simp) b hb)
      have htail := ih (by intro c hc b hb; exact h c (by simp [hc]) b hb)
      rw [hrow, htail]
      grind

theorem sound_GiniDistance_SeparatedBlocks : GiniDistance.SeparatedBlocks.Goal := by
  intro x y h
  simp only [GiniDistance.measure]
  rw [pairs_sum_append, cart_distance_separated x y h]
  simp only [size]
  grind

private theorem count_filter_eq_self {α : Type} [DecidableEq α] (xs : List α)
    (p : α → Bool) (a : α) (ha : p a = true) :
    (xs.filter p).count a = xs.count a := by
  induction xs with
  | nil => simp
  | cons b xs ih =>
      rw [List.filter_cons]
      by_cases hp : p b = true
      · rw [if_pos hp, List.count_cons, List.count_cons]
        by_cases hba : b = a
        · subst b
          simp [ih]
        · simp [hba, ih]
      · rw [if_neg hp, List.count_cons]
        by_cases hbeq : (b == a) = true
        · have hba : b = a := of_decide_eq_true hbeq
          exact hp (hba ▸ ha)
        · rw [ite_eq_right hbeq, ih]
          omega

private theorem sum_eraseDups_weighted_aux (n : Nat) :
    ∀ (xs : List Int), xs.length = n → ∀ f : Int → Int,
      (xs.eraseDups.map fun a => f a * xs.count a).sum = (xs.map f).sum := by
  induction n with
  | zero =>
      intro xs hlen f
      cases xs with
      | nil => simp
      | cons a xs => simp at hlen
  | succ n ih =>
      intro xs hlen f
      cases xs with
      | nil => simp at hlen
      | cons a xs =>
          have hxslen : xs.length = n := by simpa using hlen
          rw [List.eraseDups_cons, List.map_cons, List.sum_cons, List.map_cons, List.sum_cons]
          let ys := xs.filter fun b => !b == a
          let ds := ys.eraseDups
          have hnot : a ∉ ds := by
            intro hm
            have hm' := List.mem_eraseDups.mp hm
            exact Bool.eq_false_of_ne_true (by simp) (List.mem_filter.mp hm').2
          have hcounts : ∀ b ∈ ds, (a :: xs).count b = ys.count b := by
            intro b hb
            have hbne : b ≠ a := by
              intro heq
              exact hnot (heq ▸ hb)
            have hy : b ∈ ys := List.mem_eraseDups.mp hb
            have hp : (!b == a) = true := by
              change (!(b == a)) = true
              simp [hbne]
            have hxs : xs.count b = ys.count b :=
              (count_filter_eq_self xs (fun z => !z == a) b hp).symm
            simp [List.count_cons, hbne, hxs]
          have hds : (ds.map fun b => f b * (a :: xs).count b).sum =
              (ds.map fun b => f b * ys.count b).sum := by
            apply congrArg List.sum
            apply List.map_congr_left
            intro b hb
            rw [hcounts b hb]
          have hyslen : ys.length ≤ n := by
            dsimp only [ys]
            simpa [hxslen] using List.length_filter_le (fun b => !b == a) xs
          have hfiltered := ih ys hyslen f
          rw [hds, hfiltered]
          have hsplit : (xs.map f).sum =
              ((xs.filter fun b => !b == a).map f).sum +
                ((xs.filter fun b => b == a).map f).sum := by
            induction xs with
            | nil => simp
            | cons b xs ihtail =>
                by_cases hba : b = a
                · subst b
                  simp [ihtail]
                  omega
                · simp [hba, ihtail]
                  omega
          have hsame : ((xs.filter fun b => b == a).map f).sum = f a * xs.count a := by
            induction xs with
            | nil => simp
            | cons b xs ihtail =>
                by_cases hba : b = a
                · subst b
                  simp [ihtail]
                  grind
                · simp [hba, ihtail]
          rw [hsplit, hsame]
          grind

private theorem sum_eraseDups_weighted (xs : List Int) (f : Int → Int) :
    (xs.eraseDups.map fun a => f a * xs.count a).sum = (xs.map f).sum :=
  sum_eraseDups_weighted_aux xs.length xs rfl f

theorem sound_GiniDistance_DictionaryPairs : GiniDistance.DictionaryPairs.Goal := by
  intro x
  rw [← cart_self_distance]
  rw [cart_sum_nested, cart_sum_nested]
  symm
  calc
    (x.eraseDups.map fun a =>
        (x.eraseDups.map fun b => distance a b * x.count a * x.count b).sum).sum =
      (x.eraseDups.map fun a =>
        x.count a * (x.eraseDups.map fun b => distance a b * x.count b).sum).sum := by
          apply congrArg List.sum
          apply List.map_congr_left
          intro a _
          have hm : x.eraseDups.map (fun b => distance a b * x.count a * x.count b) =
              x.eraseDups.map (fun b => x.count a * (distance a b * x.count b)) := by
            apply List.map_congr_left
            intro b _
            grind
          rw [hm, sum_map_mul_left]
    _ = (x.map fun a => (x.map fun b => distance a b).sum).sum := by
      have hlast := sum_eraseDups_weighted x (fun a => (x.map fun b => distance a b).sum)
      have hm : x.eraseDups.map (fun a => x.count a * (x.map fun b => distance a b).sum) =
          x.eraseDups.map (fun a => (x.map fun b => distance a b).sum) := by
        apply List.map_congr_left
        intro a _
        grind
      rw [hm]
      exact hlast.symm

private theorem sum_abs_le (xs : List Int) (f : Int → Int) (c : Int)
    (hc : 0 ≤ c) (h : ∀ x ∈ xs, ((f x).natAbs : Int) ≤ c) :
    ((xs.map f).sum).natAbs ≤ (xs.length : Int) * c := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      simp only [List.map_cons, List.sum_cons, List.length_cons]
      have hx := h x (by simp)
      have ht : ∀ y ∈ xs, ((f y).natAbs : Int) ≤ c := by
        intro y hy; exact h y (by simp [hy])
      have hi := ih ht
      have habs := Int.natAbs_add_le (f x) (xs.map f).sum
      have hx' : (f x).natAbs ≤ c := by exact_mod_cast hx
      have hi' : ((xs.map f).sum).natAbs ≤ (xs.length : Int) * c := by
        exact_mod_cast hi
      push_cast at habs hx' hi' ⊢
      omega

theorem sound_GiniDistance_Lipschitz : GiniDistance.Lipschitz.Goal := by
  intro x a b
  have hmeasure : ∀ c : Int,
      GiniDistance.measure (c :: x) =
        (x.map fun u => distance c u).sum + GiniDistance.measure x := by
    intro c
    simp only [GiniDistance.measure, pairs_cons, List.map_append, List.map_map,
      List.sum_append, Function.comp_def]
    grind
  rw [hmeasure, hmeasure]
  have hpoint : ∀ u ∈ x,
      ((distance a u - distance b u).natAbs : Int) ≤ distance a b := by
    intro u hu
    have h1 := distance_triangle a b u
    have h2 := distance_triangle b a u
    rw [distance_symm b a] at h2
    have hnon : 0 ≤ distance a b := Int.natCast_nonneg _
    by_cases hz : 0 ≤ distance a u - distance b u
    · rw [Int.natAbs_of_nonneg hz]
      omega
    · rw [show distance a u - distance b u = -(distance b u - distance a u) by grind,
        Int.natAbs_neg, Int.natAbs_of_nonneg (by omega : 0 ≤ distance b u - distance a u)]
      omega
  have h := sum_abs_le x (fun u => distance a u - distance b u) (distance a b)
    (Int.natCast_nonneg _) hpoint
  simpa [size] using h

private theorem sum_sq_cauchy (xs : List Int) :
    xs.sum ^ 2 ≤ (xs.length : Int) * (xs.map fun x => x ^ 2).sum := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      simp only [List.sum_cons, List.length_cons, List.map_cons]
      push_cast
      by_cases hk : xs.length = 0
      · simp [hk]
        grind
      · have hkpos : (0 : Int) < xs.length := by exact_mod_cast Nat.pos_of_ne_zero hk
        let k : Int := xs.length
        let Q : Int := (xs.map fun x => x ^ 2).sum
        let S : Int := xs.sum
        have hih : S ^ 2 ≤ k * Q := by simpa [k, Q, S] using ih
        have hv : 0 ≤ k * Q - S ^ 2 := by omega
        have hsq : 0 ≤ (k * x - S) ^ 2 := sq_nonneg _
        have hident : (k * x - S) ^ 2 + (k * Q - S ^ 2) =
            k * (k * x ^ 2 - 2 * x * S + Q) := by grind
        have hprod : 0 ≤ k * (k * x ^ 2 - 2 * x * S + Q) := by
          rw [← hident]
          omega
        have hmain : 0 ≤ k * x ^ 2 - 2 * x * S + Q := by
          by_cases hn : 0 ≤ k * x ^ 2 - 2 * x * S + Q
          · exact hn
          · have hneg : k * x ^ 2 - 2 * x * S + Q < 0 := by omega
            have := Int.mul_neg_of_pos_of_neg hkpos hneg
            omega
        have hlhs : (x + S) ^ 2 = x ^ 2 + 2 * x * S + S ^ 2 := by grind
        have hrhs : (k + 1) * (x ^ 2 + Q) = x ^ 2 + Q + k * x ^ 2 + k * Q := by grind
        change (x + S) ^ 2 ≤ (k + 1) * (x ^ 2 + Q)
        rw [hlhs, hrhs]
        omega

private theorem distance_sq (a b : Int) : distance a b ^ 2 = (a - b) ^ 2 := by
  unfold distance
  have h1 : ((b - a).natAbs : Int) ^ 2 =
      ((b - a).natAbs : Int) * ((b - a).natAbs : Int) := by grind
  have h2 : (a - b) ^ 2 = (a - b) * (a - b) := by grind
  rw [h1, Int.natAbs_mul_self', h2]
  grind

theorem sound_GiniDistance_VarianceFence : GiniDistance.VarianceFence.Goal := by
  intro x
  have hc := sum_sq_cauchy ((pairs x).map fun t => distance t.1 t.2)
  have hsq : (((pairs x).map fun t => distance t.1 t.2).map fun v => v ^ 2).sum =
      CenteredScatter.measure x := by
    simp only [List.map_map, Function.comp_def]
    apply congrArg List.sum
    apply List.map_congr_left
    intro t _
    exact distance_sq t.1 t.2
  rw [hsq, List.length_map, pairs_length] at hc
  simpa only [GiniDistance.measure] using hc

end TinylambOpt.ResearchProofs.Atlas.OrderedMeasures4
