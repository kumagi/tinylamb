import TinylambOpt.ResearchGoals.ExecutionStrategies.PairAggregates

namespace TinylambOpt.ResearchProofs.Wave7PairAggregates

open TinylambOpt.ResearchGoals.PairAggregates

private theorem perm_sum_int {l₁ l₂ : List Int} (p : l₁.Perm l₂) : l₁.sum = l₂.sum := by
  induction p with
  | nil => rfl
  | cons a p ih => simpa using congrArg (fun s => a + s) ih
  | swap a b l => simp [List.sum_cons, Int.add_left_comm]
  | trans p1 p2 ih1 ih2 => exact ih1.trans ih2

private theorem pairs_perm_left (f : Int → Int → Int) {xs xs' : List Int} (p : xs.Perm xs') (ys : List Int) :
    pairs xs ys f = pairs xs' ys f := by
  induction p with
  | nil => rfl
  | cons a p ih => simpa [pairs] using congrArg (fun s => (ys.map (f a)).sum + s) ih
  | swap a b l => simp [pairs, Int.add_left_comm]
  | trans p1 p2 ih1 ih2 => exact ih1.trans ih2

private theorem pairs_perm_right (f : Int → Int → Int) (xs : List Int) {ys ys' : List Int} (p : ys.Perm ys') :
    pairs xs ys f = pairs xs ys' f := by
  induction xs with
  | nil => rfl
  | cons x xs ih =>
      have hp : (ys.map (f x)).Perm (ys'.map (f x)) := p.map (f x)
      have hs := perm_sum_int hp
      have ih' : (List.map (fun x => (List.map (f x) ys).sum) xs).sum =
          (List.map (fun x => (List.map (f x) ys').sum) xs).sum := by simpa [pairs] using ih
      simp only [pairs, List.map_cons, List.sum_cons]
      rw [hs, ih']

private def scanFrom (n s acc : Int) (xs : List Int) : Int :=
  (xs.foldl (fun (n, s, acc) x => (n+1, s+x, acc+n*x-s)) (n, s, acc)).2.2

private theorem scanFrom_acc_add (n s acc d : Int) (ys : List Int) :
    scanFrom n s (acc + d) ys = scanFrom n s acc ys + d := by
  induction ys generalizing n s acc with
  | nil => simp [scanFrom]
  | cons y ys ih =>
      change scanFrom (n+1) (s+y) ((acc + d) + n*y - s) ys =
        scanFrom (n+1) (s+y) (acc + n*y - s) ys + d
      rw [show (acc + d) + n*y - s = (acc + n*y - s) + d by omega]
      rw [ih]

private theorem scanFrom_shift (n s acc x : Int) (ys : List Int) :
    scanFrom (n+1) (s+x) acc ys = scanFrom n s acc ys + (ys.map fun y => y - x).sum := by
  induction ys generalizing n s acc with
  | nil => simp [scanFrom]
  | cons y ys ih =>
      change scanFrom (n+1+1) (s+x+y) (acc+(n+1)*y-(s+x)) ys =
        scanFrom (n+1) (s+y) (acc+n*y-s) ys + ((y-x) + (ys.map fun y => y-x).sum)
      have hacc : acc + (n + 1) * y - (s + x) = (acc + n * y - s) + (y - x) := by grind
      rw [hacc]
      rw [show s + x + y = (s + y) + x by omega]
      rw [scanFrom_acc_add]
      rw [ih]
      omega

private theorem distanceScan_cons (x : Int) (xs : List Int) :
    distanceScan (x :: xs) = distanceScan xs + (xs.map fun y => y - x).sum := by
  rw [show distanceScan (x :: xs) = scanFrom (0+1) (0+x) 0 xs by
    simp [distanceScan, scanFrom]]
  rw [show distanceScan xs = scanFrom 0 0 0 xs by
    simp [distanceScan, scanFrom]]
  simpa using scanFrom_shift 0 0 0 x xs

private theorem sum_map_add_int {α : Type} (l : List α) (f g : α → Int) :
    (l.map fun t => f t + g t).sum = (l.map f).sum + (l.map g).sum := by
  induction l with
  | nil => rfl
  | cons a l ih => simp [ih, Int.add_assoc, Int.add_comm, Int.add_left_comm]

private theorem pairs_cons_cons (x : Int) (s : List Int) :
    pairs (x::s) (x::s) (fun x y => (x-y).natAbs) =
      (s.map fun y => (((x-y).natAbs : Nat) : Int)).sum +
      (s.map fun y => (((y-x).natAbs : Nat) : Int)).sum +
      pairs s s (fun x y => (x-y).natAbs) := by
  unfold pairs
  simp only [List.map_cons, List.sum_cons]
  rw [sum_map_add_int]
  simp [Int.add_assoc, Int.add_comm, Int.add_left_comm]

private theorem sorted_mergeSort (xs : List Int) : (xs.mergeSort).Pairwise (fun a b => a ≤ b) := by
  have h := List.pairwise_mergeSort (le := fun a b : Int => a ≤ b)
    (by intro a b c hab hbc
        have hab' : a ≤ b := of_decide_eq_true hab
        have hbc' : b ≤ c := of_decide_eq_true hbc
        exact decide_eq_true (by omega))
    (by intro a b
        by_cases h : a ≤ b
        · simp [h]
        · have h' : b ≤ a := by omega
          simp [h']) xs
  exact h.imp (by intro a b hab; exact of_decide_eq_true hab)

private theorem distanceScan_sorted_abs (s : List Int) (hs : s.Pairwise (fun a b => a ≤ b)) :
    2 * distanceScan s = pairs s s (fun x y => (x-y).natAbs) := by
  induction s with
  | nil => simp [distanceScan, pairs]
  | cons x s ih =>
      have hsTail : s.Pairwise (fun a b => a ≤ b) := List.Pairwise.of_cons hs
      have hxle : ∀ y ∈ s, x ≤ y := (List.pairwise_cons.mp hs).1
      rw [distanceScan_cons, pairs_cons_cons]
      rw [show 2 * (distanceScan s + (s.map fun y => y - x).sum) =
          2 * distanceScan s + 2 * (s.map fun y => y - x).sum by omega]
      rw [ih hsTail]
      have h1 : ∀ y ∈ s, (((x-y).natAbs : Nat) : Int) = y - x := by
        intro y hy
        have hxy : x ≤ y := hxle y hy
        have hnon : 0 ≤ y - x := by omega
        have hneg : x - y = -(y - x) := by omega
        rw [hneg, Int.natAbs_neg, Int.natAbs_of_nonneg hnon]
      have h2 : ∀ y ∈ s, (((y-x).natAbs : Nat) : Int) = y - x := by
        intro y hy
        exact Int.natAbs_of_nonneg (by have := hxle y hy; omega)
      have hs1 : (s.map fun y => ((x-y).natAbs : Nat) : List Int).sum = (s.map fun y => y - x).sum := by
        apply congrArg List.sum
        apply List.map_congr_left
        intro y hy
        exact h1 y hy
      have hs2 : (s.map fun y => ((y-x).natAbs : Nat) : List Int).sum = (s.map fun y => y - x).sum := by
        apply congrArg List.sum
        apply List.map_congr_left
        intro y hy
        exact h2 y hy
      rw [hs1, hs2]
      omega

theorem AbsoluteDistance_sound : TinylambOpt.ResearchGoals.PairAggregates.AbsoluteDistance.Goal := by
  intro xs
  let s := xs.mergeSort
  have hs : s.Pairwise (fun a b => a ≤ b) := sorted_mergeSort xs
  have hmain := distanceScan_sorted_abs s hs
  have hp : s.Perm xs := List.mergeSort_perm xs (fun a b : Int => a ≤ b)
  calc
    2 * distanceScan (xs.mergeSort) = 2 * distanceScan s := rfl
    _ = pairs s s (fun x y => (x-y).natAbs) := hmain
    _ = pairs xs xs (fun x y => (x-y).natAbs) := by
      calc
        pairs s s (fun x y => (x-y).natAbs) = pairs s xs (fun x y => (x-y).natAbs) := pairs_perm_right _ s hp
        _ = pairs xs xs (fun x y => (x-y).natAbs) := (pairs_perm_left _ hp.symm xs).symm

private def histFrom {α : Type} [BEq α] [Hashable α] (h : Std.HashMap α Nat) (xs : List α) : Std.HashMap α Nat :=
  xs.foldl (fun h x => h.insert x (h.getD x 0 + 1)) h

set_option pp.all false in

private theorem histFrom_getD {α : Type} [BEq α] [Hashable α] [EquivBEq α] [LawfulHashable α]
    (h : Std.HashMap α Nat) (xs : List α) (k : α) :
    (histFrom h xs).getD k 0 = h.getD k 0 + (xs.filter fun y => y == k).length := by
  induction xs generalizing h with
  | nil => simp [histFrom]
  | cons x xs ih =>
      simp only [histFrom, List.foldl_cons]
      change (histFrom (h.insert x (h.getD x 0 + 1)) xs).getD k 0 =
        h.getD k 0 + (List.filter (fun y => y == k) (x :: xs)).length
      rw [ih]
      by_cases hx : (x == k) = true
      · have hcongr : h.getD x 0 = h.getD k 0 := Std.HashMap.getD_congr hx
        simp [Std.HashMap.getD_insert, hx, hcongr]; omega
      · simp [Std.HashMap.getD_insert, hx]

private theorem hist_getD_count {α : Type} [BEq α] [Hashable α] [EquivBEq α] [LawfulHashable α]
    (xs : List α) (k : α) : (hist xs).getD k 0 = (xs.filter fun y => y == k).length := by
  simpa [hist, histFrom] using histFrom_getD ({} : Std.HashMap α Nat) xs k

private theorem sum_sqdiff (x : Int) (ys : List Int) :
    (ys.map fun y => (x - y)^2).sum = (ys.length : Int) * (x*x) - 2*x*ys.sum + squares ys := by
  induction ys with
  | nil => simp [squares]
  | cons y ys ih =>
      simp only [List.map_cons, List.sum_cons, List.length_cons]
      rw [ih]
      have hs : squares (y :: ys) = y*y + squares ys := by simp [squares]
      rw [hs]
      grind

theorem SquaredDistance_sound : TinylambOpt.ResearchGoals.PairAggregates.SquaredDistance.Goal := by
  intro xs ys
  induction xs with
  | nil => simp [pairs, squares]
  | cons x xs ih =>
      change (ys.length : Int) * squares (x :: xs) + ((xs.length + 1 : Nat) : Int) * squares ys -
          2 * (x + xs.sum) * ys.sum =
        (ys.map fun y => (x - y)^2).sum + pairs xs ys (fun x y => (x-y)^2)
      rw [← ih, sum_sqdiff]
      have hs : squares (x :: xs) = x*x + squares xs := by simp [squares]
      rw [hs]
      have hlen : ((xs.length + 1 : Nat) : Int) = (xs.length : Int) + 1 := by omega
      rw [hlen]
      grind

private theorem filter_eq_complement (x t : Int) (ys : List Int) :
    (ys.filter fun y => y == (t - x)).length = (ys.filter fun y => x + y == t).length := by
  induction ys with
  | nil => rfl
  | cons y ys ih =>
      by_cases h1 : y == (t - x)
      · have h2 : x + y == t := by
          apply decide_eq_true
          have : y = t - x := by exact of_decide_eq_true h1
          omega
        simp [h1, h2, ih]
      · have h2 : (x + y == t) = false := by
          cases hb : (x + y == t)
          · rfl
          · exfalso
            have ht : x + y = t := of_decide_eq_true hb
            apply h1
            apply decide_eq_true
            omega
        simp [h1, h2, ih]

theorem ComplementProbe_sound : TinylambOpt.ResearchGoals.PairAggregates.ComplementProbe.Goal := by
  intro xs ys t
  induction xs with
  | nil => rfl
  | cons x xs ih =>
      change (hist ys).getD (t - x) 0 + (xs.map fun x => (hist ys).getD (t - x) 0).sum =
        (ys.filter fun y => x + y == t).length + (xs.map fun x => (ys.filter fun y => x + y == t).length).sum
      rw [ih]
      rw [hist_getD_count]
      rw [filter_eq_complement]

private theorem row_filter_subset (x : Row) (r : List Row) :
    (r.filter fun y => y == x).length ≤ (r.filter fun y => y.1 == x.1).length := by
  induction r with
  | nil => simp
  | cons y r ih =>
      by_cases hrow : y == x
      · have hten : y.1 == x.1 := by
          have hyx : y = x := beq_iff_eq.mp hrow
          rw [hyx]
          simp
        simp [hrow, hten, ih]
      · have hrow_f : (y == x) = false := by simpa using hrow
        by_cases hten : y.1 == x.1
        · simp [hrow_f, hten, ih]
          omega
        · have hten_f : (y.1 == x.1) = false := by simpa using hten
          simp [hrow_f, hten_f, ih]

private theorem tenant_sub_row (x : Row) (r : List Row) :
    (r.filter fun y => y.1 == x.1).length - (r.filter fun y => y == x).length =
      (r.filter fun y => x.1 == y.1 && x.2 != y.2).length := by
  induction r with
  | nil => rfl
  | cons y r ih =>
      by_cases hten : y.1 == x.1
      · have hten_rev : x.1 == y.1 := by
          have : y.1 = x.1 := beq_iff_eq.mp hten
          exact beq_iff_eq.mpr (by omega)
        by_cases hrow : y == x
        · have huser_false : (x.2 != y.2) = false := by
            have hyx : y = x := beq_iff_eq.mp hrow
            rw [hyx]
            simp
          simp [hten, hten_rev, hrow, huser_false, ih]
        · have hrow_f : (y == x) = false := by simpa using hrow
          have huser_true : (x.2 != y.2) = true := by
            have hyne : y ≠ x := by intro h; apply hrow; exact beq_iff_eq.mpr h
            have h1eq : y.1 = x.1 := beq_iff_eq.mp hten
            have h2ne : x.2 ≠ y.2 := by
              intro h2
              apply hyne
              cases y with
              | mk y1 y2 =>
                cases x with
                | mk x1 x2 =>
                  simp at h1eq h2 ⊢
                  omega
            simpa using h2ne
          simp [hten, hten_rev, hrow_f, huser_true]
          have hle := row_filter_subset x r
          omega
      · have hten_f : (y.1 == x.1) = false := by simpa using hten
        have hrow_f : (y == x) = false := by
          by_cases hb : y == x
          · exfalso
            apply hten
            have hyx : y = x := beq_iff_eq.mp hb
            rw [hyx]
            simp
          · simpa using hb
        have hrev : (x.1 == y.1) = false := by
          by_cases hb : x.1 == y.1
          · exfalso
            have : x.1 = y.1 := beq_iff_eq.mp hb
            apply hten
            exact beq_iff_eq.mpr (by omega)
          · simpa using hb
        simp [hten_f, hrow_f, hrev, ih]

private theorem map_filter_fst (x : Row) (r : List Row) :
    ((r.map Prod.fst).filter fun t => t == x.1).length =
      (r.filter fun y => y.1 == x.1).length := by
  induction r with
  | nil => rfl
  | cons y r ih =>
      by_cases h : y.1 == x.1
      · simp [h, ih]
      · have hf : (y.1 == x.1) = false := by simpa using h
        simp [hf, ih]

theorem ResidualInequality_sound : TinylambOpt.ResearchGoals.PairAggregates.ResidualInequality.Goal := by
  intro l r
  induction l with
  | nil => simp [differentUsers]
  | cons x l ih =>
      change ((hist (r.map Prod.fst)).getD x.1 0 - (hist r).getD x 0) +
          (l.map fun x => (hist (r.map Prod.fst)).getD x.1 0 - (hist r).getD x 0).sum =
        (r.filter fun y => x.1 == y.1 && x.2 != y.2).length +
          (l.map fun x => (r.filter fun y => x.1 == y.1 && x.2 != y.2).length).sum
      have ih' : (l.map fun x => (hist (r.map Prod.fst)).getD x.1 0 - (hist r).getD x 0).sum =
          (l.map fun x => (r.filter fun y => x.1 == y.1 && x.2 != y.2).length).sum := by
        simpa [differentUsers] using ih
      rw [ih']
      rw [hist_getD_count, hist_getD_count, map_filter_fst, tenant_sub_row]

private theorem sum_map_zero (l : List Nat) : (l.map fun _ => 0).sum = 0 := by
  induction l with
  | nil => rfl
  | cons a l ih => simpa using ih

private theorem filter_range_min_length (n m : Nat) :
    ((List.range n).filter fun t => t < m).length = min n m := by
  induction n with
  | zero => simp
  | succ n ih =>
      rw [List.range_succ, List.filter_append]
      by_cases h : n < m
      · have h1 : min (n + 1) m = n + 1 := by omega
        have h2 : min n m = n := by omega
        simp [h, ih, h1, h2]
      · have h1 : min (n + 1) m = min n m := by omega
        simp [h, ih, h1]

private theorem min_survival (x y cap : Nat) (hx : x ≤ cap) :
    ((List.range cap).filter fun t => t < x && t < y).length = min x y := by
  have hpred : ∀ t : Nat, (decide (t < x) && decide (t < y)) = decide (t < min x y) := by
    intro t
    by_cases h1 : t < x <;> by_cases h2 : t < y <;> by_cases h3 : t < min x y <;> simp_all [Nat.lt_min]
  have hf : (List.range cap).filter (fun t => t < x && t < y) =
      (List.range cap).filter (fun t => t < min x y) := by
    apply List.filter_congr
    intro t ht
    exact hpred t
  rw [hf, filter_range_min_length]
  have hle : min x y ≤ cap := Nat.le_trans (Nat.min_le_left x y) hx
  exact Nat.min_eq_right hle

private theorem sum_map_add_nat (l : List Nat) (f g : Nat → Nat) :
    (l.map fun t => f t + g t).sum = (l.map f).sum + (l.map g).sum := by
  induction l with
  | nil => rfl
  | cons a l ih => simp [ih, Nat.add_assoc, Nat.add_left_comm]

private theorem sum_indicator_filter (l : List Nat) (p : Nat → Bool) :
    (l.map fun t => if p t then 1 else 0).sum = (l.filter p).length := by
  induction l with
  | nil => rfl
  | cons a l ih =>
      by_cases h : p a = true <;> simp [h, ih] <;> omega

private theorem sum_mul_indicator_filter (l : List Nat) (p : Nat → Bool) (w : Nat → Nat) :
    (l.map fun t => (if p t then 1 else 0) * w t).sum =
      ((l.filter p).map fun t => w t).sum := by
  induction l with
  | nil => rfl
  | cons a l ih =>
      by_cases h : p a = true <;> simp [h, ih] <;> omega

private theorem survival_sum (x cap : Nat) (hx : x ≤ cap) (b : List Nat) :
    (b.map fun y => min x y).sum =
      (((List.range cap).filter fun t => t < x).map fun t =>
        (b.filter fun y => t < y).length).sum := by
  induction b with
  | nil => simp [sum_map_zero]
  | cons y b ih =>
      change min x y + (b.map fun y => min x y).sum =
        (((List.range cap).filter fun t => t < x).map fun t =>
          ((y :: b).filter fun v => t < v).length).sum
      rw [ih]
      have hfilter : (((List.range cap).filter fun t => t < x).map fun t =>
          ((y :: b).filter fun v => t < v).length) =
          (((List.range cap).filter fun t => t < x).map fun t =>
            (if decide (t < y) then 1 else 0) + (b.filter fun v => t < v).length) := by
        apply List.map_congr_left
        intro t ht
        simp [List.filter_cons]
        by_cases h : t < y <;> simp [h] <;> omega
      rw [hfilter, sum_map_add_nat]
      have hind :
          (((List.range cap).filter fun t => t < x).map fun t =>
            (if decide (t < y) then 1 else 0)).sum =
          (((List.range cap).filter fun t => t < x).filter fun t => t < y).length := by
        exact sum_indicator_filter ((List.range cap).filter fun t => t < x) (fun t => t < y)
      rw [hind]
      have hff : ((List.range cap).filter fun t => t < x).filter (fun t => t < y) =
          (List.range cap).filter (fun t => t < x && t < y) := by
        induction List.range cap with
        | nil => rfl
        | cons t ts iht => by_cases htx : t < x <;> by_cases hty : t < y <;> simp [htx, hty, iht]
      rw [hff, min_survival x y cap hx]

theorem LayerCakeMinimum_sound : TinylambOpt.ResearchGoals.PairAggregates.LayerCakeMinimum.Goal := by
  intro a b cap ha
  induction a with
  | nil => simp [sum_map_zero]
  | cons x a ih =>
      have hx : x ≤ cap := ha x (by simp)
      have htail : ∀ y ∈ a, y ≤ cap := by intro y hy; exact ha y (by simp [hy])
      have iha := ih htail
      change ((List.range cap).map fun t => ((x :: a).filter fun v => t < v).length *
          (b.filter fun v => t < v).length).sum =
        (b.map fun y => min x y).sum + (a.map fun x => (b.map (min x)).sum).sum
      rw [← iha]
      have hfilter : ((List.range cap).map fun t => ((x :: a).filter fun v => t < v).length *
          (b.filter fun v => t < v).length) =
          (List.range cap).map fun t =>
            (if decide (t < x) then 1 else 0) * (b.filter fun v => t < v).length +
              ((a.filter fun v => t < v).length * (b.filter fun v => t < v).length) := by
        apply List.map_congr_left
        intro t ht
        simp [List.filter_cons]
        by_cases htx : t < x <;> simp [htx]
        grind
      rw [hfilter, sum_map_add_nat]
      have hleft : ((List.range cap).map fun t =>
          (if decide (t < x) then 1 else 0) * (b.filter fun v => t < v).length).sum =
          (((List.range cap).filter fun t => t < x).map fun t =>
            (b.filter fun v => t < v).length).sum := by
        exact sum_mul_indicator_filter (List.range cap) (fun t => t < x)
          (fun t => (b.filter fun v => t < v).length)
      rw [hleft, survival_sum x cap hx b]

private theorem sum_map_sum_range (ys : List UInt8) (f : Nat → UInt8 → Nat) :
    (ys.map fun y => ((List.range 8).map fun i => f i y).sum).sum =
      ((List.range 8).map fun i => (ys.map fun y => f i y).sum).sum := by
  induction ys with
  | nil => simp [sum_map_zero]
  | cons y ys ih =>
      change ((List.range 8).map fun i => f i y).sum +
        (ys.map fun y => ((List.range 8).map fun i => f i y).sum).sum =
        ((List.range 8).map fun i => (f i y + (ys.map fun y => f i y).sum)).sum
      rw [ih, sum_map_add_nat]

private theorem sum_sum_range_two (xs ys : List UInt8) (f : Nat → UInt8 → UInt8 → Nat) :
    (xs.map fun x => (ys.map fun y => ((List.range 8).map fun i => f i x y).sum).sum).sum =
      ((List.range 8).map fun i => (xs.map fun x => (ys.map fun y => f i x y).sum).sum).sum := by
  induction xs with
  | nil => simp [sum_map_zero]
  | cons x xs ih =>
      change (ys.map fun y => ((List.range 8).map fun i => f i x y).sum).sum +
        (xs.map fun x => (ys.map fun y => ((List.range 8).map fun i => f i x y).sum).sum).sum =
        ((List.range 8).map fun i =>
          (ys.map fun y => f i x y).sum + (xs.map fun x => (ys.map fun y => f i x y).sum).sum).sum
      rw [sum_map_sum_range, ih, sum_map_add_nat]

private theorem xor_bits (x y : UInt8) :
    (x ^^^ y).toNat =
      ((List.range 8).map fun i => if (x.toNat.testBit i ^^ y.toNat.testBit i) then 2^i else 0).sum := by
  revert x y
  native_decide

private def bitCount (i : Nat) (xs : List UInt8) :=
  (xs.filter fun x => x.toNat.testBit i).length

private theorem bit_count_cons (i : Nat) (x : UInt8) (xs : List UInt8) :
    bitCount i (x :: xs) = bitCount i xs + (if x.toNat.testBit i then 1 else 0) := by
  by_cases h : x.toNat.testBit i <;> simp [bitCount, h]

private theorem bit_count_le (i : Nat) (xs : List UInt8) : bitCount i xs ≤ xs.length := by
  induction xs with
  | nil => simp [bitCount]
  | cons x xs ih =>
      rw [bit_count_cons]
      by_cases h : x.toNat.testBit i <;> simp [h] <;> omega

private theorem not_bit_count (i : Nat) (xs : List UInt8) :
    xs.length - bitCount i xs = (xs.filter fun x => !(x.toNat.testBit i)).length := by
  induction xs with
  | nil => rfl
  | cons x xs ih =>
      rw [bit_count_cons]
      have hle := bit_count_le i xs
      by_cases h : x.toNat.testBit i <;> simp [h] <;> omega

private theorem bit_row_true (i : Nat) (ys : List UInt8) :
    (ys.map fun y => if y.toNat.testBit i then 2^i else 0).sum = bitCount i ys * 2^i := by
  induction ys with
  | nil => simp [bitCount]
  | cons y ys ih =>
      rw [bit_count_cons]
      simp only [List.map_cons, List.sum_cons]
      rw [ih]
      by_cases h : y.toNat.testBit i
      · simp [h, Nat.add_mul, Nat.add_comm, Nat.add_left_comm]
      · simp [h, Nat.add_comm, Nat.add_left_comm]

private theorem bit_row_false (i : Nat) (ys : List UInt8) :
    (ys.map fun y => if y.toNat.testBit i then 0 else 2^i).sum =
      (ys.length - bitCount i ys) * 2^i := by
  induction ys with
  | nil => simp [bitCount]
  | cons y ys ih =>
      rw [bit_count_cons]
      simp only [List.map_cons, List.sum_cons]
      rw [ih]
      have hle := bit_count_le i ys
      by_cases h : y.toNat.testBit i
      · have hsub : ys.length + 1 - (bitCount i ys + 1) = ys.length - bitCount i ys := by omega
        simp [h, hsub]
      · have hsub : ys.length + 1 - bitCount i ys = (ys.length - bitCount i ys) + 1 := by omega
        simp [h, hsub, Nat.add_mul, Nat.add_comm, Nat.add_left_comm]

private theorem bit_row_false' (i : Nat) (ys : List UInt8) :
    (ys.map fun y => if y.toNat.testBit i = false then 2^i else 0).sum =
      (ys.length - bitCount i ys) * 2^i := by
  rw [show (ys.map fun y => if y.toNat.testBit i = false then 2^i else 0) =
      (ys.map fun y => if y.toNat.testBit i then 0 else 2^i) by
    apply List.map_congr_left
    intro y hy
    by_cases h : y.toNat.testBit i <;> simp [h]]
  exact bit_row_false i ys

private theorem bit_row (i : Nat) (x : UInt8) (ys : List UInt8) :
    (ys.map fun y => if (x.toNat.testBit i ^^ y.toNat.testBit i) then 2^i else 0).sum =
      if x.toNat.testBit i then (ys.length - bitCount i ys) * 2^i else bitCount i ys * 2^i := by
  by_cases hx : x.toNat.testBit i
  · simp [hx, bit_row_false']
  · simp [hx, bit_row_true]

private theorem bit_term (i : Nat) (xs ys : List UInt8) :
    2^i * (bitCount i xs * (ys.length - bitCount i ys) +
      (xs.length - bitCount i xs) * bitCount i ys) =
      (xs.map fun x => (ys.map fun y =>
        if (x.toNat.testBit i ^^ y.toNat.testBit i) then 2^i else 0).sum).sum := by
  induction xs with
  | nil => simp [bitCount]
  | cons x xs ih =>
      rw [bit_count_cons]
      simp only [List.length_cons, List.map_cons, List.sum_cons]
      rw [bit_row]
      rw [← ih]
      have hle := bit_count_le i xs
      have hsubT : xs.length + 1 - (bitCount i xs + 1) = xs.length - bitCount i xs := by omega
      have hsubT2 : xs.length + 1 - (1 + bitCount i xs) = xs.length - bitCount i xs := by omega
      have hsubF : xs.length + 1 - bitCount i xs = (xs.length - bitCount i xs) + 1 := by omega
      by_cases hx : x.toNat.testBit i
      · simp [hx, hsubT2, Nat.mul_add, Nat.add_mul, Nat.mul_comm, Nat.add_assoc, Nat.add_comm, Nat.add_left_comm]
      · simp [hx, hsubF, Nat.mul_add, Nat.add_mul, Nat.mul_comm, Nat.add_assoc, Nat.add_comm, Nat.add_left_comm]

theorem XorDistance_sound : TinylambOpt.ResearchGoals.PairAggregates.XorDistance.Goal := by
  intro xs ys
  change ((List.range 8).map fun i =>
    let a := (xs.filter fun x => x.toNat.testBit i).length
    let b := (ys.filter fun y => y.toNat.testBit i).length
    2^i * (a*(ys.length-b) + (xs.length-a)*b)).sum =
    (xs.map fun x => (ys.map fun y => (x ^^^ y).toNat).sum).sum
  rw [show (xs.map fun x => (ys.map fun y => (x ^^^ y).toNat).sum).sum =
      (xs.map fun x => (ys.map fun y =>
        ((List.range 8).map fun i => if (x.toNat.testBit i ^^ y.toNat.testBit i) then 2^i else 0).sum).sum).sum by
        apply congrArg List.sum
        apply List.map_congr_left
        intro x hx
        apply congrArg List.sum
        apply List.map_congr_left
        intro y hy
        exact xor_bits x y]
  rw [sum_sum_range_two xs ys (fun i x y => if (x.toNat.testBit i ^^ y.toNat.testBit i) then 2^i else 0)]
  apply congrArg List.sum
  apply List.map_congr_left
  intro i hi
  simpa [bitCount] using (bit_term i xs ys)


private def keepPred {α : Type} [BEq α] (k : α) (e : α × Nat) : Bool := decide ¬(k == e.1)

private theorem perm_map_sum_nat {α : Type} (f : α → Nat) {l₁ l₂ : List α} (p : l₁.Perm l₂) :
    (l₁.map f).sum = (l₂.map f).sum := by
  induction p with
  | nil => rfl
  | cons a p ih => simpa using congrArg (fun s => f a + s) ih
  | swap a b l => simp [Nat.add_left_comm]
  | trans p1 p2 ih1 ih2 => exact ih1.trans ih2

private theorem hashMap_toList_entry {α : Type} [BEq α] [Hashable α] [EquivBEq α] [LawfulHashable α]
    (m : Std.HashMap α Nat) : ∀ e ∈ m.toList, m.getD e.1 0 = e.2 := by
  intro e he
  rw [Std.HashMap.mem_toList_iff_getKey?_eq_some_and_getElem?_eq_some] at he
  rcases he with ⟨_, he2⟩
  simp [Std.HashMap.getD_eq_getD_getElem?, he2]

private theorem histFrom_toList_pos {α : Type} [BEq α] [Hashable α] [EquivBEq α] [LawfulHashable α]
    (h : Std.HashMap α Nat) (xs : List α)
    (hpos : ∀ e ∈ h.toList, 0 < e.2) :
    ∀ e ∈ (histFrom h xs).toList, 0 < e.2 := by
  induction xs generalizing h with
  | nil => simpa [histFrom] using hpos
  | cons x xs ih =>
      simp only [histFrom, List.foldl_cons]
      change ∀ e ∈ (histFrom (h.insert x (h.getD x 0 + 1)) xs).toList, 0 < e.2
      apply ih
      intro e he
      have hp := Std.HashMap.toList_insert_perm (m := h) (k := x) (v := h.getD x 0 + 1)
      have he' := hp.mem_iff.mp he
      simp only [List.mem_cons, List.mem_filter] at he'
      rcases he' with rfl | ⟨hein, hkey⟩
      · omega
      · exact hpos e hein

private theorem hist_toList_pos {α : Type} [BEq α] [Hashable α] [EquivBEq α] [LawfulHashable α]
    (xs : List α) : ∀ e ∈ (hist xs).toList, 0 < e.2 := by
  simpa [hist, histFrom] using histFrom_toList_pos ({} : Std.HashMap α Nat) xs (by simp [Std.HashMap.toList_empty])


private theorem toList_sum_decomp {α : Type} [BEq α] [LawfulBEq α]
    (l : List (α × Nat)) (k : α) (v : Nat) (ge : α × Nat → Nat)
    (huniq : l.Pairwise (fun a b => (a.1 == b.1) = false))
    (hex : ∃ e ∈ l, (e.1 == k) = true ∧ e.2 = v) :
    (l.map ge).sum =
      ge (k, v) + ((l.filter (keepPred k)).map ge).sum := by
  induction l with
  | nil => simp at hex
  | cons e es ih =>
      by_cases hke : (k == e.1) = true
      · have hek : (e.1 == k) = true := by
          have hEq : k = e.1 := beq_iff_eq.mp hke
          exact beq_iff_eq.mpr hEq.symm
        have hev : e.2 = v := by
          rcases hex with ⟨e', he'in, he'k, he'v⟩
          simp at he'in
          rcases he'in with rfl | he'es
          · exact he'v
          · exfalso
            have hd : (e.1 == e'.1) = false := (List.pairwise_cons.mp huniq).1 e' he'es
            have hEq1 : e'.1 = k := beq_iff_eq.mp he'k
            have hEq2 : k = e.1 := beq_iff_eq.mp hke
            have hEq3 : e.1 = e'.1 := hEq2.symm.trans hEq1.symm
            rw [hEq3] at hd
            have herr : (e'.1 == e'.1) = true := beq_iff_eq.mpr rfl
            rw [herr] at hd
            cases hd
        have htail : (es.filter (keepPred k)) = es := by
          apply List.filter_eq_self.mpr
          intro a ha
          have hkf : (k == a.1) = false := by
            by_cases hb : (k == a.1) = true
            · exfalso
              have hEq1 : k = a.1 := beq_iff_eq.mp hb
              have hEq2 : k = e.1 := beq_iff_eq.mp hke
              have hEq3 : e.1 = a.1 := hEq2.symm.trans hEq1
              have hd : (e.1 == a.1) = false := (List.pairwise_cons.mp huniq).1 a ha
              rw [hEq3] at hd
              have herr : (a.1 == a.1) = true := beq_iff_eq.mpr rfl
              rw [herr] at hd
              cases hd
            · cases hb2 : (k == a.1)
              · rfl
              · exact (hb hb2).elim
          simp [keepPred, hkf]
        have hkp : keepPred k e = false := by simp [keepPred, hke]
        have heEq : e = (k, v) := by
          cases e
          simp_all [beq_iff_eq.mp hek]
        have hkp' : keepPred k (k, v) = false := by simp [keepPred]
        simp [hkp', htail, heEq]
      · have hke_f : (k == e.1) = false := by
          cases hb : (k == e.1)
          · rfl
          · exfalso; exact hke hb
        have hexTail : ∃ e' ∈ es, (e'.1 == k) = true ∧ e'.2 = v := by
          rcases hex with ⟨e', he'in, he'k, he'v⟩
          simp at he'in
          rcases he'in with rfl | he'es
          · exfalso
            apply hke
            have hEq : e'.1 = k := beq_iff_eq.mp he'k
            exact beq_iff_eq.mpr hEq.symm
          · exact ⟨e', he'es, he'k, he'v⟩
        have hkp : keepPred k e = true := by simp [keepPred, hke_f]
        simp [hkp]
        rw [ih (List.Pairwise.of_cons huniq) hexTail]
        omega

private theorem toList_sum_insert {α : Type} [BEq α] [Hashable α] [EquivBEq α] [LawfulHashable α] [LawfulBEq α]
    (m : Std.HashMap α Nat) (k : α) (ge : α × Nat → Nat) (hfun : Nat → Nat)
    (hrel : ∀ n, ge (k, n) = (fun n => ge (k, n)) n)
    (hg0 : ge (k, 0) = 0) (hgsucc : ∀ n, ge (k, n+1) = ge (k, n) + hfun n)
    (hpos : ∀ e ∈ m.toList, 0 < e.2)
    (hentry : ∀ e ∈ m.toList, m.getD e.1 0 = e.2) :
    (((m.insert k (m.getD k 0 + 1)).toList).map ge).sum =
      ((m.toList.map ge).sum) + hfun (m.getD k 0) := by
  let v := m.getD k 0
  have hp := Std.HashMap.toList_insert_perm (m := m) (k := k) (v := v + 1)
  rw [perm_map_sum_nat ge hp]
  simp only [List.map_cons, List.sum_cons]
  rw [hgsucc v]
  by_cases hv : v = 0
  · subst v
    have hfilt : m.toList.filter (keepPred k) = m.toList := by
      apply List.filter_eq_self.mpr
      intro e he
      have hkf : (k == e.1) = false := by
        by_cases hb : (k == e.1) = true
        · exfalso
          have hcongr := Std.HashMap.getD_congr (m := m) (fallback := 0) hb
          have hen := hentry e he
          have hpE := hpos e he
          have : m.getD e.1 0 = 0 := by rw [← hcongr]; exact hv
          omega
        · cases hb2 : (k == e.1)
          · rfl
          · exact (hb hb2).elim
      simp [keepPred, hkf]
    have hfilt' : m.toList.filter (fun x => decide ¬(k == x.fst) = true) = m.toList := by
      simpa [keepPred] using hfilt
    rw [hfilt', hv, hg0]
    omega
  · have hvpos : 0 < v := by omega
    have hmem : k ∈ m := by
      by_cases hm : k ∈ m
      · exact hm
      · exfalso
        have hz := Std.HashMap.getD_eq_fallback (m := m) (a := k) (fallback := 0) hm
        have : v = 0 := by simpa [v] using hz
        omega
    have hget : m[k]? = some v := by
      have ht := Std.HashMap.getElem?_eq_some_getD (m := m) (a := k) (fallback := 0) hmem
      simpa [v] using ht
    have hexists := (Std.HashMap.getElem?_eq_some_iff_exists_beq_and_mem_toList (m := m) (k := k) (v := v)).mp hget
    rcases hexists with ⟨k', hbeq, hkin⟩
    have hk'k : (k' == k) = true := by
      have hEq : k = k' := beq_iff_eq.mp hbeq
      exact beq_iff_eq.mpr hEq.symm
    have hex : ∃ e ∈ m.toList, (e.1 == k) = true ∧ e.2 = v := ⟨(k', v), hkin, hk'k, rfl⟩
    have hdec := toList_sum_decomp m.toList k v ge (Std.HashMap.distinct_keys_toList (m := m)) hex
    have hfiltEq : m.toList.filter (keepPred k) = m.toList.filter (fun x => !(k == x.fst)) := by
      apply List.filter_congr
      intro e he
      simp [keepPred]
    have hdec' : (m.toList.map ge).sum =
        ge (k, v) + ((m.toList.filter (fun x => !(k == x.fst))).map ge).sum := by
      rw [← hfiltEq]
      exact hdec
    rw [hdec']
    simp [v]
    omega


private theorem toList_insert_pos {α : Type} [BEq α] [Hashable α] [EquivBEq α] [LawfulHashable α]
    (m : Std.HashMap α Nat) (k : α) (v : Nat)
    (hv : 0 < v)
    (hpos : ∀ e ∈ m.toList, 0 < e.2) :
    ∀ e ∈ (m.insert k v).toList, 0 < e.2 := by
  intro e he
  have hp := Std.HashMap.toList_insert_perm (m := m) (k := k) (v := v)
  have he' := hp.mem_iff.mp he
  simp only [List.mem_cons, List.mem_filter] at he'
  rcases he' with rfl | ⟨hein, hkey⟩
  · exact hv
  · exact hpos e hein

private theorem toList_insert_entry {α : Type} [BEq α] [Hashable α] [EquivBEq α] [LawfulHashable α]
    (m : Std.HashMap α Nat) (k : α) (v : Nat)
    (hentry : ∀ e ∈ m.toList, m.getD e.1 0 = e.2) :
    ∀ e ∈ (m.insert k v).toList, (m.insert k v).getD e.1 0 = e.2 := by
  intro e he
  have hp := Std.HashMap.toList_insert_perm (m := m) (k := k) (v := v)
  have he' := hp.mem_iff.mp he
  simp only [List.mem_cons, List.mem_filter] at he'
  rcases he' with rfl | ⟨hein, hkey⟩
  · simp [Std.HashMap.getD_insert]
  · rw [Std.HashMap.getD_insert]
    have hkf : (k == e.1) = false := by
      cases hb : (k == e.1)
      · rfl
      · simp [hb] at hkey
    simp [hkf, hentry e hein]

private theorem insert_getD_add {α : Type} [BEq α] [Hashable α] [EquivBEq α] [LawfulHashable α]
    (m : Std.HashMap α Nat) (x y : α) :
    (m.insert x (m.getD x 0 + 1)).getD y 0 = m.getD y 0 + (if x == y then 1 else 0) := by
  rw [Std.HashMap.getD_insert]
  by_cases hxy : x == y
  · simp [hxy]
    have hcongr := Std.HashMap.getD_congr (m := m) (fallback := 0) hxy
    rw [hcongr]
    
  · simp [hxy]

private theorem count_take_succ_cons {α : Type} [BEq α] [LawfulBEq α] (x y : α) (xs : List α) (i : Nat) :
    (((x :: xs).take (i+1)).filter fun z => y == z).length =
      (if x == y then 1 else 0) + ((xs.take i).filter fun z => y == z).length := by
  have hsym : (x == y) = (y == x) := by
    by_cases h : x == y
    · have hy : y == x := beq_iff_eq.mpr (beq_iff_eq.mp h).symm
      simp [h, hy]
    · have hy : (y == x) = false := by
        by_cases h2 : y == x
        · have h' : x == y := beq_iff_eq.mpr (beq_iff_eq.mp h2).symm
          exact (h h').elim
        · cases h2' : (y == x)
          · rfl
          · exact (h2 h2').elim
      simp [h, hy]
  rw [hsym]
  rw [List.take_succ_cons]
  simp only [List.filter_cons]
  by_cases hyx : y == x <;> simp [hyx] <;> omega

private theorem zipIdx_add {α : Type} (xs : List α) (n : Nat) :
    xs.zipIdx (n+1) = (xs.zipIdx n).map fun p => (p.1, p.2 + 1) := by
  induction xs generalizing n with
  | nil => rfl
  | cons a as ih =>
      rw [List.zipIdx_cons, List.zipIdx_cons, ih (n+1), ih n]
      simp

private theorem histFrom_weighted_sum {α : Type} [BEq α] [Hashable α] [EquivBEq α] [LawfulHashable α] [LawfulBEq α]
    (h : Std.HashMap α Nat) (xs : List α) (g : α → Nat → Nat) (hfun : α → Nat → Nat)
    (hg0 : ∀ k, g k 0 = 0) (hgsucc : ∀ k n, g k (n+1) = g k n + hfun k n)
    (hpos : ∀ e ∈ h.toList, 0 < e.2)
    (hentry : ∀ e ∈ h.toList, h.getD e.1 0 = e.2) :
    ((histFrom h xs).toList.map fun e => g e.1 e.2).sum =
      (h.toList.map fun e => g e.1 e.2).sum +
        (xs.zipIdx.map fun (x,i) => hfun x (h.getD x 0 + ((xs.take i).filter fun y => x == y).length)).sum := by
  induction xs generalizing h with
  | nil => simp [histFrom, List.zipIdx]
  | cons x xs ih =>
      simp only [histFrom, List.foldl_cons]
      change ((histFrom (h.insert x (h.getD x 0 + 1)) xs).toList.map fun e => g e.1 e.2).sum = _
      rw [ih]
      · rw [toList_sum_insert h x (fun e : α × Nat => g e.1 e.2) (fun n => hfun x n) (fun n => rfl) (hg0 x) (hgsucc x) hpos hentry]
        simp only [List.zipIdx_cons, List.map_cons, List.sum_cons, List.take_zero, List.filter_nil, List.length_nil, Nat.add_zero]
        have hzipAdd : (xs.zipIdx 1) = (xs.zipIdx).map fun p => (p.1, p.2 + 1) := zipIdx_add xs 0
        rw [hzipAdd]
        simp only [List.map_map]
        have hmap :
            (List.map (fun p : α × Nat =>
              hfun p.1 ((h.insert x (h.getD x 0 + 1)).getD p.1 0 +
                ((xs.take p.2).filter fun y => p.1 == y).length)) (xs.zipIdx)) =
            (List.map ((fun p : α × Nat => hfun p.1 (h.getD p.1 0 +
              (((x :: xs).take p.2).filter fun y => p.1 == y).length)) ∘ fun p => (p.1, p.2 + 1)) (xs.zipIdx)) := by
          apply List.map_congr_left
          intro p hp
          simp only [Function.comp_apply]
          rw [insert_getD_add]
          rw [Nat.add_assoc]
          rw [← count_take_succ_cons]
        rw [hmap]
        rw [Nat.add_assoc]
      · exact toList_insert_pos h x (h.getD x 0 + 1) (by omega) hpos
      · exact toList_insert_entry h x (h.getD x 0 + 1) hentry


private theorem choose2_succ (n : Nat) : choose2 (n+1) = choose2 n + n := by
  cases n with
  | zero => simp [choose2]
  | succ m =>
      unfold choose2
      simp
      rw [show (m + 1 + 1) * (m + 1) = (m + 1) * m + 2 * (m + 1) by grind]
      rw [Nat.add_mul_div_left _ _ (by omega : 0 < 2)]

private theorem sum_map_add {β : Type} (l : List β) (f g : β → Nat) :
    (l.map fun t => f t + g t).sum = (l.map f).sum + (l.map g).sum := by
  induction l with
  | nil => rfl
  | cons a l ih => simp [ih, Nat.add_assoc, Nat.add_left_comm]

private def equalPairs {α : Type} [BEq α] (xs : List α) : Nat :=
  (xs.zipIdx.map fun (x,i) => ((xs.drop (i+1)).filter fun y => x == y).length).sum

private theorem equalPairs_cons {α : Type} [BEq α] [LawfulBEq α] (x : α) (xs : List α) :
    equalPairs (x :: xs) = equalPairs xs + (xs.filter fun y => x == y).length := by
  induction xs with
  | nil => simp [equalPairs]
  | cons y ys ih =>
      unfold equalPairs
      rw [List.zipIdx_cons (i := 0)]
      simp only [List.map_cons, List.sum_cons]
      rw [List.zipIdx_cons (i := 1)]
      simp only [List.map_cons, List.sum_cons]
      rw [show 1 + 1 = 2 by omega, zipIdx_add ys 1]
      simp only [List.map_map]
      rw [List.zipIdx_cons (i := 0)]
      simp only [List.map_cons, List.sum_cons]
      have hdrop3 : ∀ j, (y :: ys).drop (j + 1) = ys.drop j := by
        intro j
        rw [show j + 1 = Nat.succ j by omega, List.drop_succ_cons]
      have hdrop : ∀ i, (x :: y :: ys).drop (i + 1) = (y :: ys).drop i := by
        intro i
        rw [show i + 1 = Nat.succ i by omega, List.drop_succ_cons]
      have hdrop2 : ∀ i, (x :: y :: ys).drop ((i + 1) + 1) = (y :: ys).drop (i + 1) := by
        intro i
        rw [show (i+1)+1 = Nat.succ (i+1) by omega, List.drop_succ_cons]
      have htail : (List.map ((fun p : α × Nat => (List.filter (fun y => p.1 == y) (List.drop (p.2 + 1) (x :: y :: ys))).length) ∘ fun p => (p.1, p.2 + 1)) (ys.zipIdx 1)) =
          (List.map (fun p : α × Nat => ((List.filter (fun y => p.1 == y) (List.drop p.2 ys)).length)) (ys.zipIdx 1)) := by
        apply List.map_congr_left
        intro p hp
        simp [Function.comp_apply, hdrop2, hdrop3]
      rw [htail]
      simp [hdrop, hdrop3]
      omega

private def prefixEqualSum {α : Type} [BEq α] (xs : List α) : Nat :=
  (xs.zipIdx.map fun (x,i) => ((xs.take i).filter fun y => x == y).length).sum

private theorem prefixEqualSum_cons {α : Type} [BEq α] [LawfulBEq α] (x : α) (xs : List α) :
    prefixEqualSum (x :: xs) = prefixEqualSum xs + (xs.filter fun y => x == y).length := by
  unfold prefixEqualSum
  rw [List.zipIdx_cons]
  simp only [List.map_cons, List.sum_cons, List.take_zero, List.filter_nil, List.length_nil]
  rw [show (xs.zipIdx 1) = (xs.zipIdx).map fun p => (p.1, p.2 + 1) by exact zipIdx_add xs 0]
  simp only [List.map_map]
  have hmap : (List.map ((fun p : α × Nat => (((x :: xs).take p.2).filter fun y => p.1 == y).length) ∘ (fun p : α × Nat => (p.1, p.2 + 1))) (xs.zipIdx)) =
      (List.map (fun p : α × Nat => (if p.1 == x then 1 else 0) + ((xs.take p.2).filter fun y => p.1 == y).length) (xs.zipIdx)) := by
    apply List.map_congr_left
    intro p hp
    simp only [Function.comp_apply]
    rw [count_take_succ_cons x p.1 xs p.2]
    have hs : (x == p.1) = (p.1 == x) := by
      by_cases h1 : x == p.1
      · have h2 : p.1 == x := beq_iff_eq.mpr (beq_iff_eq.mp h1).symm
        simp [h1, h2]
      · have h2 : (p.1 == x) = false := by
          by_cases hb : p.1 == x
          · have h1' : x == p.1 := beq_iff_eq.mpr (beq_iff_eq.mp hb).symm
            exact (h1 h1').elim
          · cases hb2 : (p.1 == x)
            · rfl
            · exact (hb hb2).elim
        simp [h1, h2]
    rw [hs]
  simp only [Nat.zero_add]
  rw [hmap]
  have hsumAdd : (xs.zipIdx.map fun p : α × Nat => (if p.1 == x then 1 else 0) + ((xs.take p.2).filter fun y => p.1 == y).length).sum =
      (xs.zipIdx.map fun p : α × Nat => if p.1 == x then 1 else 0).sum +
      (xs.zipIdx.map fun p : α × Nat => ((xs.take p.2).filter fun y => p.1 == y).length).sum := by
    exact sum_map_add (xs.zipIdx) (fun p => if p.1 == x then 1 else 0) (fun p => ((xs.take p.2).filter fun y => p.1 == y).length)
  rw [hsumAdd]
  clear hmap hsumAdd
  have hind : (xs.zipIdx.map fun p : α × Nat => if p.1 == x then 1 else 0).sum = (xs.filter fun y => x == y).length := by
    induction xs with
    | nil => rfl
    | cons a as ih2 =>
        rw [List.zipIdx_cons]
        simp only [List.map_cons, List.sum_cons]
        rw [show (as.zipIdx 1).map (fun p : α × Nat => if p.1 == x then 1 else 0) =
            (as.zipIdx).map (fun p : α × Nat => if p.1 == x then 1 else 0) by
          rw [zipIdx_add as 0]
          simp only [List.map_map]
          apply List.map_congr_left
          intro p hp
          simp [Function.comp_apply]]
        rw [ih2]
        by_cases hx : x == a
        · have hax : a == x := beq_iff_eq.mpr (beq_iff_eq.mp hx).symm
          simp [hx, hax]
          omega
        · have hax : (a == x) = false := by
            by_cases hb : a == x
            · have h' : x == a := beq_iff_eq.mpr (beq_iff_eq.mp hb).symm
              exact (hx h').elim
            · cases hb2 : (a == x)
              · rfl
              · exact (hb hb2).elim
          simp [hx, hax]
  rw [hind]
  omega

private theorem prefixEqualSum_eq_equalPairs {α : Type} [BEq α] [LawfulBEq α] (xs : List α) :
    prefixEqualSum xs = equalPairs xs := by
  induction xs with
  | nil => rfl
  | cons x xs ih => rw [prefixEqualSum_cons, equalPairs_cons, ih]

private theorem hist_sum_choose2 {α : Type} [BEq α] [Hashable α] [EquivBEq α] [LawfulHashable α] [LawfulBEq α]
    (xs : List α) :
    ((hist xs).toList.map fun e => choose2 e.2).sum = equalPairs xs := by
  have hmain := histFrom_weighted_sum ({} : Std.HashMap α Nat) xs
    (fun _ n => choose2 n) (fun _ n => n)
    (by intro k; simp [choose2])
    (by intro k n; exact choose2_succ n)
    (by intro e he; simp at he)
    (by intro e he; simp at he)
  rw [show hist xs = histFrom ({} : Std.HashMap α Nat) xs by rfl]
  rw [hmain]
  rw [Std.HashMap.toList_empty]
  simp only [List.map_nil, List.sum_nil, Std.HashMap.getD_empty, Nat.zero_add]
  change prefixEqualSum xs = equalPairs xs
  exact prefixEqualSum_eq_equalPairs xs

private theorem map_filter_fst_int (k : Int) (r : List Row) :
    ((List.filter fun y => k == y) (r.map Prod.fst)).length =
      (r.filter fun y => y.1 == k).length := by
  induction r with
  | nil => rfl
  | cons y r ih =>
      by_cases h : y.1 == k
      · have h' : k == y.1 := beq_iff_eq.mpr (beq_iff_eq.mp h).symm
        simp [h, h', ih]
      · have hf : (y.1 == k) = false := by simpa using h
        have hf' : (k == y.1) = false := by
          by_cases hb : k == y.1
          · have hb' : y.1 == k := beq_iff_eq.mpr (beq_iff_eq.mp hb).symm
            exact (h hb').elim
          · cases hb2 : (k == y.1)
            · rfl
            · exact (hb hb2).elim
        simp [hf, hf', ih]


private def countPairs {α : Type} (xs : List α) (p : α → α → Bool) : Nat :=
  (xs.zipIdx.map fun (x,i) => ((xs.drop (i+1)).filter (p x)).length).sum

private theorem countPairs_cons {α : Type} (x : α) (xs : List α) (p : α → α → Bool) :
    countPairs (x :: xs) p = countPairs xs p + (xs.filter (p x)).length := by
  induction xs with
  | nil => simp [countPairs]
  | cons y ys ih =>
      unfold countPairs
      rw [List.zipIdx_cons (i := 0)]
      simp only [List.map_cons, List.sum_cons]
      rw [List.zipIdx_cons (i := 1)]
      simp only [List.map_cons, List.sum_cons]
      rw [show 1 + 1 = 2 by omega, zipIdx_add ys 1]
      simp only [List.map_map]
      rw [List.zipIdx_cons (i := 0)]
      simp only [List.map_cons, List.sum_cons]
      have hdrop : ∀ i, (x :: y :: ys).drop (i + 1) = (y :: ys).drop i := by
        intro i
        rw [show i + 1 = Nat.succ i by omega, List.drop_succ_cons]
      have hdrop2 : ∀ i, (x :: y :: ys).drop ((i + 1) + 1) = (y :: ys).drop (i + 1) := by
        intro i
        rw [show (i+1)+1 = Nat.succ (i+1) by omega, List.drop_succ_cons]
      have hdrop3 : ∀ j, (y :: ys).drop (j + 1) = ys.drop j := by
        intro j
        rw [show j + 1 = Nat.succ j by omega, List.drop_succ_cons]
      have htail : (List.map ((fun p' : α × Nat => (((x :: y :: ys).drop (p'.2 + 1)).filter (p p'.1)).length) ∘ fun p' => (p'.1, p'.2 + 1)) (ys.zipIdx 1)) =
          (List.map (fun p' : α × Nat => (((ys.drop p'.2).filter (p p'.1)).length)) (ys.zipIdx 1)) := by
        apply List.map_congr_left
        intro p' hp'
        simp [Function.comp_apply, hdrop2, hdrop3]
      rw [htail]
      simp [hdrop, hdrop3]
      omega

private theorem sameGroup_sum (xs : List Row) :
    equalPairs (xs.map Prod.fst) = equalPairs xs +
      countPairs xs (fun y z => y.1 == z.1 && y.2 != z.2) := by
  induction xs with
  | nil => simp [equalPairs, countPairs]
  | cons x xs ih =>
      rw [show (x :: xs).map Prod.fst = x.1 :: xs.map Prod.fst by rfl]
      rw [equalPairs_cons, equalPairs_cons, countPairs_cons, ih]
      rw [map_filter_fst_int]
      have ht : (xs.filter fun y => y.1 == x.1).length =
          (xs.filter fun y => x.1 == y.1).length := by
        apply congrArg List.length
        apply List.filter_congr
        intro y hy
        by_cases h : y.1 == x.1
        · have h' : x.1 == y.1 := beq_iff_eq.mpr (beq_iff_eq.mp h).symm
          simp [h, h']
        · have h' : (x.1 == y.1) = false := by
            by_cases hb : x.1 == y.1
            · have hb' : y.1 == x.1 := beq_iff_eq.mpr (beq_iff_eq.mp hb).symm
              exact (h hb').elim
            · cases hb2 : (x.1 == y.1)
              · rfl
              · exact (hb hb2).elim
          simp [h, h']
      rw [ht]
      have hrow : (xs.filter fun y => x == y).length = (xs.filter fun y => y == x).length := by
        apply congrArg List.length
        apply List.filter_congr
        intro y hy
        by_cases h : x == y
        · have h' : y == x := beq_iff_eq.mpr (beq_iff_eq.mp h).symm
          simp [h, h']
        · have h' : (y == x) = false := by
            by_cases hb : y == x
            · have hb' : x == y := beq_iff_eq.mpr (beq_iff_eq.mp hb).symm
              exact (h hb').elim
            · cases hb2 : (y == x)
              · rfl
              · exact (hb hb2).elim
          simp [h, h']
      rw [hrow]
      have hsub := tenant_sub_row x xs
      have hle := row_filter_subset x xs
      omega

theorem SameGroupPairs_sound : TinylambOpt.ResearchGoals.PairAggregates.SameGroupPairs.Goal := by
  intro xs
  rw [hist_sum_choose2 (xs.map Prod.fst), hist_sum_choose2 xs]
  change equalPairs (xs.map Prod.fst) - equalPairs xs =
    countPairs xs (fun y z => y.1 == z.1 && y.2 != z.2)
  have h := sameGroup_sum xs
  omega


private theorem sum_map_zero_gen {β : Type} (l : List β) : (l.map fun _ => 0).sum = 0 := by
  induction l with
  | nil => rfl
  | cons a l ih => simpa using ih

private theorem zipIdx_map_sum {α : Type} (l : List α) (f : α → Nat) :
    (l.zipIdx.map fun p => f p.1).sum = (l.map f).sum := by
  induction l with
  | nil => simp [List.zipIdx]
  | cons x xs ih =>
      rw [List.zipIdx_cons]
      simp only [List.map_cons, List.sum_cons]
      rw [show (xs.zipIdx 1).map (fun p => f p.1) = (xs.zipIdx).map (fun p => f p.1) by
        rw [zipIdx_add xs 0]
        simp only [List.map_map]
        apply List.map_congr_left
        intro p hp
        simp [Function.comp_apply]]
      rw [ih]

private theorem count_between (x y : Int) (c : List Int) :
    (c.filter fun z => x < y && y < z).length =
      if x < y then (c.filter fun z => y < z).length else 0 := by
  by_cases h : x < y
  · have hf : (c.filter fun z => x < y && y < z) = (c.filter fun z => y < z) := by
      apply List.filter_congr
      intro z hz
      simp [h]
    rw [hf]
    simp [h]
  · have hf : (c.filter fun z => x < y && y < z) = [] := by
      apply List.filter_eq_nil_iff.mpr
      intro z hz
      simp [h]
    rw [hf]
    simp [h]

private theorem sum_between_a (a b c : List Int) :
    (a.map fun x => (b.map fun y => (c.filter fun z => x < y && y < z).length).sum).sum =
      (b.map fun y => (a.filter fun x => x < y).length * (c.filter fun z => y < z).length).sum := by
  induction a with
  | nil =>
      simp [sum_map_zero_gen]
  | cons x a ih =>
      change (b.map fun y => (c.filter fun z => x < y && y < z).length).sum +
        (a.map fun x => (b.map fun y => (c.filter fun z => x < y && y < z).length).sum).sum =
        (b.map fun y => ((x :: a).filter fun x' => x' < y).length * (c.filter fun z => y < z).length).sum
      rw [ih]
      have hpoint : ∀ y : Int,
          (c.filter fun z => x < y && y < z).length + (a.filter fun x' => x' < y).length * (c.filter fun z => y < z).length =
          ((x :: a).filter fun x' => x' < y).length * (c.filter fun z => y < z).length := by
        intro y
        rw [count_between]
        simp [List.filter_cons]
        by_cases h : x < y <;> simp [h] <;> grind
      rw [show (b.map fun y => ((x :: a).filter fun x' => x' < y).length * (c.filter fun z => y < z).length) =
          (b.map fun y => (c.filter fun z => x < y && y < z).length +
            (a.filter fun x' => x' < y).length * (c.filter fun z => y < z).length) by
        apply List.map_congr_left
        intro y hy
        exact (hpoint y).symm]
      rw [sum_map_add b
        (fun y => (c.filter fun z => x < y && y < z).length)
        (fun y => (a.filter fun x' => x' < y).length * (c.filter fun z => y < z).length)]

theorem IncreasingTriples_sound : TinylambOpt.ResearchGoals.PairAggregates.IncreasingTriples.Goal := by
  intro a b c
  unfold anchoredTriples
  rw [sum_between_a]
  have hmain := histFrom_weighted_sum ({} : Std.HashMap Int Nat) b
    (fun k n => n * (a.filter fun x => x < k).length * (c.filter fun z => k < z).length)
    (fun k _ => (a.filter fun x => x < k).length * (c.filter fun z => k < z).length)
    (by intro k; simp)
    (by intro k n; grind)
    (by intro e he; simp at he)
    (by intro e he; simp at he)
  change ((histFrom ({} : Std.HashMap Int Nat) b).toList.map fun e =>
      e.2 * (a.filter fun x => x < e.1).length * (c.filter fun z => e.1 < z).length).sum =
    (b.map fun y => (a.filter fun x => x < y).length * (c.filter fun z => y < z).length).sum
  rw [hmain]
  rw [Std.HashMap.toList_empty]
  simp [sum_map_zero_gen]
  exact zipIdx_map_sum b (fun y => (a.filter fun x => x < y).length * (c.filter fun z => y < z).length)

end TinylambOpt.ResearchProofs.Wave7PairAggregates
