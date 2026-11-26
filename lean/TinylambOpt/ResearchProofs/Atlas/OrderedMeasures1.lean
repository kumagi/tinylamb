import TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

set_option maxHeartbeats 200000
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.OrderedMeasures1

open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures
open TinylambOpt.ResearchGoals.PairAggregates (choose2)

/-! ### Generic list and pair helpers -/

private theorem flatMap_map {α β γ : Type} (f : β → List γ) (g : α → β) (xs : List α) :
    (xs.map g).flatMap f = xs.flatMap (fun x => f (g x)) := by
  induction xs with
  | nil => rfl
  | cons a as ih => simp [ih]

private theorem zipIdx_add {α : Type} (xs : List α) (n m : Nat) :
    xs.zipIdx (n + m) = (xs.zipIdx n).map (fun (v, i) => (v, i + m)) := by
  induction xs generalizing n with
  | nil => simp
  | cons a as ih =>
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

private theorem pairs_nil : pairs [] = [] := by
  unfold pairs
  rw [List.zipIdx_nil]
  rfl

private theorem pairs_singleton (a : Int) : pairs [a] = [] := by
  simpa [pairs_nil] using pairs_cons a []

private theorem cart_cons (a : Int) (xs y : Series) :
    cart (a :: xs) y = y.map (fun b => (a, b)) ++ cart xs y := by
  simp [cart]

private theorem countP_map_comp {α β : Type} (P : β → Bool) (g : α → β) (xs : List α) :
    (xs.map g).countP P = xs.countP (fun x => P (g x)) := by
  induction xs with
  | nil => rfl
  | cons x xs ih =>
      by_cases h : P (g x) = true <;> simp [List.countP_cons, h, ih]

private theorem pairs_sum_append (f : Int × Int → Int) (x y : Series) :
    ((pairs (x ++ y)).map f).sum =
      ((pairs x).map f).sum + ((pairs y).map f).sum + ((cart x y).map f).sum := by
  induction x with
  | nil => simp [pairs, cart]
  | cons a xs ih =>
      rw [List.cons_append, pairs_cons, pairs_cons, cart_cons]
      simp only [List.map_append, List.map_map, Function.comp_def, List.sum_append]
      change (List.map (fun b => f (a, b)) xs).sum +
          (List.map (fun b => f (a, b)) y).sum + _ = _
      rw [ih]
      grind

private theorem pairs_countP_append (P : Int × Int → Bool) (x y : Series) :
    (pairs (x ++ y)).countP P =
      (pairs x).countP P + (pairs y).countP P + (cart x y).countP P := by
  induction x with
  | nil => simp [pairs, cart]
  | cons a xs ih =>
      rw [List.cons_append, pairs_cons, pairs_cons, cart_cons]
      simp only [List.countP_append]
      rw [countP_map_comp, countP_map_comp, List.countP_map]
      simp only [Function.comp_def]
      rw [List.countP_append, ih]
      omega

private theorem pairs_map (f : Int → Int) (x : Series) :
    pairs (x.map f) = (pairs x).map (fun p => (f p.1, f p.2)) := by
  induction x with
  | nil => rfl
  | cons a xs ih =>
      simp only [List.map_cons, pairs_cons, ih, List.map_append, List.map_map,
        Function.comp_def]

private theorem choose2_twice (n : Nat) : 2 * choose2 n = n * (n - 1) := by
  unfold choose2
  rcases Nat.mod_two_eq_zero_or_one n with h0 | h1
  · have he : n = 2 * (n / 2) := by omega
    rw [he]
    have hd : 2 ∣ (2 * (n / 2)) * (2 * (n / 2) - 1) := by
      refine ⟨(n / 2) * (2 * (n / 2) - 1), ?_⟩
      simp [Nat.mul_assoc, Nat.mul_comm]
    have hc := Nat.mul_div_cancel' hd
    omega
  · have he : n = 2 * (n / 2) + 1 := by omega
    rw [he]
    have hsub : 2 * (n / 2) + 1 - 1 = 2 * (n / 2) := by omega
    rw [hsub]
    have hd : 2 ∣ (2 * (n / 2) + 1) * (2 * (n / 2)) := by
      refine ⟨(2 * (n / 2) + 1) * (n / 2), ?_⟩
      simp [Nat.mul_assoc, Nat.mul_comm]
    have hc := Nat.mul_div_cancel' hd
    omega

private theorem choose2_succ (n : Nat) : choose2 (n + 1) = choose2 n + n := by
  have h1 := choose2_twice n
  have h2 := choose2_twice (n + 1)
  have hsub : n + 1 - 1 = n := by omega
  simp only [hsub] at h2
  have hp : n * (n - 1) + 2 * n = (n + 1) * n := by
    cases n with
    | zero => rfl
    | succ m =>
        simp only [Nat.add_sub_cancel, Nat.succ_mul, Nat.mul_succ]
        omega
  omega

private theorem pairs_length (x : Series) : (pairs x).length = choose2 x.length := by
  induction x with
  | nil => rfl
  | cons a xs ih =>
      rw [pairs_cons, List.length_append, List.length_map, ih]
      simp only [List.length_cons]
      simpa [Nat.add_comm] using (choose2_succ xs.length).symm

private theorem perm_sum {l₁ l₂ : List Int} (p : l₁.Perm l₂) : l₁.sum = l₂.sum := by
  induction p with
  | nil => rfl
  | cons a p ih => simp [ih]
  | swap a b l => simp; grind
  | trans p q hp hq => exact hp.trans hq

private theorem perm_map_sum (f : Int → Int) {l₁ l₂ : List Int} (p : l₁.Perm l₂) :
    (l₁.map f).sum = (l₂.map f).sum :=
  perm_sum (p.map f)

private theorem perm_countP {α : Type} (P : α → Bool) {l₁ l₂ : List α}
    (p : l₁.Perm l₂) : l₁.countP P = l₂.countP P :=
  p.countP_eq P

private theorem sq_nonneg (a : Int) : 0 ≤ a ^ 2 := by
  have h : a ^ 2 = a * a := by grind
  rw [h, ← Int.natAbs_mul_self (a := a)]
  exact Int.ofNat_nonneg _

private theorem sum_map_nonneg {α : Type} (xs : List α) (f : α → Int)
    (h : ∀ x ∈ xs, 0 ≤ f x) : 0 ≤ (xs.map f).sum := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      simp only [List.map_cons, List.sum_cons]
      have hx := h x (by simp)
      have ht : ∀ y ∈ xs, 0 ≤ f y := by intro y hy; exact h y (by simp [hy])
      have hi := ih ht
      omega

private theorem sum_sq_cauchy (xs : List Int) :
    xs.sum ^ 2 ≤ (xs.length : Int) * (xs.map (fun z => z ^ 2)).sum := by
  induction xs with
  | nil => simp
  | cons z zs ih =>
      simp only [List.sum_cons, List.length_cons, List.map_cons]
      have hcast : (((zs.length + 1 : Nat) : Int)) = (zs.length : Int) + 1 := by simp
      rw [hcast]
      by_cases hk : zs.length = 0
      · simp [hk]
        grind
      · have hkposNat : 0 < zs.length := Nat.pos_of_ne_zero hk
        have hkpos : (0 : Int) < zs.length := by exact_mod_cast hkposNat
        let k : Int := zs.length
        let Q : Int := (zs.map fun z => z ^ 2).sum
        let S : Int := zs.sum
        have hih : S ^ 2 ≤ k * Q := by simpa [k, Q, S] using ih
        have hv : 0 ≤ k * Q - S ^ 2 := by omega
        have hsq : 0 ≤ (k * z - S) ^ 2 := sq_nonneg (k * z - S)
        have hident : (k * z - S) ^ 2 + (k * Q - S ^ 2) =
            k * (k * z ^ 2 - 2 * z * S + Q) := by grind
        have hprod : 0 ≤ k * (k * z ^ 2 - 2 * z * S + Q) := by
          rw [← hident]
          omega
        have hmain : 0 ≤ k * z ^ 2 - 2 * z * S + Q := by
          by_cases hnonneg : 0 ≤ k * z ^ 2 - 2 * z * S + Q
          · exact hnonneg
          · have hneg : k * z ^ 2 - 2 * z * S + Q < 0 := by omega
            have hmulneg : k * (k * z ^ 2 - 2 * z * S + Q) < 0 :=
              Int.mul_neg_of_pos_of_neg (by simpa [k] using hkpos) hneg
            omega
        have hlhs : (z + S) ^ 2 = z ^ 2 + 2 * z * S + S ^ 2 := by grind
        have hrhs : (k + 1) * (z ^ 2 + Q) = z ^ 2 + Q + k * z ^ 2 + k * Q := by grind
        change (z + S) ^ 2 ≤ (k + 1) * (z ^ 2 + Q)
        rw [hlhs, hrhs]
        omega

private theorem max0_cons (a : Int) (xs : List Int) :
    max0 (a :: xs) = max 0 (max a (max0 xs)) := by
  simp only [max0, List.max?_cons]
  cases hx : xs.max? with
  | none => simp
  | some b =>
      simp only [hx, Option.getD_some, Option.elim_some]
      change max 0 (max a b) = max 0 (max a (max 0 b))
      omega

private theorem max0_nonneg (xs : List Int) : 0 ≤ max0 xs := by
  simp only [max0]
  omega

private theorem le_max0_of_mem {xs : List Int} {a : Int} (ha : a ∈ xs) : a ≤ max0 xs := by
  induction xs with
  | nil => simp at ha
  | cons x xs ih =>
      rw [max0_cons]
      rcases List.mem_cons.mp ha with rfl | ha
      · omega
      · have hi := ih ha
        omega

private theorem max0_le {xs : List Int} {b : Int} (hb : 0 ≤ b)
    (h : ∀ a ∈ xs, a ≤ b) : max0 xs ≤ b := by
  induction xs with
  | nil => simpa [max0] using hb
  | cons x xs ih =>
      rw [max0_cons]
      exact Int.max_le.mpr ⟨hb, Int.max_le.mpr ⟨h x (by simp),
        ih (by intro a ha; exact h a (by simp [ha]))⟩⟩

private theorem max0_map_sublist {α : Type} (f : α → Int) {xs ys : List α}
    (h : xs.Sublist ys) : max0 (xs.map f) ≤ max0 (ys.map f) := by
  apply max0_le (max0_nonneg _)
  intro a ha
  exact le_max0_of_mem ((h.map f).mem ha)

/-! ### Adjacent edges and seam algebra -/

private def edgeBridge (x y : Series) : List (Int × Int) :=
  (x.getLast?.bind (fun a => y.head?.map (fun b => (a, b)))).toList

private theorem adj_cons₂ (a b : Int) (xs : Series) :
    adj (a :: b :: xs) = (a, b) :: adj (b :: xs) := by
  simp [adj]

private theorem adj_append (x y : Series) :
    adj x ++ edgeBridge x y ++ adj y = adj (x ++ y) := by
  induction x with
  | nil => simp [adj, edgeBridge]
  | cons a xs ih =>
      cases xs with
      | nil =>
          cases y with
          | nil => simp [adj, edgeBridge]
          | cons b ys =>
              simp only [List.cons_append, List.nil_append, adj_cons₂]
              simp [adj, edgeBridge, List.getLast?_cons, List.head?_cons,
                List.append_assoc]
      | cons b bs =>
          simp only [List.cons_append]
          have hbridge : edgeBridge (a :: b :: bs) y = edgeBridge (b :: bs) y := by
            simp [edgeBridge, List.getLast?_cons]
          have hi : adj (b :: bs) ++ edgeBridge (b :: bs) y ++ adj y =
              adj (b :: (bs ++ y)) := by
            simpa only [List.cons_append] using ih
          rw [adj_cons₂, hbridge, adj_cons₂]
          simpa only [List.append_assoc, List.cons_append] using
            congrArg (fun l => (a, b) :: l) hi

private theorem seam_eq_bridge_sum (x y : Series) (f : Int → Int → Int) :
    seam x y f = ((edgeBridge x y).map (fun p : Int × Int => f p.1 p.2)).sum := by
  cases x <;> cases y <;> simp [seam, edgeBridge, List.getLast?_cons,
    List.head?_cons]

private theorem seam_append_right (x y z : Series) (hy : y ≠ [])
    (f : Int → Int → Int) : seam x (y ++ z) f = seam x y f := by
  cases y with
  | nil => exact False.elim (hy rfl)
  | cons b bs => simp [seam]

private theorem adj_map (f : Int → Int) (x : Series) :
    adj (x.map f) = (adj x).map (fun p => (f p.1, f p.2)) := by
  induction x with
  | nil => rfl
  | cons a xs ih =>
      cases xs with
      | nil => simp [adj]
      | cons b bs =>
          simp only [List.map_cons, adj_cons₂]
          change (f a, f b) :: adj ((b :: bs).map f) =
            (f a, f b) :: ((adj (b :: bs)).map (fun p => (f p.1, f p.2)))
          rw [ih]

private theorem adj_length (x : Series) : (adj x).length = x.length - 1 := by
  induction x with
  | nil => rfl
  | cons a xs ih =>
      cases xs with
      | nil => simp [adj]
      | cons b bs =>
          simp only [adj_cons₂, List.length_cons] at ih ⊢
          omega

private theorem delta_length (x : Series) : (delta x).length = x.length - 1 := by
  simp only [delta, List.length_map, adj_length]

private theorem sum_delta (x : Series) :
    (delta x).sum = x.getLast?.getD 0 - x.head?.getD 0 := by
  induction x with
  | nil => rfl
  | cons a xs ih =>
      cases xs with
      | nil => simp [delta, adj]
      | cons b bs =>
          simp only [delta, adj_cons₂, List.map_cons, List.sum_cons,
            List.head?_cons, List.getLast?_cons, Option.getD_some] at ih ⊢
          omega

private theorem adj_le_of_pairwise {x : Series} (h : x.Pairwise (· ≤ ·)) :
    ∀ p ∈ adj x, p.1 ≤ p.2 := by
  induction x with
  | nil => simp [adj]
  | cons a xs ih =>
      cases xs with
      | nil => simp [adj]
      | cons b bs =>
          have hab : a ≤ b := (List.pairwise_cons.mp h).1 b (by simp)
          have htail : (b :: bs).Pairwise (· ≤ ·) := (List.pairwise_cons.mp h).2
          intro p hp
          simp only [adj_cons₂, List.mem_cons] at hp
          rcases hp with rfl | hp
          · exact hab
          · exact ih htail p hp

private theorem sum_map_congr {α : Type} (xs : List α) (f g : α → Int)
    (h : ∀ x ∈ xs, f x = g x) : (xs.map f).sum = (xs.map g).sum := by
  induction xs with
  | nil => rfl
  | cons x xs ih =>
      simp only [List.map_cons, List.sum_cons]
      rw [h x (by simp), ih (by intro y hy; exact h y (by simp [hy]))]

private theorem sum_map_mul_left {α : Type} (c : Int) (xs : List α) (f : α → Int) :
    (xs.map (fun x => c * f x)).sum = c * (xs.map f).sum := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      simp only [List.map_cons, List.sum_cons]
      rw [ih]
      grind

private theorem mem_of_head?_eq_some {x : Series} {a : Int}
    (h : x.head? = some a) : a ∈ x := by
  cases x with
  | nil => simp at h
  | cons b bs =>
      simp only [List.head?_cons, Option.some.injEq] at h
      subst a
      simp

private theorem mem_of_getLast?_eq_some {x : Series} {a : Int}
    (h : x.getLast? = some a) : a ∈ x := by
  induction x with
  | nil => simp at h
  | cons b bs ih =>
      rw [List.getLast?_cons] at h
      cases hb : bs.getLast? with
      | none =>
          simp only [hb, Option.getD_none, Option.some.injEq] at h
          subst a
          simp
      | some c =>
          simp only [hb, Option.getD_some, Option.some.injEq] at h
          subst a
          exact List.mem_cons_of_mem _ (ih hb)

private theorem pairwise_head_le {x : Series} (h : x.Pairwise (· ≤ ·)) :
    ∀ z ∈ x, x.head?.getD 0 ≤ z := by
  cases x with
  | nil => simp
  | cons a xs =>
      intro z hz
      simp only [List.head?_cons, Option.getD_some]
      rcases List.mem_cons.mp hz with rfl | hz
      · exact Int.le_refl _
      · exact (List.pairwise_cons.mp h).1 z hz

private theorem pairwise_le_last {x : Series} (h : x.Pairwise (· ≤ ·)) :
    ∀ z ∈ x, z ≤ x.getLast?.getD 0 := by
  induction x with
  | nil => simp
  | cons a xs ih =>
      cases xs with
      | nil =>
          intro z hz
          simp only [List.mem_singleton] at hz
          subst z
          simp
      | cons b bs =>
          have htail : (b :: bs).Pairwise (· ≤ ·) := (List.pairwise_cons.mp h).2
          have hab : a ≤ b := (List.pairwise_cons.mp h).1 b (by simp)
          have hblast : b ≤ (b :: bs).getLast?.getD 0 := by
            have hm : (b :: bs).getLast?.getD 0 ∈ b :: bs := by
              cases hh : (b :: bs).getLast? with
              | none =>
                  have he : b :: bs = [] := List.getLast?_eq_none_iff.mp hh
                  exact False.elim (by simp at he)
              | some c =>
                  simp only [hh, Option.getD_some]
                  exact mem_of_getLast?_eq_some hh
            rcases List.mem_cons.mp hm with h | hm
            · omega
            · exact (List.pairwise_cons.mp htail).1 _ hm
          intro z hz
          rcases List.mem_cons.mp hz with rfl | hz
          · simpa [List.getLast?_cons] using Int.le_trans hab hblast
          · simpa [List.getLast?_cons] using ih htail z hz

private theorem sorted_max_getLast {x : Series} (h : x.Pairwise (· ≤ ·)) :
    x.max?.getD 0 = x.getLast?.getD 0 := by
  cases x with
  | nil => rfl
  | cons a xs =>
      cases hm : (a :: xs).max? with
      | none =>
          have he : a :: xs = [] := List.max?_eq_none_iff.mp hm
          simp at he
      | some v =>
          have hvmem : v ∈ a :: xs := List.max?_mem hm
          have hvle : v ≤ (a :: xs).getLast?.getD 0 := pairwise_le_last h v hvmem
          have hlastmem : (a :: xs).getLast?.getD 0 ∈ a :: xs := by
            cases hh : (a :: xs).getLast? with
            | none =>
                have he : a :: xs = [] := List.getLast?_eq_none_iff.mp hh
                simp at he
            | some c =>
                simp only [hh, Option.getD_some]
                exact mem_of_getLast?_eq_some hh
          have hlev : (a :: xs).getLast?.getD 0 ≤ v :=
            ((List.max?_eq_some_iff.mp hm).2 _ hlastmem)
          simp only [hm, Option.getD_some]
          omega

private theorem sorted_min_head {x : Series} (h : x.Pairwise (· ≤ ·)) :
    x.min?.getD 0 = x.head?.getD 0 := by
  cases x with
  | nil => rfl
  | cons a xs =>
      cases hm : (a :: xs).min? with
      | none =>
          have he : a :: xs = [] := List.min?_eq_none_iff.mp hm
          simp at he
      | some v =>
          have hvmem : v ∈ a :: xs := List.min?_mem hm
          have hav : (a :: xs).head?.getD 0 ≤ v := pairwise_head_le h v hvmem
          have hheadmem : (a :: xs).head?.getD 0 ∈ a :: xs := by
            cases hh : (a :: xs).head? with
            | none =>
                have he : a :: xs = [] := List.head?_eq_none_iff.mp hh
                simp at he
            | some c =>
                simp only [hh, Option.getD_some]
                exact mem_of_head?_eq_some hh
          have hva : v ≤ (a :: xs).head?.getD 0 :=
            ((List.min?_eq_some_iff.mp hm).2 _ hheadmem)
          simp only [hm, Option.getD_some]
          omega

private def internalTVSeams : List Series → Int
  | [] => 0
  | [_] => 0
  | x :: y :: rest => seam x y distance + internalTVSeams (y :: rest)

/-! ### TotalVariation -/

namespace TotalVariation

private def tv (x : Series) : Int := TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.TotalVariation.measure x

private theorem tv_nonneg (x : Series) : 0 ≤ tv x := by
  unfold tv TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.TotalVariation.measure
  apply sum_map_nonneg
  intro v hv
  exact Int.natCast_nonneg _

private theorem measure_cons₂ (a b : Int) (xs : Series) :
    tv (a :: b :: xs) = distance a b + tv (b :: xs) := by
  simp [tv, TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.TotalVariation.measure,
    delta, adj_cons₂, distance]

private theorem repeated_head (n : Nat) (a : Int) (hn : 0 < n) :
    (repeated n a).head? = some a := by
  cases n with
  | zero => omega
  | succ k =>
      cases k with
      | zero => rfl
      | succ m =>
          change (List.replicate (m + 1 + 1) a).head? = some a
          rw [List.replicate_succ]
          rfl

private theorem repeated_last (n : Nat) (a : Int) (hn : 0 < n) :
    (repeated n a).getLast? = some a := by
  induction n with
  | zero => omega
  | succ k ih =>
      cases k with
      | zero => rfl
      | succ m =>
          change (List.replicate (m + 1 + 1) a).getLast? = some a
          rw [List.replicate_succ, List.getLast?_cons]
          change some ((List.replicate (m + 1) a).getLast?.getD a) = some a
          have hi : (List.replicate (m + 1) a).getLast? = some a := by
            simpa [repeated] using ih
          rw [hi]
          rfl

private theorem measure_repeated (n : Nat) (a : Int) : tv (repeated n a) = 0 := by
  induction n with
  | zero => rfl
  | succ n ih =>
      cases n with
      | zero => rfl
      | succ m =>
          simp only [repeated, List.replicate_succ]
          rw [measure_cons₂]
          have hi : tv (a :: List.replicate m a) = 0 := by
            simpa [repeated, List.replicate_succ] using ih
          rw [hi]
          simp [distance]

private theorem measure_append (x y : Series) :
    tv (x ++ y) = tv x + tv y + seam x y distance := by
  unfold tv TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.TotalVariation.measure delta
  rw [← adj_append]
  simp only [List.map_append, List.sum_append]
  rw [seam_eq_bridge_sum]
  simp only [List.map_map, Function.comp_def, distance]
  omega

private theorem measure_map_scaled (a : Int) (x : Series) :
    tv (scaled a x) = (a.natAbs : Int) * tv x := by
  unfold tv TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.TotalVariation.measure scaled delta
  rw [adj_map]
  simp only [List.map_map, Function.comp_def]
  have hmap : (adj x).map (fun p => ((a * p.2 - a * p.1).natAbs : Int)) =
      (adj x).map (fun p => (a.natAbs : Int) * ((p.2 - p.1).natAbs : Int)) := by
    apply List.map_congr_left
    intro p hp
    have hdiff : a * p.2 - a * p.1 = a * (p.2 - p.1) := by grind
    rw [hdiff, Int.natAbs_mul]
    rfl
  rw [hmap, sum_map_mul_left]

private theorem internalSeams_zero_repeated (x : Series) (n : Nat) (hn : 0 < n) :
    internalTVSeams (x.map (repeated n)) = tv x := by
  induction x with
  | nil => rfl
  | cons a xs ih =>
      cases xs with
      | nil => rfl
      | cons b bs =>
          have hseam : seam (repeated n a) (repeated n b) distance = distance a b := by
            simp [seam, repeated_head n a hn, repeated_last n a hn,
              repeated_head n b hn]
          simp only [List.map_cons, internalTVSeams]
          have hi : internalTVSeams (repeated n b :: List.map (repeated n) bs) =
              tv (b :: bs) := by
            simpa only [List.map_cons] using ih
          rw [hseam, hi]
          exact (measure_cons₂ a b bs).symm

private theorem sum_measure_repeated (x : Series) (n : Nat) :
    ((x.map (repeated n)).map tv).sum = 0 := by
  induction x with
  | nil => rfl
  | cons a xs ih =>
      simp only [List.map_cons, List.sum_cons, measure_repeated, ih]
      simp

private theorem flatten_measure (blocks : List Series)
    (hne : ∀ b ∈ blocks, b ≠ []) :
    tv blocks.flatten =
      (blocks.map tv).sum + internalTVSeams blocks := by
  induction blocks with
  | nil => rfl
  | cons b bs ih =>
      have hbs : ∀ z ∈ bs, z ≠ [] := by intro z hz; exact hne z (by simp [hz])
      cases bs with
      | nil => simp [internalTVSeams]
      | cons c cs =>
          rw [List.flatten_cons, measure_append, ih hbs]
          have hseam : seam b (c :: cs).flatten distance = seam b c distance := by
            exact seam_append_right b c cs.flatten (hne c (by simp)) distance
          rw [hseam]
          simp only [internalTVSeams, List.map_cons, List.sum_cons]
          grind

theorem sound_TotalVariation_Seam : TotalVariation.Seam.Goal := by
  intro x y
  exact measure_append x y

theorem sound_TotalVariation_Rescale : TotalVariation.Rescale.Goal := by
  intro a x
  exact measure_map_scaled a x

theorem sound_TotalVariation_Monotone : TotalVariation.Monotone.Goal := by
  intro x hx
  change tv x = spread x
  have hnon : ∀ p ∈ adj x, 0 ≤ p.2 - p.1 := by
    intro p hp
    have hle : p.1 ≤ p.2 := adj_le_of_pairwise hx p hp
    omega
  have habs : (delta x).map (fun v => (v.natAbs : Int)) =
      (delta x).map (fun v => v) := by
    apply List.map_congr_left
    intro v hv
    rcases List.mem_map.mp hv with ⟨p, hp, rfl⟩
    exact Int.natAbs_of_nonneg (hnon p hp)
  unfold tv TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.TotalVariation.measure
  rw [habs]
  simp only [List.map_id']
  change (delta x).sum = spread x
  rw [sum_delta]
  unfold spread
  rw [sorted_max_getLast hx, sorted_min_head hx]

theorem sound_TotalVariation_DropDuplicates : TotalVariation.DropDuplicates.Goal := by
  intro x n hn
  change tv (x.flatMap (repeated n)) = tv x
  rw [List.flatMap_def, flatten_measure]
  · rw [sum_measure_repeated, internalSeams_zero_repeated x n hn]
    simp
  · intro b hb
    rcases List.mem_map.mp hb with ⟨a, ha, rfl⟩
    cases n with
    | zero => omega
    | succ k => simp [repeated]

theorem sound_TotalVariation_EndpointFence : TotalVariation.EndpointFence.Goal := by
  intro x
  change distance (x.head?.getD 0) (x.getLast?.getD 0) ≤ tv x
  induction x with
  | nil => simp [distance, tv, TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.TotalVariation.measure,
    delta, adj]
  | cons a xs ih =>
      cases xs with
      | nil => simp [distance, tv, TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.TotalVariation.measure,
        delta, adj]
      | cons b bs =>
          rw [measure_cons₂]
          have htri : distance a (Option.getD (List.getLast? (b :: bs)) 0) ≤
              distance a b + distance b (Option.getD (List.getLast? (b :: bs)) 0) := by
            unfold distance
            have h := Int.natAbs_add_le (b - a) (Option.getD (List.getLast? (b :: bs)) 0 - b)
            have heq : Option.getD (List.getLast? (b :: bs)) 0 - a =
                (b - a) + (Option.getD (List.getLast? (b :: bs)) 0 - b) := by grind
            rw [heq]
            exact_mod_cast h
          have hi := ih
          have hlast : Option.getD (List.getLast? (a :: b :: bs)) 0 =
              Option.getD (List.getLast? (b :: bs)) 0 := by
            simp [List.getLast?_cons]
          simp only [List.head?_cons, Option.getD_some] at hi ⊢
          rw [hlast]
          omega

end TotalVariation

/-! ### QuadraticVariation -/

namespace QuadraticVariation

private def qv (x : Series) : Int :=
  TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.QuadraticVariation.measure x

private theorem qv_cons₂ (a b : Int) (xs : Series) :
    qv (a :: b :: xs) = (b - a) ^ 2 + qv (b :: xs) := by
  simp [qv, TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.QuadraticVariation.measure,
    delta, adj_cons₂]

private theorem qv_singleton (a : Int) : qv [a] = 0 := by
  simp [qv, TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.QuadraticVariation.measure,
    delta, adj]

private theorem qv_append (x y : Series) :
    qv (x ++ y) = qv x + qv y + seam x y (fun a b => (b - a) ^ 2) := by
  unfold qv TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.QuadraticVariation.measure delta
  rw [← adj_append]
  simp only [List.map_append, List.map_map, Function.comp_def, List.sum_append]
  rw [seam_eq_bridge_sum]
  omega

private theorem qv_map_affine (a b : Int) (x : Series) :
    qv (shifted b (scaled a x)) = a ^ 2 * qv x := by
  unfold qv TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.QuadraticVariation.measure shifted scaled delta
  rw [adj_map, adj_map]
  simp only [List.map_map, Function.comp_def]
  have hmap : (adj x).map (fun p =>
      (a * p.2 + b - (a * p.1 + b)) ^ 2) =
      (adj x).map (fun p => a ^ 2 * (p.2 - p.1) ^ 2) := by
    apply List.map_congr_left
    intro p hp
    have hdiff : a * p.2 + b - (a * p.1 + b) = a * (p.2 - p.1) := by grind
    rw [hdiff]
    grind
  rw [hmap, sum_map_mul_left]

private def ramp (n : Nat) (a d : Int) : Series :=
  (List.range n).map (fun i : Nat => a + d * (i : Nat))

private theorem ramp_succ (n : Nat) (a d : Int) :
    ramp (n + 1) a d = a :: ramp n (a + d) d := by
  simp only [ramp, List.range_succ_eq_map, List.map_cons, List.map_map,
    Function.comp_def]
  simp only [Int.natCast_zero, Int.mul_zero, Int.add_zero]
  congr 1
  apply List.map_congr_left
  intro i hi
  grind

private theorem ramp_succ₂ (n : Nat) (a d : Int) :
    ramp (n + 2) a d = a :: (a + d) :: ramp n (a + 2 * d) d := by
  rw [show n + 2 = (n + 1) + 1 by omega, ramp_succ, ramp_succ]
  congr 2
  grind

private theorem qv_ramp (n : Nat) (a d : Int) :
    qv (ramp n a d) = ((n - 1 : Nat) : Int) * d ^ 2 := by
  induction n generalizing a with
  | zero =>
      simp [ramp, qv, TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.QuadraticVariation.measure,
        delta, adj]
  | succ n ih =>
      cases n with
      | zero =>
          simp [ramp, qv, TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.QuadraticVariation.measure,
            delta, adj]
      | succ m =>
          rw [ramp_succ₂, qv_cons₂]
          have ht : (a + d) :: ramp m (a + 2 * d) d = ramp (m + 1) (a + d) d := by
            rw [ramp_succ]
            congr 1
            grind
          rw [ht, ih (a + d)]
          have hsub1 : m + 1 - 1 = m := by omega
          have hsub2 : m + 1 + 1 - 1 = m + 1 := by omega
          simp only [hsub1, hsub2]
          have hd : a + d - a = d := by grind
          rw [hd]
          grind

theorem sound_QuadraticVariation_SeamEnergy : QuadraticVariation.SeamEnergy.Goal := by
  intro x y
  exact qv_append x y

theorem sound_QuadraticVariation_Affine : QuadraticVariation.Affine.Goal := by
  intro a b x
  exact qv_map_affine a b x

theorem sound_QuadraticVariation_EndpointEnergy : QuadraticVariation.EndpointEnergy.Goal := by
  intro x
  cases x with
  | nil =>
      simp [TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.QuadraticVariation.measure,
        delta, adj]
  | cons a xs =>
      change (Option.getD (a :: xs).getLast? 0 - Option.getD (a :: xs).head? 0) ^ 2 ≤
        (((a :: xs).length : Int) - 1) * qv (a :: xs)
      rw [← sum_delta]
      unfold qv TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.QuadraticVariation.measure
      have hlen : ((delta (a :: xs)).length : Int) = (((a :: xs).length : Int) - 1) := by
        rw [delta_length, Int.ofNat_sub (m := 1) (by simp)]
        simp
      simpa only [hlen] using sum_sq_cauchy (delta (a :: xs))

theorem sound_QuadraticVariation_LinearRamp : QuadraticVariation.LinearRamp.Goal := by
  intro n a d
  exact qv_ramp n a d

theorem sound_QuadraticVariation_LocalEdit : QuadraticVariation.LocalEdit.Goal := by
  intro x y a b
  have hb : qv (x ++ b :: y) =
      qv x + qv y + seam x [b] (fun u v => (v - u) ^ 2) +
        seam [b] y (fun u v => (v - u) ^ 2) := by
    rw [show x ++ b :: y = x ++ ([b] ++ y) by simp, qv_append, qv_append,
      qv_singleton, seam_append_right x [b] y (by simp)]
    grind
  have ha : qv (x ++ a :: y) =
      qv x + qv y + seam x [a] (fun u v => (v - u) ^ 2) +
        seam [a] y (fun u v => (v - u) ^ 2) := by
    rw [show x ++ a :: y = x ++ ([a] ++ y) by simp, qv_append, qv_append,
      qv_singleton, seam_append_right x [a] y (by simp)]
    grind
  change qv (x ++ b :: y) - qv (x ++ a :: y) = _
  rw [hb, ha]
  grind

end QuadraticVariation

/-! ### Shared pair-count and multiplicity helpers -/

private def tieCount (x : Series) : Nat :=
  (pairs x).countP (fun p => p.1 == p.2)

private def riseCount (x : Series) : Nat :=
  (pairs x).countP rising

private def fallCount (x : Series) : Nat :=
  (pairs x).countP falling

private theorem beq_comm_int (a b : Int) : (a == b) = (b == a) := by
  apply Bool.eq_iff_iff.mpr
  simpa only [beq_iff_eq] using eq_comm

private theorem eraseDups_nodup : ∀ x : Series, x.eraseDups.Nodup
  | [] => by simp
  | a :: as => by
      rw [List.eraseDups_cons]
      apply List.nodup_cons.mpr
      constructor
      · intro hmem
        have hm := List.mem_eraseDups.mp hmem
        simp at hm
      · exact eraseDups_nodup (as.filter fun b => !b == a)
termination_by x => x.length
decreasing_by
  have hlen := List.length_filter_le (fun b : Int => !b == a) as
  change (List.filter (fun b : Int => !b == a) as).length < as.length + 1
  omega


private theorem sum_map_add_int {α : Type} (xs : List α) (f g : α → Int) :
    (xs.map (fun x => f x + g x)).sum = (xs.map f).sum + (xs.map g).sum := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      simp only [List.map_cons, List.sum_cons]
      rw [ih]
      grind

private theorem sum_ite_filter_int {α : Type} (xs : List α) (p : α → Bool) (f : α → Int) :
    (xs.map (fun x => if p x then f x else 0)).sum = ((xs.filter p).map f).sum := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      by_cases h : p x = true <;> simp [h, ih]

private theorem sum_eq_single_of_mem (xs : Series) (hnd : xs.Nodup) (a : Int) (f : Int → Int) :
    (xs.map (fun v => if v == a then f a else 0)).sum =
      if xs.contains a then f a else 0 := by
  induction xs with
  | nil => simp
  | cons v vs ih =>
      have hvnot : v ∉ vs := (List.nodup_cons.mp hnd).1
      have hndvs : vs.Nodup := (List.nodup_cons.mp hnd).2
      rw [List.map_cons, List.sum_cons, List.contains_cons, ih hndvs]
      by_cases hva : (v == a) = true
      · have hveq : v = a := of_decide_eq_true hva
        subst v
        have hnot : vs.contains a = false := by
          apply Bool.eq_false_of_ne_true
          intro hc
          exact hvnot (List.contains_iff_mem.mp hc)
        rw [hnot]
        simp
      · have hvf : (v == a) = false := Bool.eq_false_of_ne_true hva
        have hva' : (a == v) = false := by rw [beq_comm_int]; exact hvf
        simp [hvf, hva', ih hndvs]

private theorem sum_filter_ne_int (xs : Series) (hnd : xs.Nodup) (a : Int) (f : Int → Int) :
    (xs.map f).sum =
      (if xs.contains a then f a else 0) +
        ((xs.filter (fun b => b != a)).map f).sum := by
  have hpoint : ∀ b : Int, f b =
      (if b == a then f a else 0) + (if b != a then f b else 0) := by
    intro b
    by_cases hba : b = a
    · subst b
      simp
    · simp [hba]
  have hmap : (xs.map f).sum =
      (xs.map (fun b => (if b == a then f a else 0) +
        (if b != a then f b else 0))).sum := by
    apply congrArg List.sum
    apply List.map_congr_left
    intro b hb
    exact hpoint b
  rw [hmap, sum_map_add_int, sum_eq_single_of_mem xs hnd a f, sum_ite_filter_int]

private theorem count_filter_eq_self {α : Type} [DecidableEq α] (xs : List α)
    (p : α → Bool) (a : α) (ha : p a = true) :
    (xs.filter p).count a = xs.count a := by
  induction xs with
  | nil => simp
  | cons b xs ih =>
      by_cases hpb : p b = true
      · simp [List.filter_cons, hpb, List.count_cons]
        by_cases hba : b = a
        · subst b
          simp [ih]
        · simp [hba, ih]
      · have hpfalse : p b = false := by
          cases h : p b <;> simp_all
        simp [List.filter_cons, hpfalse, List.count_cons]
        by_cases hba : b = a
        · subst b
          have h1 : p a = true := ha
          have h2 : p a = false := hpfalse
          contradiction
        · simp [hba, ih]

private theorem sum_split_ne_eq (xs : Series) (a : Int) (f : Int → Int) :
    (xs.map f).sum =
      ((xs.filter (fun b => !b == a)).map f).sum +
      ((xs.filter (fun b => b == a)).map f).sum := by
  induction xs with
  | nil => simp
  | cons b xs ih =>
      by_cases hba : b = a
      · have hne : (b != a) = false := by simp [hba]
        have heq : (b == a) = true := by simp [hba]
        subst b
        simp [hne, heq, ih]
        omega
      · have hne : (b != a) = true := by simp [hba]
        have heq : (b == a) = false := by simp [hba]
        simp [hne, heq, ih]
        omega

private theorem sum_filter_eq_const (xs : Series) (a : Int) (f : Int → Int) :
    ((xs.filter (fun b => b == a)).map f).sum = (xs.count a : Int) * f a := by
  induction xs with
  | nil => simp
  | cons b xs ih =>
      by_cases hba : b = a
      · have heq : (b == a) = true := by simp [hba]
        subst b
        simp [heq, ih]
        rewrite [Int.add_mul]
        simp
        omega
      · have heq : (b == a) = false := by simp [hba]
        simp [heq, ih, List.count_cons, hba]

private theorem sum_eraseDups_weighted_aux (n : Nat) :
    ∀ (xs : List Int), xs.length ≤ n → ∀ f : Int → Int,
      (xs.eraseDups.map fun a => (xs.count a : Int) * f a).sum = (xs.map f).sum := by
  induction n with
  | zero =>
      intro xs hlen f
      cases xs with
      | nil => simp
      | cons a xs =>
          have : 0 < (a :: xs).length := by simp
          omega
  | succ n ih =>
      intro xs hlen f
      cases xs with
      | nil => simp
      | cons a xs =>
          have hxslen : xs.length ≤ n := by simp [List.length] at hlen; omega
          rw [List.eraseDups_cons, List.map_cons, List.sum_cons, List.map_cons, List.sum_cons]
          let ys := xs.filter fun b => !b == a
          let ds := ys.eraseDups
          have hnot : a ∉ ds := by
            intro hm
            have hm' := List.mem_eraseDups.mp hm
            have hcond := (List.mem_filter.mp hm').2
            simp at hcond
          have hcounts : ∀ b ∈ ds, (a :: xs).count b = ys.count b := by
            intro b hb
            have hbne : b ≠ a := by
              intro heq
              cases heq
              exact hnot hb
            have hy : b ∈ ys := List.mem_eraseDups.mp hb
            have hp : (!b == a) = true := by
              change (!(b == a)) = true
              simp [hbne]
            have hxs : xs.count b = ys.count b :=
              (count_filter_eq_self xs (fun z => !z == a) b hp).symm
            simp [List.count_cons, hbne.symm, hxs]
          have hds : (ds.map fun b => ( (a :: xs).count b : Int) * f b).sum =
              (ds.map fun b => (ys.count b : Int) * f b).sum := by
            apply congrArg List.sum
            apply List.map_congr_left
            intro b hb
            rw [hcounts b hb]
          have hyslen : ys.length ≤ n := by
            dsimp only [ys]
            exact Nat.le_trans (List.length_filter_le (fun b => !b == a) xs) hxslen
          have hcount : (a :: xs).count a = xs.count a + 1 := by simp [List.count_cons]
          have hfiltered := ih ys hyslen f
          rw [hcount, hds, hfiltered]
          have hsplit := sum_split_ne_eq xs a f
          have hsame := sum_filter_eq_const xs a f
          rw [hsplit, hsame]
          simp [Int.natCast_add, Int.natCast_one, ys]
          rewrite [Int.add_mul]
          simp [ys]
          omega

private theorem sum_eraseDups_weighted (x : Series) (f : Int → Int) :
    (x.map f).sum =
      (x.eraseDups.map (fun a => (x.count a : Int) * f a)).sum := by
  have h := sum_eraseDups_weighted_aux x.length x (Nat.le_refl _) f
  exact h.symm

private theorem intCast_sum_nat (xs : List Nat) :
    (xs.sum : Int) = (xs.map (fun n : Nat => (↑n : Int))).sum := by
  induction xs with
  | nil => rfl
  | cons n ns ih =>
      simp [List.sum_cons, List.map_cons, Int.natCast_add, ih]

private theorem sum_map_const_int {α : Type} (xs : List α) (c : Int) :
    (xs.map (fun _ => c)).sum = (xs.length : Int) * c := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      simp only [List.map_cons, List.sum_cons, List.length_cons, Int.natCast_add,
        Int.natCast_one]
      rw [ih]
      grind

private theorem sum_coe_eraseDups (x : Series) :
    (x.eraseDups.map (fun a => (↑(x.count a) : Int))).sum =
      (↑((x.eraseDups.map x.count).sum) : Int) := by
  induction x.eraseDups with
  | nil => simp
  | cons a as ih => simp [List.map_cons, List.sum_cons, Int.natCast_add, ih]

private theorem sum_eraseDups_count (x : Series) :
    (x.eraseDups.map x.count).sum = x.length := by
  have h := sum_eraseDups_weighted x (fun _ => 1)
  simp only [Int.mul_one, sum_map_const_int, Int.natCast_one] at h
  have hcoe := sum_coe_eraseDups x
  rw [hcoe] at h
  exact_mod_cast h.symm

private theorem countP_eq_count_comm (x : Series) (a : Int) :
    x.countP (fun b => a == b) = x.count a := by
  rw [List.count_eq_countP]
  congr 1
  funext b
  exact beq_comm_int a b

private theorem tieCount_cons (a : Int) (x : Series) :
    tieCount (a :: x) = x.count a + tieCount x := by
  unfold tieCount
  rw [pairs_cons, List.countP_append, countP_map_comp]
  change x.countP (fun b => a == b) + (pairs x).countP (fun p => p.1 == p.2) = _
  rw [countP_eq_count_comm]

private theorem riseCount_cons (a : Int) (x : Series) :
    riseCount (a :: x) = x.countP (fun b => a < b) + riseCount x := by
  unfold riseCount
  rw [pairs_cons, List.countP_append, countP_map_comp]
  rfl

private theorem fallCount_cons (a : Int) (x : Series) :
    fallCount (a :: x) = x.countP (fun b => b < a) + fallCount x := by
  unfold fallCount
  rw [pairs_cons, List.countP_append, countP_map_comp]
  rfl

private theorem eraseDups_filter : ∀ {α : Type} [DecidableEq α],
    ∀ (xs : List α) (p : α → Bool), (xs.filter p).eraseDups = xs.eraseDups.filter p
  | _, _, [], _ => rfl
  | _, _, a :: as, p => by
      by_cases hpa : p a = true
      · have ih1 := @eraseDups_filter _ _ as (fun x => p x && !(x == a))
        have ih2 := @eraseDups_filter _ _ as (fun x => !(x == a))
        simp [List.eraseDups_cons, List.filter_filter, hpa, ih1, ih2, Bool.and_comm]
      · have hpaf : p a = false := Bool.eq_false_of_ne_true hpa
        have ih1 := @eraseDups_filter _ _ as p
        have ih2 := @eraseDups_filter _ _ as (fun x => !(x == a))
        have hcongr : as.eraseDups.filter p = as.eraseDups.filter (fun x => p x && !(x == a)) := by
          apply List.filter_congr
          intro x hx
          by_cases hxa : x == a
          · have hxeq : x = a := of_decide_eq_true hxa
            subst x
            simp [hpaf]
          · simp [hxa]
        simp [List.eraseDups_cons, List.filter_filter, hpaf, ih1, ih2, hcongr]


private theorem sum_map_add_nat {α : Type} (xs : List α) (f g : α → Nat) :
    (xs.map (fun x => f x + g x)).sum = (xs.map f).sum + (xs.map g).sum := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      simp only [List.map_cons, List.sum_cons]
      rw [ih]
      omega

private theorem sum_ite_filter_nat {α : Type} (xs : List α) (p : α → Bool) (f : α → Nat) :
    (xs.map (fun x => if p x then f x else 0)).sum = ((xs.filter p).map f).sum := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      by_cases h : p x = true <;> simp [h, ih]

private theorem sum_eq_single_of_mem_nat (xs : Series) (hnd : xs.Nodup) (a : Int) (f : Int → Nat) :
    (xs.map (fun v => if v == a then f a else 0)).sum =
      if xs.contains a then f a else 0 := by
  induction xs with
  | nil => simp
  | cons v vs ih =>
      have hvnot : v ∉ vs := (List.nodup_cons.mp hnd).1
      have hndvs : vs.Nodup := (List.nodup_cons.mp hnd).2
      rw [List.map_cons, List.sum_cons, List.contains_cons, ih hndvs]
      by_cases hva : (v == a) = true
      · have hveq : v = a := of_decide_eq_true hva
        subst v
        have hnot : vs.contains a = false := by
          apply Bool.eq_false_of_ne_true
          intro hc
          exact hvnot (List.contains_iff_mem.mp hc)
        rw [hnot]
        simp
      · have hvf : (v == a) = false := Bool.eq_false_of_ne_true hva
        have hva' : (a == v) = false := by rw [beq_comm_int]; exact hvf
        simp [hvf, hva']

private theorem sum_filter_ne_nat (xs : Series) (hnd : xs.Nodup) (a : Int) (f : Int → Nat) :
    (xs.map f).sum =
      (if xs.contains a then f a else 0) +
        ((xs.filter (fun b => b != a)).map f).sum := by
  have hpoint : ∀ b : Int, f b =
      (if b == a then f a else 0) + (if b != a then f b else 0) := by
    intro b
    by_cases hba : b = a
    · subst b
      simp
    · simp [hba]
  have hmap : (xs.map f).sum =
      (xs.map (fun b => (if b == a then f a else 0) +
        (if b != a then f b else 0))).sum := by
    apply congrArg List.sum
    apply List.map_congr_left
    intro b hb
    exact hpoint b
  rw [hmap, sum_map_add_nat, sum_eq_single_of_mem_nat xs hnd a f, sum_ite_filter_nat]

private theorem tieCount_multiplicity (x : Series) :
    tieCount x = (x.eraseDups.map (fun a => choose2 (x.count a))).sum := by
  induction x with
  | nil => rfl
  | cons a xs ih =>
      rw [tieCount_cons, ih, List.eraseDups_cons, List.map_cons, List.sum_cons]
      have hmap : ((xs.filter (fun b => !b == a)).eraseDups.map
          (fun b => choose2 ((a :: xs).count b))).sum =
          ((xs.filter (fun b => !b == a)).eraseDups.map
            (fun b => choose2 (xs.count b))).sum := by
        apply congrArg List.sum
        apply List.map_congr_left
        intro b hb
        have hbmem : b ∈ xs.filter (fun b => !b == a) := List.mem_eraseDups.mp hb
        have hbne : (b == a) = false := by
          simpa using (List.mem_filter.mp hbmem).2
        have hne : a ≠ b := by
          intro h; subst h; simp at hbne
        simp [List.count_cons, hne]
      rw [hmap]
      have hcount : (a :: xs).count a = xs.count a + 1 := by simp [List.count_cons]
      rw [hcount]
      have hf := sum_filter_ne_nat xs.eraseDups (eraseDups_nodup xs) a
        (fun b => choose2 (xs.count b))
      have hfilter : xs.eraseDups.filter (fun b => b != a) =
          (xs.filter (fun b => !b == a)).eraseDups := by
        change xs.eraseDups.filter (fun b => !b == a) = _
        exact (@eraseDups_filter Int _ xs (fun b => !b == a)).symm
      rw [hfilter] at hf
      by_cases hca : xs.eraseDups.contains a = true
      · have hmem : a ∈ xs := by
          exact List.mem_eraseDups.mp (List.contains_iff_mem.mp hca)
        have hfa := hf
        simp [hca] at hfa
        have hchoose := choose2_succ (xs.count a)
        rw [hfa, hchoose]
        omega
      · have hmem : a ∉ xs := by
          intro hm
          exact hca (List.contains_iff_mem.mpr (List.mem_eraseDups.mpr hm))
        have hfa := hf
        simp [hca] at hfa
        have hcnt0 : xs.count a = 0 := (List.count_eq_zero).2 hmem
        have hchoose : choose2 (xs.count a + 1) = 0 := by
          rw [hcnt0]
          decide
        omega

private theorem tieCount_append (x y : Series) :
    tieCount (x ++ y) = tieCount x + tieCount y + (cart x y).countP (fun p => p.1 == p.2) := by
  exact pairs_countP_append (fun p => p.1 == p.2) x y

private theorem count_repeated_self (n : Nat) (a : Int) : (repeated n a).count a = n := by
  simp [repeated, List.count_replicate]

private theorem tieCount_repeated (n : Nat) (a : Int) : tieCount (repeated n a) = choose2 n := by
  induction n with
  | zero => rfl
  | succ n ih =>
      simp only [repeated, List.replicate_succ]
      rw [tieCount_cons]
      have hi : tieCount (List.replicate n a) = choose2 n := by
        simpa [repeated] using ih
      have hcount : (List.replicate n a).count a = n := by
        simpa [repeated] using count_repeated_self n a
      rw [hi, hcount]
      exact choose2_succ n

private theorem cart_tie_repeated (n : Nat) (a : Int) (x : Series) :
    (cart (repeated n a) x).countP (fun p => p.1 == p.2) = n * x.count a := by
  induction n with
  | zero => simp [cart, repeated]
  | succ n ih =>
      simp only [repeated, List.replicate_succ, cart_cons, List.countP_append,
        countP_map_comp]
      change x.countP (fun b => a == b) +
        (cart (List.replicate n a) x).countP (fun p => p.1 == p.2) = _
      rw [countP_eq_count_comm]
      have hi : (cart (List.replicate n a) x).countP (fun p => p.1 == p.2) =
          n * x.count a := by
        simpa [repeated] using ih
      rw [hi]
      simp [Nat.succ_mul, Nat.add_comm]

private theorem cart_tie_zero (x y : Series) (h : ∀ a ∈ x, a ∉ y) :
    (cart x y).countP (fun p => p.1 == p.2) = 0 := by
  induction x with
  | nil => rfl
  | cons a xs ih =>
      have ha : a ∉ y := h a (by simp)
      have htail : ∀ b ∈ xs, b ∉ y := by intro b hb; exact h b (by simp [hb])
      rw [cart_cons, List.countP_append, countP_map_comp, ih htail]
      change y.countP (fun b => a == b) + 0 = 0
      rw [countP_eq_count_comm, List.count_eq_zero.mpr ha]

private theorem countP_map_tie_partition (ps : List (Int × Int)) (f : Int → Int) :
    ps.countP (fun p => f p.1 == f p.2) =
      ps.countP (fun p => p.1 == p.2) +
        ps.countP (fun p => p.1 != p.2 && f p.1 == f p.2) := by
  induction ps with
  | nil => rfl
  | cons p ps ih =>
      rcases p with ⟨a, b⟩
      by_cases hab : a = b
      · subst b
        have hfab : (f a == f a) = true := by simp [beq_self_eq_true]
        simp [List.countP_cons, hfab, ih]
      · have hneq : (a != b) = true := by simp [hab]
        by_cases hfab : (f a == f b) = true <;>
          simp [List.countP_cons, hab, hneq, hfab, ih]

private theorem tieCount_map (f : Int → Int) (x : Series) :
    tieCount (x.map f) = tieCount x +
      (pairs x).countP (fun p => p.1 != p.2 && f p.1 == f p.2) := by
  unfold tieCount
  rw [pairs_map, countP_map_comp]
  exact countP_map_tie_partition (pairs x) f

private theorem sum_map_le_sum_nat {α : Type} (xs : List α) (f g : α → Nat)
    (h : ∀ x ∈ xs, f x ≤ g x) : (xs.map f).sum ≤ (xs.map g).sum := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      simp only [List.map_cons, List.sum_cons]
      have hx := h x (by simp)
      have ht : ∀ y ∈ xs, f y ≤ g y := by intro y hy; exact h y (by simp [hy])
      have hi := ih ht
      omega

private theorem sum_map_mul_const_nat {α : Type} (c : Nat) (xs : List α) (f : α → Nat) :
    (xs.map (fun x => c * f x)).sum = c * (xs.map f).sum := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      simp only [List.map_cons, List.sum_cons]
      rw [ih]
      grind

private theorem sum_map_mul_right_nat {α : Type} (xs : List α) (f : α → Nat) (c : Nat) :
    (xs.map (fun x => f x * c)).sum = (xs.map f).sum * c := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      simp only [List.map_cons, List.sum_cons]
      rw [ih]
      grind

/-! ### TiePairs -/

namespace TiePairs

theorem sound_TiePairs_Multiplicity : TiePairs.Multiplicity.Goal := by
  intro x
  change ((tieCount x : Nat) : Int) =
    ((x.eraseDups.map (fun a => choose2 (x.count a))).sum : Int)
  rw [tieCount_multiplicity]

theorem sound_TiePairs_DeleteBatch : TiePairs.DeleteBatch.Goal := by
  intro x a n
  change ((tieCount (repeated n a ++ x) : Nat) : Int) - (tieCount x : Int) =
    (choose2 n : Int) + (n : Int) * (x.count a : Int)
  rw [tieCount_append, tieCount_repeated, cart_tie_repeated]
  grind

theorem sound_TiePairs_DisjointKeys : TiePairs.DisjointKeys.Goal := by
  intro x y h
  change ((tieCount (x ++ y) : Nat) : Int) = (tieCount x : Int) + (tieCount y : Int)
  rw [tieCount_append, cart_tie_zero x y h]
  simp

theorem sound_TiePairs_CollisionPenalty : TiePairs.CollisionPenalty.Goal := by
  intro x f
  change ((tieCount (x.map f) : Nat) : Int) - (tieCount x : Int) =
    ((pairs x).countP (fun p => p.1 != p.2 && f p.1 == f p.2) : Int)
  rw [tieCount_map]
  grind

theorem sound_TiePairs_FanoutBound : TiePairs.FanoutBound.Goal := by
  intro x c hc
  change 2 * (tieCount x : Int) ≤ (x.length : Int) * ((c - 1 : Nat) : Int)
  have hpoint : ∀ a ∈ x.eraseDups,
      2 * choose2 (x.count a) ≤ x.count a * (c - 1) := by
    intro a ha
    have hax : a ∈ x := List.mem_eraseDups.mp ha
    have hk : x.count a ≤ c := hc a hax
    have hsub : x.count a - 1 ≤ c - 1 := Nat.sub_le_sub_right hk 1
    rw [choose2_twice]
    exact Nat.mul_le_mul_left _ hsub
  have hsum := sum_map_le_sum_nat x.eraseDups
    (fun a => 2 * choose2 (x.count a)) (fun a => x.count a * (c - 1)) hpoint
  rw [sum_map_mul_const_nat, sum_map_mul_right_nat, sum_eraseDups_count] at hsum
  rw [tieCount_multiplicity]
  exact_mod_cast hsum

end TiePairs

end TinylambOpt.ResearchProofs.Atlas.OrderedMeasures1
