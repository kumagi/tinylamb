import TinylambOpt.ResearchGoals.StorageExecution.BagKernels

/-!
A grouped or join aggregate can operate on distinct keys and multiplicities instead of
expanded non-NULL rows when the weighted result agrees.

Proved

- `RotationCycle.JoinFanout` — Evaluate band join count from the two weighted supports.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_RotationCycle_JoinFanout

open TinylambOpt.ResearchGoals.StorageExecution.BagKernels
open TinylambOpt.ResearchGoals.StorageExecution.BagKernels.RotationCycle

private theorem cycle_iff (q a b k : Nat) (hq : 0 < q) (ha : a < q) (hb : b < q) (hk : k < q) :
    (b + a) % q = k ↔ (k + q - b) % q = a := by
  constructor
  · intro h
    by_cases hba : b + a < q
    · have hmod : (b + a) % q = b + a := Nat.mod_eq_of_lt hba
      have hkv : k = b + a := by omega
      have hsum : k + q - b = a + q := by omega
      rw [hsum, Nat.add_mod_right, Nat.mod_eq_of_lt ha]
    · have hge : q ≤ b + a := by omega
      have hlt2 : b + a < 2 * q := by omega
      have hmod : (b + a) % q = b + a - q := by
        rw [Nat.mod_eq_sub_mod hge]
        exact Nat.mod_eq_of_lt (by omega)
      have hkv : k = b + a - q := by omega
      have hsum : k + q - b = a := by omega
      rw [hsum, Nat.mod_eq_of_lt ha]
  · intro h
    by_cases hbk : b ≤ k
    · have hsub : k + q - b = (k - b) + q := by omega
      have hmod : (k + q - b) % q = k - b := by
        rw [hsub, Nat.add_mod_right, Nat.mod_eq_of_lt (by omega)]
      have hkv : k - b = a := by omega
      have hlt : b + a < q := by omega
      rw [Nat.mod_eq_of_lt hlt]
      omega
    · have hbklt : k < b := by omega
      have hlt1 : k + q - b < q := by omega
      have hmod : (k + q - b) % q = k + q - b := Nat.mod_eq_of_lt hlt1
      have hkv : k + q - b = a := by omega
      have hge : q ≤ b + a := by omega
      have hlt2 : b + a < 2 * q := by omega
      rw [Nat.mod_eq_sub_mod hge, Nat.mod_eq_of_lt (by omega)]
      omega

private theorem mod_shift_eq_iff (n q s k : Nat) (hq : 0 < q) (hk : k < q) :
    (s + n) % q = k ↔ (k + q - s % q) % q = n % q := by
  have hb : s % q < q := Nat.mod_lt s hq
  have ha : n % q < q := Nat.mod_lt n hq
  rw [Nat.add_mod]
  exact cycle_iff q (n % q) (s % q) k hq ha hb hk

private theorem mass_step (n q s k : Nat) (hq : 0 < q) (hk : k < q) :
    (if (s + n) % q == k then 1 else 0) + n / q +
      (decide ((k + q - s % q) % q < n % q)).toNat =
    (n + 1) / q + (decide ((k + q - s % q) % q < (n + 1) % q)).toNat := by
  let a := n % q
  let bq := n / q
  let r := (k + q - s % q) % q
  have ha : a < q := Nat.mod_lt n hq
  have hr : r < q := Nat.mod_lt _ hq
  have hdecomp : n = q * bq + a := by
    dsimp [a, bq]
    exact (Nat.div_add_mod n q).symm
  have hiff := mod_shift_eq_iff n q s k hq hk
  have hind : (if (s + n) % q == k then 1 else 0) = (if r == a then 1 else 0) := by
    by_cases hle : (s + n) % q = k
    · have hra : r = a := hiff.mp hle
      simp [hle, hra]
    · have hra : r ≠ a := fun h => hle (hiff.mpr h)
      simp [hle, hra]
  rw [hind]
  change (if r == a then 1 else 0) + bq + (decide (r < a)).toNat =
    (n + 1) / q + (decide (r < (n + 1) % q)).toNat
  by_cases hlt : a + 1 < q
  · have hn1 : n + 1 = q * bq + (a + 1) := by omega
    have hdiv : (n + 1) / q = bq := by
      rw [hn1, Nat.mul_add_div hq]
      rw [Nat.div_eq_of_lt hlt]
      omega
    have hmod : (n + 1) % q = a + 1 := by
      rw [hn1, Nat.mul_add_mod, Nat.mod_eq_of_lt hlt]
    rw [hdiv, hmod]
    by_cases hreq : r = a
    · simp [hreq]
      omega
    · have hrne : (r == a) = false := by simp [hreq]
      rw [hrne]
      simp only [Bool.false_eq_true, ite_false, Nat.zero_add]
      by_cases hrlt : r < a
      · have hlt1 : r < a + 1 := by omega
        simp [hrlt, hlt1]
      · have hlt1 : ¬ r < a + 1 := by omega
        simp [hrlt, hlt1]
  · have heq : a + 1 = q := by omega
    have hn1' : n + 1 = q * bq + q := by omega
    have hn1 : n + 1 = q * (bq + 1) + 0 := by
      rw [hn1']
      simp [Nat.mul_add]
    have hdiv : (n + 1) / q = bq + 1 := by
      rw [hn1, Nat.mul_add_div hq]
      simp
    have hmod : (n + 1) % q = 0 := by
      rw [hn1, Nat.mul_add_mod]
      simp
    rw [hdiv, hmod]
    have hrle : r ≤ a := by omega
    by_cases hreq : r = a
    · simp [hreq]
      omega
    · have hrlt : r < a := by omega
      have hrne : (r == a) = false := by simp [hreq]
      rw [hrne]
      simp only [Bool.false_eq_true, ite_false, Nat.zero_add]
      simp [hrlt]

private theorem count_residue (n q s k : Nat) (hq : 0 < q) (hk : k < q) :
    (List.range n).countP (fun i => (s + i) % q == k) =
      n / q + (decide ((k + q - s % q) % q < n % q)).toNat := by
  induction n with
  | zero => simp
  | succ n ih =>
      rw [List.range_succ, List.countP_append, ih, List.countP_cons, List.countP_nil]
      simp only [Nat.zero_add, List.mem_singleton, Bool.and_true]
      rw [show (n + 1) / q + (decide ((k + q - s % q) % q < (n + 1) % q)).toNat =
        (if (s + n) % q == k then 1 else 0) + n / q +
          (decide ((k + q - s % q) % q < n % q)).toNat by
            exact (mass_step n q s k hq hk).symm]
      omega

private theorem count_map_eq_countP {α : Type} [BEq α] (xs : List α) (f : α → Nat) (k : Nat) :
    (xs.map f).count k = xs.countP (fun x => f x == k) := by
  induction xs with
  | nil => rfl
  | cons x xs ih =>
      simp only [List.map_cons]
      rw [List.count_cons, List.countP_cons, ih]

private theorem sum_eq_zero_of_all_zero (l : List Nat) (h : ∀ x ∈ l, x = 0) : l.sum = 0 := by
  induction l with
  | nil => rfl
  | cons x xs ih =>
      simp only [List.sum_cons]
      rw [h x (by simp), ih (fun y hy => h y (by simp [hy]))]

private theorem sum_range_indicator (q x : Nat) (hx : x < q) (f : Nat → Nat) :
    ((List.range q).map (fun k => if x == k then f k else 0)).sum = f x := by
  induction q with
  | zero => omega
  | succ q ih =>
      rw [List.range_succ, List.map_append, List.sum_append, List.map_singleton, List.sum_singleton]
      by_cases hxq : x == q
      · have hxq' : x = q := beq_iff_eq.mp hxq
        subst q
        have hprev : ((List.range x).map (fun k => if x == k then f k else 0)).sum = 0 := by
          apply sum_eq_zero_of_all_zero
          intro v hv
          rcases List.mem_map.mp hv with ⟨k, hk, hkv⟩
          have hklt : k < x := List.mem_range.mp hk
          have hne : (x == k) = false := by
            apply beq_eq_false_iff_ne.mpr
            omega
          rw [← hkv]
          simp [hne]
        rw [hprev, hxq]
        simp
      · have hxneq : x ≠ q := by
          intro h
          exact hxq (by simp [h])
        have hxltq : x < q := by omega
        rw [ih hxltq]
        simp [hxq]

private theorem sum_count_over_range (q : Nat) (xs : List Nat) (hxs : ∀ x ∈ xs, x < q)
    (f : Nat → Nat) :
    ((List.range q).map (fun k => xs.count k * f k)).sum = (xs.map f).sum := by
  induction xs with
  | nil =>
      apply sum_eq_zero_of_all_zero
      intro v hv
      rcases List.mem_map.mp hv with ⟨k, hk, rfl⟩
      simp
  | cons x xs ih =>
      have hxq : x < q := hxs x (by simp)
      have hxs' : ∀ y ∈ xs, y < q := fun y hy => hxs y (by simp [hy])
      have hpoint : ∀ k, (x :: xs).count k * f k = xs.count k * f k + (if x == k then f k else 0) := by
        intro k
        rw [List.count_cons]
        by_cases hxk : x == k
        · have hxk' : x = k := beq_iff_eq.mp hxk
          subst k
          simp [hxk, Nat.add_mul, Nat.add_comm]
        · simp [hxk]
      rw [show ((List.range q).map (fun k => (x :: xs).count k * f k)).sum =
          ((List.range q).map (fun k => xs.count k * f k + (if x == k then f k else 0))).sum by
            apply congrArg List.sum
            apply List.map_congr_left
            intro k hk
            exact hpoint k]
      rw [show ((List.range q).map (fun k => xs.count k * f k + (if x == k then f k else 0))).sum =
          ((List.range q).map (fun k => xs.count k * f k)).sum +
          ((List.range q).map (fun k => if x == k then f k else 0)).sum by
            induction (List.range q) with
            | nil => simp
            | cons a as iht =>
                simp only [List.map_cons, List.sum_cons]
                rw [iht]
                omega]
      rw [ih hxs', sum_range_indicator q x hxq f]
      simp [Nat.add_comm]

private theorem eraseDups_eq_self_of_nodup {α : Type} [BEq α] [LawfulBEq α]
    (l : List α) (h : l.Nodup) : l.eraseDups = l := by
  induction l with
  | nil => rfl
  | cons a as ih =>
      rw [List.nodup_cons] at h
      rw [List.eraseDups_cons]
      have hfilter : as.filter (fun b => !b == a) = as := by
        apply List.filter_eq_self.mpr
        intro b hb
        have hba : b ≠ a := by
          intro hba
          exact h.1 (hba ▸ hb)
        simp [hba]
      rw [hfilter, ih h.2]

private theorem mass_eq_count (n m s k : Nat) :
    bag.mass (n, m, s) k = (bag.expand (n, m, s)).count k := by
  change (if k ≤ m then n / (m + 1) +
      (decide ((k + (m + 1) - s % (m + 1)) % (m + 1) < n % (m + 1))).toNat else 0) =
    ((List.range n).map (fun i => (s + i) % (m + 1))).count k
  rw [count_map_eq_countP (List.range n) (fun i => (s + i) % (m + 1)) k]
  by_cases hk : k ≤ m
  · have hklt : k < m + 1 := by omega
    have hc := count_residue n (m + 1) s k (by omega) hklt
    rw [hc]
    simp [hk]
  · have hgt : m < k := by omega
    simp [hk]
    symm
    apply (List.countP_eq_zero).mpr
    intro i hi
    have himod : (s + i) % (m + 1) ≤ m := by
      have hlt := Nat.mod_lt (s + i) (by omega : 0 < m + 1)
      omega
    have hne : ((s + i) % (m + 1) == k) = false := by
      apply beq_eq_false_iff_ne.mpr
      omega
    simp [hne]

private theorem weighted_flat (p : Nat × Nat × Nat) (f : Nat → Nat) :
    weighted bag p f = ((bag.expand p).map f).sum := by
  rcases p with ⟨n, m, s⟩
  unfold weighted
  rw [show bag.keys (n, m, s) = List.range (m + 1) by rfl]
  rw [eraseDups_eq_self_of_nodup (List.range (m + 1)) List.nodup_range]
  calc
    ((List.range (m + 1)).map (fun k => bag.mass (n, m, s) k * f k)).sum =
        ((List.range (m + 1)).map (fun k => (bag.expand (n, m, s)).count k * f k)).sum := by
          apply congrArg List.sum
          apply List.map_congr_left
          intro k hk
          rw [mass_eq_count]
    _ = ((bag.expand (n, m, s)).map f).sum := by
      apply sum_count_over_range (m + 1) (bag.expand (n, m, s))
      · intro x hx
        change x ∈ (List.range n).map (fun i => (s + i) % (m + 1)) at hx
        rcases List.mem_map.mp hx with ⟨i, hi, rfl⟩
        exact Nat.mod_lt _ (by omega)

private theorem bool_sum_filter (ys : List Nat) (P : Nat → Bool) :
    ((ys.filter P).map (fun y => (P y).toNat)).sum = (ys.filter P).length := by
  induction ys with
  | nil => simp
  | cons y ys ih =>
      by_cases h : P y
      · simp [h, ih, Nat.add_comm]
      · simp [h, ih]

private theorem sum_bool_toNat_filter (ys : List Nat) (P : Nat → Bool) :
    (ys.map (fun y => (P y).toNat)).sum = ((ys.filter P).map (fun y => (P y).toNat)).sum := by
  induction ys with
  | nil => simp
  | cons y ys ih =>
      by_cases h : P y
      · simp [h, ih]
      · simp [h, ih]

private theorem length_pairs (xs ys : List Nat) (P : Nat → Nat → Bool) :
    (pairs xs ys P).length =
      (xs.map (fun x => ((ys.filter (P x)).map (fun y => (P x y).toNat)).sum)).sum := by
  induction xs with
  | nil => simp [pairs]
  | cons x xs ih =>
      simp [pairs, ih, bool_sum_filter, List.length_map]

theorem sound_JoinFanout : JoinFanout.Goal := by
  intro a c s
  let P : Nat → Nat → Bool := fun x y => x ≤ y && y ≤ x + s
  have hleft :
      weighted bag a (fun x => weighted bag c (fun y => (decide (x ≤ y ∧ y ≤ x + s)).toNat)) =
        ((bag.expand a).map (fun x =>
          (((bag.expand c).filter (P x)).map (fun y => (P x y).toNat)).sum)).sum := by
    rw [weighted_flat]
    congr 1
    apply List.map_congr_left
    intro x hx
    rw [weighted_flat]
    have hinner :
        ((bag.expand c).map (fun y => (decide (x ≤ y ∧ y ≤ x + s)).toNat)).sum =
          ((bag.expand c).map (fun y => (P x y).toNat)).sum := by
      simp [P]
    rw [hinner]
    exact sum_bool_toNat_filter (bag.expand c) (P x)
  rw [hleft, length_pairs]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_RotationCycle_JoinFanout
