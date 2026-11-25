import TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.DevinBatch.OM_RisePairs_TiePairs

open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures
open TinylambOpt.ResearchGoals.PairAggregates (choose2)

/-! ### Basic `pairs` lemmas -/

/-- Recursion for `pairs` on the head. -/
theorem pairs_cons (a : Int) (xs : Series) :
    pairs (a :: xs) = xs.map (fun b => (a, b)) ++ pairs xs := by
  simp only [pairs, List.zipIdx_cons', List.flatMap_cons, List.flatMap_map,
    List.drop_succ_cons, List.drop_zero]
  rfl

/-- `pairs` of a singleton is empty. -/
theorem pairs_singleton (a : Int) : pairs [a] = [] := by
  simp [pairs]

/-- Every component of a pair in `pairs x` is a member of `x`. -/
theorem mem_pairs {x : Series} {p : Int × Int} (h : p ∈ pairs x) :
    p.1 ∈ x ∧ p.2 ∈ x := by
  induction x with
  | nil => simp [pairs] at h
  | cons a xs ih =>
    rw [pairs_cons] at h
    rw [List.mem_append] at h
    rcases h with h | h
    · rw [List.mem_map] at h
      obtain ⟨b, hb, rfl⟩ := h
      exact ⟨List.mem_cons_self, List.mem_cons_of_mem a hb⟩
    · obtain ⟨h1, h2⟩ := ih h
      exact ⟨List.mem_cons_of_mem a h1, List.mem_cons_of_mem a h2⟩

/-- `flatMap` congruence on the function, pointwise on the list. -/
theorem flatMap_congr {α β : Type} {f g : α → List β} {l : List α}
    (h : ∀ a ∈ l, f a = g a) : l.flatMap f = l.flatMap g := by
  induction l with
  | nil => rfl
  | cons a l ih =>
    rw [List.flatMap_cons, List.flatMap_cons, h a (List.mem_cons_self),
      ih (fun b hb => h b (List.mem_cons_of_mem a hb))]

/-- Sum over a `flatMap` splits into per-element sums. -/
theorem sum_flatMap_nat {α : Type} {f : α → List Nat} (l : List α) :
    (l.flatMap f).sum = (l.map fun a => (f a).sum).sum := by
  induction l with
  | nil => rfl
  | cons a l ih => simp [List.flatMap_cons, List.map_cons, List.sum_append, ih]

/-! ### `choose2` arithmetic -/

theorem choose2_zero : choose2 0 = 0 := rfl

theorem choose2_two_mul (n : Nat) : 2 * choose2 n = n * (n - 1) := by
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

theorem choose2_succ (n : Nat) : choose2 (n + 1) = choose2 n + n := by
  have h1 := choose2_two_mul n
  have h2 := choose2_two_mul (n + 1)
  have hsub : n + 1 - 1 = n := by omega
  simp only [hsub] at h2
  have hp : n * (n - 1) + 2 * n = (n + 1) * n := by
    cases n with
    | zero => rfl
    | succ m =>
        simp only [Nat.add_sub_cancel, Nat.succ_mul, Nat.mul_succ]
        omega
  omega

/-- `pairs` commutes with `map` (as an equality of lists). -/
theorem pairs_map (f : Int → Int) (x : Series) :
    pairs (x.map f) = (pairs x).map (fun p => (f p.1, f p.2)) := by
  induction x with
  | nil => rfl
  | cons a xs ih =>
    simp only [List.map_cons, pairs_cons, ih, List.map_append, List.map_map,
      Function.comp_def]

/-- `pairs` has `choose2` many elements. -/
theorem pairs_length (x : Series) : (pairs x).length = choose2 x.length := by
  induction x with
  | nil => rfl
  | cons a xs ih =>
    rw [pairs_cons, List.length_append, List.length_map, ih]
    simp only [List.length_cons]
    simpa [Nat.add_comm] using (choose2_succ xs.length).symm

/-! ### `cart` and cross-pair counting -/

theorem cart_cons (a : Int) (xs y : Series) :
    cart (a :: xs) y = y.map (fun b => (a, b)) ++ cart xs y := by
  simp [cart]

theorem countP_map_comp {α β : Type} (P : β → Bool) (g : α → β) (xs : List α) :
    (xs.map g).countP P = xs.countP (fun x => P (g x)) := by
  induction xs with
  | nil => rfl
  | cons x xs ih =>
      by_cases h : P (g x) = true <;> simp [List.countP_cons, h, ih]

/-- `countP` over `pairs (x ++ y)` splits into the three blocks. -/
theorem pairs_countP_append (P : Int × Int → Bool) (x y : Series) :
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

/-- `countP` over a cartesian product unfolds to per-element counts. -/
theorem countP_cart {P : Int × Int → Bool} (u v : Series) :
    (cart u v).countP P = (u.map fun a => v.countP (fun b => P (a, b))).sum := by
  have h : ∀ a : Int, (v.map fun b => (a, b)).countP P =
      v.countP (fun b => P (a, b)) := by
    intro a
    rw [List.countP_map]
    rfl
  unfold cart
  rw [List.countP_flatMap]
  simp only [Function.comp_def]
  congr 1
  apply congrArg List.sum
  apply List.map_congr_left
  intro a _
  exact h a

end TinylambOpt.ResearchProofs.DevinBatch.OM_RisePairs_TiePairs
