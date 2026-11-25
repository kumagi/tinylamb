import TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas_OrderedMeasures_G4

open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

/-! #### Generic list helpers -/

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

private theorem countP_map_comp {α β : Type} (P : β → Bool) (g : α → β) (xs : List α) :
    (xs.map g).countP P = xs.countP (fun x => P (g x)) := by
  induction xs with
  | nil => rfl
  | cons x xs ih =>
      by_cases h : P (g x) = true <;> simp [h, ih]

private theorem cart_cons (a : Int) (xs y : Series) :
    cart (a :: xs) y = y.map (fun b => (a, b)) ++ cart xs y := by
  simp [cart]

private theorem pairs_sum_append (f : Int × Int → Int) (x y : Series) :
    ((pairs (x ++ y)).map f).sum =
      ((pairs x).map f).sum + ((pairs y).map f).sum + ((cart x y).map f).sum := by
  induction x with
  | nil => simp [pairs, cart]
  | cons a xs ih =>
      rw [List.cons_append, pairs_cons, pairs_cons, cart_cons]
      have hcomp : (f ∘ fun b : Int => (a, b)) = fun b => f (a, b) := rfl
      simp only [List.map_append, List.map_map, List.sum_append, hcomp]
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
      rw [List.map_append, List.countP_append]
      rw [countP_map_comp, countP_map_comp]
      rw [ih]
      omega

private theorem perm_sum : ∀ {l₁ l₂ : List Int}, l₁.Perm l₂ → l₁.sum = l₂.sum
  | _, _, .nil => rfl
  | _, _, .cons a p => by simp [perm_sum p]
  | _, _, .swap a b l => by simp; grind
  | _, _, .trans p q => (perm_sum p).trans (perm_sum q)

private theorem perm_map_sum (f : Int → Int) {l₁ l₂ : List Int} (p : l₁.Perm l₂) :
    (l₁.map f).sum = (l₂.map f).sum := by
  exact perm_sum (p.map f)

private theorem sq_nonneg (a : Int) : 0 ≤ a ^ 2 := by
  have h : a ^ 2 = a * a := by grind
  rw [h, ← Int.natAbs_mul_self (a := a)]
  exact Int.natCast_nonneg _

private theorem sum_of_sq_nonneg (xs : List Int) : 0 ≤ (xs.map (fun v => v ^ 2)).sum := by
  induction xs with
  | nil => simp
  | cons a as ih =>
      simp only [List.map_cons, List.sum_cons]
      have ha := sq_nonneg a
      omega

private theorem sum_map_sub_sq (a : Int) (xs : Series) :
    (xs.map (fun b => (a - b) ^ 2)).sum =
      (xs.length : Int) * a ^ 2 - 2 * a * xs.sum + (xs.map (fun b => b ^ 2)).sum := by
  induction xs with
  | nil => simp
  | cons b bs ih =>
      simp only [List.map_cons, List.sum_cons, List.length_cons]
      rw [ih]
      grind

private theorem sum_map_sq_shift (a b : Int) (xs : Series) :
    (xs.map (fun v => (v + a + b) ^ 2)).sum =
      (xs.map (fun v => (v + a) ^ 2)).sum + 2 * b * (xs.map (fun v => v + a)).sum +
        (xs.length : Int) * b ^ 2 := by
  induction xs with
  | nil => simp
  | cons v vs ih =>
      simp only [List.map_cons, List.sum_cons, List.length_cons]
      rw [ih]
      grind

namespace CenteredScatter

open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.CenteredScatter

theorem MomentCollapse_sound : MomentCollapse.Goal := by
  intro x
  induction x with
  | nil => simp [CenteredScatter.measure, pairs, size]
  | cons a xs ih =>
      simp only [CenteredScatter.measure, size] at ih ⊢
      rw [pairs_cons]
      simp only [List.map_append, List.map_map, List.sum_append, List.length_cons,
        List.sum_cons, List.map_cons] at ih ⊢
      change (xs.map (fun b => (a - b) ^ 2)).sum +
          (List.map (fun p : Int × Int => (p.1 - p.2) ^ 2) (pairs xs)).sum = _
      rw [sum_map_sub_sq, ih]
      grind

-- TODO(PairMerge): pending
-- TODO(Translation): pending
-- TODO(ReplicatedGroup): pending
-- TODO(OutlierCap): pending

end CenteredScatter

end TinylambOpt.ResearchProofs.Atlas_OrderedMeasures_G4
