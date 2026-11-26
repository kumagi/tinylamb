import TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

/-!
SQL execution can use cumulative counts or ordered measures to answer range and rank
queries under the modeled bounds.

Proved

- `RisePairs.BlockRepeat` — `∀ x n, measure (List.replicate n x |>.flatten) =
  (n:Int)*measure x + (choose2 n:Int)*(cart x x).countP rising`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_RisePairs_BlockRepeat

open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures
open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.RisePairs
open TinylambOpt.ResearchGoals.PairAggregates (choose2)

private def riseNat (x : Series) : Nat := (pairs x).countP rising

private theorem pairs_cons (a : Int) (xs : Series) :
    pairs (a :: xs) = xs.map (fun b => (a, b)) ++ pairs xs := by
  simp only [pairs, List.zipIdx_cons', List.flatMap_cons, List.flatMap_map,
    List.drop_succ_cons, List.drop_zero]
  rfl

private theorem countP_map_comp {α β : Type} (P : β → Bool) (g : α → β) (xs : List α) :
    (xs.map g).countP P = xs.countP (fun x => P (g x)) := by
  induction xs with
  | nil => rfl
  | cons x xs ih =>
      by_cases h : P (g x) = true <;> simp [h, ih]

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
      simp only [List.countP_append, List.map_append]
      rw [countP_map_comp, countP_map_comp]
      rw [ih]
      omega

private theorem cart_append_right_countP (P : Int × Int → Bool) (x y z : Series) :
    (cart x (y ++ z)).countP P = (cart x y).countP P + (cart x z).countP P := by
  induction x with
  | nil => simp [cart]
  | cons a xs ih =>
      rw [cart_cons, cart_cons, cart_cons]
      simp only [List.map_append, List.countP_append]
      rw [ih]
      omega

private theorem choose2_twice (n : Nat) : 2 * choose2 n = n * (n - 1) := by
  unfold choose2
  rcases Nat.mod_two_eq_zero_or_one n with h0 | h1
  · have he : n = 2 * (n / 2) := by omega
    rw [he]
    have hd : 2 ∣ (2 * (n / 2)) * (2 * (n / 2) - 1) := by
      refine ⟨(n / 2) * (2 * (n / 2) - 1), ?_⟩
      simp [Nat.mul_assoc]
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

private theorem cross_blocks (x : Series) (k : Nat) :
    (cart x ((List.replicate k x).flatten)).countP rising =
      k * (cart x x).countP rising := by
  induction k with
  | zero => simp [cart]
  | succ j ih =>
      rw [List.replicate_succ, List.flatten_cons, cart_append_right_countP, ih]
      simp [Nat.succ_mul, Nat.add_comm]

private theorem main_nat (x : Series) (n : Nat) :
    riseNat ((List.replicate n x).flatten) =
      n * riseNat x + choose2 n * (cart x x).countP rising := by
  induction n with
  | zero => simp [riseNat, pairs, choose2]
  | succ k ih =>
      unfold riseNat at ih ⊢
      rw [List.replicate_succ, List.flatten_cons, pairs_countP_append rising,
        cross_blocks, ih]
      rw [show choose2 (Nat.succ k) = choose2 k + k from choose2_succ k]
      simp [Nat.succ_mul, Nat.add_mul, Nat.add_comm, Nat.add_left_comm, Nat.add_assoc]

theorem sound_RisePairs_BlockRepeat : RisePairs.BlockRepeat.Goal := by
  intro x n
  unfold RisePairs.measure
  exact_mod_cast main_nat x n

end TinylambOpt.ResearchProofs.Atlas.Goal_RisePairs_BlockRepeat
