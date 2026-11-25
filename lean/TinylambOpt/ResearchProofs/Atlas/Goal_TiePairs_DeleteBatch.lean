import TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_TiePairs_DeleteBatch

open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures
open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.TiePairs
open TinylambOpt.ResearchGoals.PairAggregates (choose2)

/-! ### Pair and cartesian-product plumbing -/

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
      simp only [List.countP_append]
      rw [countP_map_comp, countP_map_comp, List.countP_map]
      simp only [Function.comp_def]
      rw [List.countP_append, ih]
      omega

/-! ### Binomial coefficients -/

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

/-! ### Counting equal-value pairs -/

private def tieCount (x : Series) : Nat :=
  (pairs x).countP (fun p => p.1 == p.2)

private theorem beq_comm_int (a b : Int) : (a == b) = (b == a) := by
  apply Bool.eq_iff_iff.mpr
  simpa only [beq_iff_eq] using eq_comm

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

private theorem tieCount_append (x y : Series) :
    tieCount (x ++ y) = tieCount x + tieCount y + (cart x y).countP (fun p => p.1 == p.2) := by
  exact pairs_countP_append (fun p => p.1 == p.2) x y

private theorem count_repeated_self (n : Nat) (a : Int) : (repeated n a).count a = n := by
  simp [repeated]

private theorem tieCount_repeated (n : Nat) (a : Int) : tieCount (repeated n a) = choose2 n := by
  induction n with
  | zero => rfl
  | succ n ih =>
      simp only [repeated, List.replicate_succ]
      rw [tieCount_cons]
      have hi : tieCount (List.replicate n a) = choose2 n := by
        simpa [repeated] using ih
      have hcount : (List.replicate n a).count a = n := by
        simp
      rw [hi, hcount]
      have hs := choose2_succ n
      omega

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

/-! ### The goal -/

theorem sound_TiePairs_DeleteBatch : TiePairs.DeleteBatch.Goal := by
  intro x a n
  change ((tieCount (repeated n a ++ x) : Nat) : Int) - (tieCount x : Int) =
    (choose2 n : Int) + (n : Int) * (x.count a : Int)
  rw [tieCount_append, tieCount_repeated, cart_tie_repeated]
  grind

end TinylambOpt.ResearchProofs.Atlas.Goal_TiePairs_DeleteBatch
