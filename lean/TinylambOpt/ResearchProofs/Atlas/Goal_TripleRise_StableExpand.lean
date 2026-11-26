import TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

/-!
SQL execution can use cumulative counts or ordered measures to answer range and rank
queries under the modeled bounds.

Proved

- `TripleRise.StableExpand` — `∀ x (n : Nat), measure (x.flatMap (repeated n)) =
  (n:Int)^3*measure x`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_TripleRise_StableExpand

open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures
open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.TripleRise

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

private theorem pairs_cons (a : Int) (xs : Series) :
    pairs (a :: xs) = xs.map (fun b => (a, b)) ++ pairs xs := by
  unfold pairs
  rw [List.zipIdx_cons]
  simp only [List.flatMap_cons, List.drop]
  rw [show (1 : Nat) = 0 + 1 by rfl, zipIdx_add]
  rw [List.flatMap_map]

private theorem triples_cons (a : Int) (xs : Series) :
    triples (a :: xs) = (pairs xs).map (fun (b, c) => (a, b, c)) ++ triples xs := by
  unfold triples
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

private def measureNat (x : Series) : Nat := (triples x).countP strict3

private theorem measureNat_cons (a : Int) (xs : Series) :
    measureNat (a :: xs) =
      (pairs xs).countP (fun (b, c) => a < b && b < c) + measureNat xs := by
  unfold measureNat
  rw [triples_cons, List.countP_append, countP_map_comp]
  rfl

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

theorem sound_TripleRise_StableExpand : TripleRise.StableExpand.Goal := by
  intro x n
  unfold TripleRise.measure
  change (measureNat (x.flatMap (repeated n)) : Int) = (n : Int) ^ 3 * (measureNat x : Int)
  rw [measureNat_expand]
  rw [Int.natCast_mul, Int.natCast_pow]

end TinylambOpt.ResearchProofs.Atlas.Goal_TripleRise_StableExpand
