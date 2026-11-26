import TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

/-!
SQL execution can use cumulative counts or ordered measures to answer range and rank
queries under the modeled bounds.

Proved

- `TripleRise.MiddlePivot` — `∀ x, measure x = ((x.zipIdx.map (fun (v,i) => (x.take
  i).countP (· < v) * (x.drop (i+1)).countP (v < ·))).sum:Int)`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_TripleRise_MiddlePivot

open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures
open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.TripleRise

private theorem zipIdx_add {α : Type} (xs : List α) (n m : Nat) :
    xs.zipIdx (n + m) = (xs.zipIdx n).map (fun (v, i) => (v, i + m)) := by
  induction xs generalizing n with
  | nil => simp
  | cons a as ih =>
      simp only [List.zipIdx_cons, List.map_cons]
      congr 1
      simpa [Nat.add_assoc, Nat.add_comm, Nat.add_left_comm] using ih (n + 1)

private theorem countP_map_comp {α β : Type} (P : β → Bool) (g : α → β) (xs : List α) :
    (xs.map g).countP P = xs.countP (fun x => P (g x)) := by
  induction xs with
  | nil => rfl
  | cons x xs ih =>
      by_cases h : P (g x) = true <;> simp [h, ih]

private theorem sum_map_add_nat {α : Type} (xs : List α) (f g : α → Nat) :
    (xs.map (fun x => f x + g x)).sum = (xs.map f).sum + (xs.map g).sum := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      simp only [List.map_cons, List.sum_cons]
      rw [ih]
      omega

private theorem pairs_cons (a : Int) (xs : Series) :
    pairs (a :: xs) = xs.map (fun b => (a, b)) ++ pairs xs := by
  simp only [pairs, List.zipIdx_cons', List.flatMap_cons, List.flatMap_map,
    List.drop_succ_cons, List.drop_zero]
  rfl

private theorem triples_cons (a : Int) (xs : Series) :
    triples (a :: xs) = (pairs xs).map (fun p => (a, p.1, p.2)) ++ triples xs := by
  simp only [triples, List.zipIdx_cons', List.flatMap_cons, List.flatMap_map,
    List.drop_succ_cons, List.drop_zero]
  rfl

private def pivotSum (x : Series) : Nat :=
  (x.zipIdx.map (fun (v, i) =>
    (x.take i).countP (· < v) * (x.drop (i + 1)).countP (v < ·))).sum

private def boundSum (a : Int) (x : Series) : Nat :=
  (x.zipIdx.map (fun (v, i) =>
    (if a < v then 1 else 0) * (x.drop (i + 1)).countP (v < ·))).sum

private theorem boundSum_cons (a b : Int) (bs : Series) :
    boundSum a (b :: bs) = (if a < b then 1 else 0) * bs.countP (b < ·) + boundSum a bs := by
  unfold boundSum
  rw [List.zipIdx_cons]
  simp
  rw [show (1 : Nat) = 0 + 1 by omega, zipIdx_add]
  simp [Function.comp_def]

private theorem pairs_bound (a : Int) (xs : Series) :
    (pairs xs).countP (fun p => a < p.1 && p.1 < p.2) = boundSum a xs := by
  induction xs with
  | nil => rfl
  | cons b bs ih =>
      rw [pairs_cons, List.countP_append, countP_map_comp, ih, boundSum_cons]
      by_cases h : a < b <;> simp [h]

private theorem pivotSum_cons (a : Int) (xs : Series) :
    pivotSum (a :: xs) = pivotSum xs + boundSum a xs := by
  unfold pivotSum boundSum
  rw [List.zipIdx_cons]
  simp only [List.map_cons, List.sum_cons, List.take_zero, List.countP_nil,
    Nat.zero_mul, Nat.zero_add]
  have hz : xs.zipIdx 1 = xs.zipIdx.map (fun p => (p.1, p.2 + 1)) := by
    simpa using zipIdx_add xs 0 1
  rw [hz]
  simp [Function.comp_def, Nat.add_mul, Nat.add_comm, List.countP_cons]
  rw [sum_map_add_nat]

private theorem cast_sum_products {α : Type} (xs : List α) (f g : α → Nat) :
    ((xs.map (fun p => f p * g p)).sum : Int) =
      (xs.map (fun p => (f p : Int) * (g p : Int))).sum := by
  induction xs with
  | nil => simp
  | cons p ps ih =>
      simp only [List.map_cons, List.sum_cons]
      have hcasts : (((f p * g p) + (List.map (fun p => f p * g p) ps).sum : Nat) : Int) =
          (f p : Int) * (g p : Int) + ((List.map (fun p => f p * g p) ps).sum : Int) := by
        exact_mod_cast (rfl : (f p * g p) + (List.map (fun p => f p * g p) ps).sum =
          (f p * g p) + (List.map (fun p => f p * g p) ps).sum)
      rw [hcasts, ih]

theorem sound_TripleRise_MiddlePivot : TripleRise.MiddlePivot.Goal := by
  intro x
  have h : (triples x).countP strict3 = pivotSum x := by
    induction x with
    | nil => rfl
    | cons a xs ih =>
        rw [triples_cons, List.countP_append, countP_map_comp, ih, pivotSum_cons]
        have hb : (pairs xs).countP (fun p => a < p.1 && p.1 < p.2) = boundSum a xs :=
          pairs_bound a xs
        change (pairs xs).countP (fun p => strict3 (a, p.1, p.2)) = boundSum a xs at hb
        omega
  have hc : TripleRise.measure x = (pivotSum x : Int) := by
    unfold TripleRise.measure
    exact_mod_cast h
  rw [hc]
  unfold pivotSum
  exact cast_sum_products (x.zipIdx)
    (fun p => (x.take p.2).countP (· < p.1))
    (fun p => (x.drop (p.2 + 1)).countP (p.1 < ·))

end TinylambOpt.ResearchProofs.Atlas.Goal_TripleRise_MiddlePivot
