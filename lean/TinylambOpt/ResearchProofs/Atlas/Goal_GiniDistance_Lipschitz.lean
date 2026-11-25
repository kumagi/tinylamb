import TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_GiniDistance_Lipschitz

open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures
open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.GiniDistance

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

private theorem distance_symm (a b : Int) : distance a b = distance b a := by
  unfold distance
  rw [show b - a = -(a - b) by grind, Int.natAbs_neg]

private theorem distance_triangle (a b c : Int) :
    distance a c ≤ distance a b + distance b c := by
  unfold distance
  have h' := Int.natAbs_add_le (c - b) (b - a)
  have heq : c - a = (c - b) + (b - a) := by grind
  have hnat : (c - a).natAbs ≤ (b - a).natAbs + (c - b).natAbs := by
    rw [heq]
    simpa [Nat.add_comm] using h'
  exact_mod_cast hnat

private theorem sum_abs_le (xs : List Int) (f : Int → Int) (c : Int)
    (_hc : 0 ≤ c) (h : ∀ x ∈ xs, ((f x).natAbs : Int) ≤ c) :
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
      have hlen : (↑xs.length + 1) * c = ↑xs.length * c + c := by grind
      rw [hlen]
      omega

private theorem sum_map_sub {α : Type} (xs : List α) (f g : α → Int) :
    (xs.map fun x => f x - g x).sum = (xs.map f).sum - (xs.map g).sum := by
  induction xs with
  | nil => simp
  | cons x xs ih => simp only [List.map_cons, List.sum_cons, ih]; omega

theorem sound_GiniDistance_Lipschitz : GiniDistance.Lipschitz.Goal := by
  intro x a b
  have hmeasure : ∀ c : Int,
      GiniDistance.measure (c :: x) =
        (x.map fun u => distance c u).sum + GiniDistance.measure x := by
    intro c
    simp only [GiniDistance.measure, pairs_cons, List.map_append, List.map_map,
      List.sum_append, Function.comp_def]
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
  have hsimpl : (x.map fun u => distance a u).sum + GiniDistance.measure x -
      ((x.map fun u => distance b u).sum + GiniDistance.measure x) =
      (x.map fun u => distance a u - distance b u).sum := by
    calc
      (x.map fun u => distance a u).sum + GiniDistance.measure x -
          ((x.map fun u => distance b u).sum + GiniDistance.measure x)
          = (x.map fun u => distance a u).sum - (x.map fun u => distance b u).sum := by omega
      _ = (x.map fun u => distance a u - distance b u).sum := by rw [sum_map_sub]
  rw [hsimpl]
  simpa [size] using h

end TinylambOpt.ResearchProofs.Atlas.Goal_GiniDistance_Lipschitz
