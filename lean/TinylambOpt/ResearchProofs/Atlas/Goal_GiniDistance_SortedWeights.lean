import TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_GiniDistance_SortedWeights

open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures
open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.GiniDistance

/-! ### Exhaustive sanity check on small inputs -/

private def rowsOf : Nat → List Int → List (List Int)
  | 0, _ => [[]]
  | n + 1, pool => pool.flatMap (fun r => (rowsOf n pool).map (r :: ·))

private def smallPool : List Int := [-2, -1, 0, 1, 2]

#eval (rowsOf 4 smallPool).all (fun x =>
  decide (measure x =
    (x.mergeSort.zipIdx.map (fun (v, i) => (2 * (i : Int) + 1 - size x) * v)).sum))

/-! ### List algebra -/

private theorem zipIdx_succ (ys : Series) (i : Nat) :
    ys.zipIdx (i + 1) = (ys.zipIdx i).map (fun (v, j) => (v, j + 1)) := by
  induction ys generalizing i with
  | nil => simp
  | cons a ys ih =>
      rw [List.zipIdx_cons, List.zipIdx_cons, ih (i + 1)]
      simp

private theorem pairs_cons (a : Int) (ys : Series) :
    pairs (a :: ys) = ys.map (fun b => (a, b)) ++ pairs ys := by
  unfold pairs
  rw [List.zipIdx_cons]
  simp only [List.flatMap_cons, Nat.zero_add, List.drop_one]
  congr 1
  rw [zipIdx_succ ys 0]
  simp only [List.flatMap_map]
  simp

private theorem sum_map_add {α : Type} (xs : List α) (f g : α → Int) :
    (xs.map (fun x => f x + g x)).sum = (xs.map f).sum + (xs.map g).sum := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      simp only [List.map_cons, List.sum_cons]
      rw [ih]
      grind

private theorem sum_map_const {α : Type} (xs : List α) (c : Int) :
    (xs.map (fun _ => c)).sum = (xs.length : Int) * c := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      simp only [List.map_cons, List.sum_cons, List.length_cons, Int.natCast_add,
        Int.natCast_one]
      rw [ih]
      grind

private theorem sum_map_sub_const (ys : Series) (a : Int) :
    (ys.map (fun b => b - a)).sum = ys.sum - (ys.length : Int) * a := by
  have h := sum_map_add ys (fun b : Int => b) (fun _ : Int => -a)
  rw [sum_map_const] at h
  simp at h
  rw [show (ys.map (fun b => b - a)).sum = (ys.map (fun b => b + (-a))).sum by
    apply congrArg List.sum
    apply List.map_congr_left
    intro b hb
    omega]
  rw [h]
  grind

private theorem map_sum_of_perm {α : Type} (f : α → Int) {xs ys : List α}
    (h : xs.Perm ys) : (xs.map f).sum = (ys.map f).sum := by
  induction h with
  | nil => rfl
  | cons a p ih =>
      simp only [List.map_cons, List.sum_cons]
      rw [ih]
  | swap a b l =>
      simp only [List.map_cons, List.sum_cons]
      omega
  | trans p₁ p₂ ih₁ ih₂ => exact ih₁.trans ih₂

/-! ### Absolute deviation -/

private theorem distance_of_le (a b : Int) (h : a ≤ b) : distance a b = b - a := by
  unfold distance
  have hnn : 0 ≤ b - a := by omega
  exact Int.natAbs_of_nonneg hnn

private theorem distance_symm (a b : Int) : distance a b = distance b a := by
  unfold distance
  have h : a - b = -(b - a) := by omega
  rw [h, Int.natAbs_neg]

private theorem measure_cons (a : Int) (ys : Series) :
    GiniDistance.measure (a :: ys) =
      (ys.map (fun b => distance a b)).sum + GiniDistance.measure ys := by
  unfold GiniDistance.measure
  rw [pairs_cons, List.map_append, List.sum_append]
  congr 1
  rw [List.map_map]
  apply congrArg List.sum
  apply List.map_congr_left
  intro b hb
  rfl

/-! ### Permutation invariance of the pairwise sum -/

private theorem measure_swap (a b : Int) (ys : Series) :
    GiniDistance.measure (b :: a :: ys) = GiniDistance.measure (a :: b :: ys) := by
  rw [measure_cons b (a :: ys), measure_cons a ys,
    measure_cons a (b :: ys), measure_cons b ys]
  simp only [List.map_cons, List.sum_cons]
  rw [distance_symm b a]
  omega

private theorem measure_of_perm {x y : Series} (h : x.Perm y) :
    GiniDistance.measure x = GiniDistance.measure y := by
  induction h with
  | nil => rfl
  | cons a p ih =>
      rw [measure_cons, measure_cons, ih, map_sum_of_perm (fun b => distance a b) p]
  | swap a b l => exact measure_swap a b l
  | trans p₁ p₂ ih₁ ih₂ => exact ih₁.trans ih₂

/-! ### The closed form on a sorted list -/

private def weighted (off : Nat) (n : Int) (y : Series) : Int :=
  (y.zipIdx off).map (fun (v, i) => (2 * (i : Int) + 1 - n) * v) |>.sum

private theorem weighted_cons (off : Nat) (n : Int) (a : Int) (ys : Series) :
    weighted off n (a :: ys) =
      (2 * (off : Int) + 1 - n) * a + weighted (off + 1) n ys := by
  unfold weighted
  rw [List.zipIdx_cons]
  simp only [List.map_cons, List.sum_cons]

private theorem weighted_shift : ∀ (ys : Series) (off : Nat) (n : Int),
    weighted (off + 1) (n + 1) ys = weighted off n ys + ys.sum
  | [], _, _ => by simp [weighted]
  | a :: ys, off, n => by
      rw [weighted_cons, weighted_cons, weighted_shift ys (off + 1) n]
      simp only [List.sum_cons]
      grind

private theorem weighted_one_succ (ys : Series) :
    weighted 1 ((size ys : Int) + 1) ys = weighted 0 (size ys) ys + ys.sum := by
  exact weighted_shift ys 0 (size ys)

private theorem sorted_identity (y : Series) :
    y.Pairwise (fun a b : Int => a ≤ b) →
    GiniDistance.measure y = weighted 0 (size y) y := by
  induction y with
  | nil =>
      intro hp
      simp [GiniDistance.measure, weighted, pairs]
  | cons a ys ih =>
      intro hp
      have hp' := List.pairwise_cons.mp hp
      have ihys := ih hp'.2
      have hdist : (ys.map (fun b => distance a b)).sum =
          (ys.map (fun b => b - a)).sum := by
        apply congrArg List.sum
        apply List.map_congr_left
        intro b hb
        exact distance_of_le a b (hp'.1 b hb)
      rw [measure_cons, hdist, ihys]
      rw [weighted_cons]
      have hlen : size (a :: ys) = (size ys : Int) + 1 := by simp [size]
      rw [hlen, weighted_one_succ, sum_map_sub_const]
      simp [size]
      grind

/-! ### Main theorem -/

theorem sound_GiniDistance_SortedWeights : GiniDistance.SortedWeights.Goal := by
  intro x
  let le : Int → Int → Bool := fun a b => a ≤ b
  have hperm : x.mergeSort.Perm x := by
    simpa [le] using List.mergeSort_perm x le
  have hsorted : x.mergeSort.Pairwise (fun a b : Int => a ≤ b) := by
    have h := List.pairwise_mergeSort (le := le)
      (by
        intro a b c hab hbc
        simp [le] at hab hbc ⊢
        omega)
      (by
        intro a b
        by_cases h : a ≤ b
        · simp [le, h]
        · have h' : b ≤ a := by omega
          simp [le, h, h'])
      x
    simpa [le] using h
  have hlen : x.mergeSort.length = x.length := hperm.length_eq
  have hsize : size x.mergeSort = size x := by
    unfold size
    rw [hlen]
  have h3 : weighted 0 (size x.mergeSort) x.mergeSort =
      (x.mergeSort.zipIdx.map (fun (v, i) => (2 * (i : Int) + 1 - size x) * v)).sum := by
    unfold weighted
    rw [hsize]
  exact (measure_of_perm hperm).symm.trans ((sorted_identity x.mergeSort hsorted).trans h3)

end TinylambOpt.ResearchProofs.Atlas.Goal_GiniDistance_SortedWeights
