import TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_GiniDistance_SeparatedBlocks

open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures
open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.GiniDistance

/-! ### Exhaustive sanity check on small inputs -/

private def rowsOf : Nat → List Int → List (List Int)
  | 0, _ => [[]]
  | n + 1, pool => pool.flatMap (fun r => (rowsOf n pool).map (r :: ·))

private def smallPool : List Int := [-2, -1, 0, 1, 2]

#eval (rowsOf 3 smallPool).all (fun x =>
  (rowsOf 3 smallPool).all (fun y =>
    decide ((∀ a ∈ x, ∀ b ∈ y, a ≤ b) →
      measure (x ++ y) = measure x + measure y + size x * y.sum - size y * x.sum)))

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

/-! ### Absolute deviation on separated blocks -/

private theorem distance_of_le (a b : Int) (h : a ≤ b) : distance a b = b - a := by
  unfold distance
  have hnn : 0 ≤ b - a := by omega
  exact Int.natAbs_of_nonneg hnn

private theorem tail_bound {a : Int} {xs y : Series}
    (h : ∀ a' ∈ a :: xs, ∀ b ∈ y, a' ≤ b) :
    ∀ a' ∈ xs, ∀ b ∈ y, a' ≤ b := by
  intro a' ha' b hb
  exact h a' (by simp [ha']) b hb

private theorem head_bound {a : Int} {xs y : Series}
    (h : ∀ a' ∈ a :: xs, ∀ b ∈ y, a' ≤ b) :
    ∀ b ∈ y, a ≤ b := by
  intro b hb
  exact h a (by simp) b hb

/-! ### Decomposition along a separated concatenation -/

private theorem separated_decomp (x y : Series)
    (h : ∀ a ∈ x, ∀ b ∈ y, a ≤ b) :
    GiniDistance.measure (x ++ y) = GiniDistance.measure x + GiniDistance.measure y +
      ((cart x y).map (fun t : Int × Int => distance t.1 t.2)).sum := by
  induction x with
  | nil =>
      simp [GiniDistance.measure, cart, pairs]
  | cons a xs ih =>
      rw [List.cons_append, measure_cons, ih (tail_bound h), measure_cons]
      simp only [cart, List.flatMap_cons, List.map_append, List.sum_append, List.map_map,
        Function.comp_def]
      omega

private theorem cross_sum (x y : Series)
    (h : ∀ a ∈ x, ∀ b ∈ y, a ≤ b) :
    ((cart x y).map (fun t : Int × Int => distance t.1 t.2)).sum =
      size x * y.sum - size y * x.sum := by
  induction x with
  | nil =>
      simp [cart, size]
  | cons a xs ih =>
      have hdist : (y.map (fun b => distance a b)).sum =
          (y.map (fun b => b - a)).sum := by
        apply congrArg List.sum
        apply List.map_congr_left
        intro b hb
        exact distance_of_le a b (head_bound h b hb)
      rw [show ((cart (a :: xs) y).map (fun t : Int × Int => distance t.1 t.2)).sum =
              (y.map (fun b => distance a b)).sum +
                ((cart xs y).map (fun t : Int × Int => distance t.1 t.2)).sum by
            simp only [cart, List.flatMap_cons, List.map_append, List.sum_append, List.map_map,
              Function.comp_def]]
      rw [hdist, ih (tail_bound h), sum_map_sub_const]
      simp [size]
      grind

/-! ### Main theorem -/

theorem sound_GiniDistance_SeparatedBlocks : GiniDistance.SeparatedBlocks.Goal := by
  intro x y h
  rw [separated_decomp x y h, cross_sum x y h]
  omega

end TinylambOpt.ResearchProofs.Atlas.Goal_GiniDistance_SeparatedBlocks
