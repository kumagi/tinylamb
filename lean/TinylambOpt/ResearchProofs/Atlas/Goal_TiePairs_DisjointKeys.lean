import TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_TiePairs_DisjointKeys

open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

/-!
  `TiePairs.DisjointKeys`: for two lists with disjoint value sets, the number of
  ordered equal-valued pairs of the concatenation is the sum of the two counts.

  Small-input exhaustive check over all pairs of short integer lists: every
  admissible pair satisfies the equality (`#eval` prints `true`).
-/
#eval
  let xss : List (List Int) := [[], [0], [1], [0,1], [2,2], [1,2,1]]
  xss.all (fun x => xss.all (fun y =>
    (∀ a ∈ x, a ∉ y) →
      TiePairs.measure (x ++ y) == TiePairs.measure x + TiePairs.measure y))

/-- The pairs of `a :: xs` are the pairs starting at `a`, followed by the pairs
    internal to `xs`: the indices of `xs` simply shift by one. -/
private theorem pairs_cons (a : Int) (xs : List Int) :
    pairs (a :: xs) = xs.map (fun b => (a, b)) ++ pairs xs := by
  unfold pairs
  rw [List.zipIdx_cons]
  rw [show xs.zipIdx 1 = xs.zipIdx.map (fun p : Int × Nat => (p.1, p.2 + 1)) by
    simpa using (List.zipIdx_succ (l := xs) (i := 0))]
  simp [List.flatMap_cons, List.flatMap_map]

/-- Counting the elements equal to `a` in a list that excludes `a` gives zero. -/
private theorem countP_eq_zero_of_not_mem {α : Type} [BEq α] [LawfulBEq α]
    (l : List α) (a : α) (h : a ∉ l) : l.countP (fun b => a == b) = 0 := by
  induction l with
  | nil => simp
  | cons b bs ih =>
      have hba : a ≠ b := by
        intro heq
        apply h
        simp [heq]
      have htail : a ∉ bs := by
        intro ha
        apply h
        simp [ha]
      simp [hba, ih htail]

/-- Adding a head contributes one tie for every equal value in the tail. -/
private theorem measure_cons (a : Int) (xs : List Int) :
    TiePairs.measure (a :: xs) =
      TiePairs.measure xs + (xs.countP (fun b => a == b) : Int) := by
  unfold TiePairs.measure
  rw [pairs_cons, List.countP_append]
  have hmap : (xs.map (fun b => (a, b))).countP (fun p : Int × Int => p.1 == p.2) =
      xs.countP (fun b => a == b) := by
    rw [List.countP_map]
    have hf : ((fun p : Int × Int => p.1 == p.2) ∘ fun b : Int => (a, b)) =
        (fun b : Int => a == b) := by
      funext b
      rfl
    rw [hf]
  rw [hmap]
  omega

theorem sound_TiePairs_DisjointKeys : TiePairs.DisjointKeys.Goal := by
  intro x y hdisj
  induction x with
  | nil => simp [TiePairs.measure, pairs]
  | cons a xs ih =>
      have htail : ∀ c ∈ xs, c ∉ y := by
        intro c hc hy
        exact hdisj c (by simp [hc]) hy
      have hnot : a ∉ y := hdisj a (by simp)
      rw [show (a :: xs) ++ y = a :: (xs ++ y) by rfl]
      rw [measure_cons a (xs ++ y), measure_cons a xs]
      rw [ih htail]
      have hcnt : (xs ++ y).countP (fun b => a == b) =
          xs.countP (fun b => a == b) := by
        rw [List.countP_append, countP_eq_zero_of_not_mem y a hnot]
        omega
      rw [hcnt]
      omega

end TinylambOpt.ResearchProofs.Atlas.Goal_TiePairs_DisjointKeys
