import TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_Drawdown_DeleteMonotone

open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

/-! Brute-force sanity check on all lists of length <= 4 over a small range,
    filtering by all subsets of the value domain. -/
private def genLists : Nat → List (List Int)
  | 0 => [[]]
  | n + 1 => (genLists n).flatMap (fun l => [-3, -2, -1, 0, 1, 2, 3].map (fun v => v :: l))

private def genSubsets : List Int → List (List Int)
  | [] => [[]]
  | a :: l => (genSubsets l).flatMap (fun s => [s, a :: s])

#eval do
  let lists : List (List Int) := genLists 4
  let bad := lists.filter (fun x : List Int =>
    (genSubsets [-3, -2, -1, 0, 1, 2, 3]).any (fun s =>
      let keep : Int → Bool := fun v => decide (v ∈ s)
      Drawdown.measure (x.filter keep) > Drawdown.measure x))
  pure bad

private theorem zipIdx_succ {α : Type} (l : List α) (n : Nat) :
    l.zipIdx (n + 1) = (l.zipIdx n).map (fun x => (x.1, x.2 + 1)) := by
  induction l generalizing n with
  | nil => simp
  | cons a l ih => simp [List.zipIdx, ih, Nat.add_assoc]

private theorem pairs_cons (a : Int) (xs : Series) :
    pairs (a :: xs) = xs.map (fun b => (a, b)) ++ pairs xs := by
  unfold pairs
  simp [List.zipIdx]
  rw [zipIdx_succ]
  rw [List.flatMap_map]

private theorem max0_cons (c : Int) (l : List Int) : max0 (c :: l) = max c (max0 l) := by
  unfold max0
  rw [List.max?_cons]
  cases h : l.max? with
  | none =>
      simp
      omega
  | some m =>
      simp
      omega

private theorem max0_append (l₁ l₂ : List Int) :
    max0 (l₁ ++ l₂) = max (max0 l₁) (max0 l₂) := by
  induction l₁ with
  | nil => simp [max0]
  | cons a l₁ ih =>
      rw [List.cons_append, max0_cons, ih, max0_cons]
      omega

private theorem max0_map_filter_le (f : Int → Int) (p : Int → Bool) (l : List Int) :
    max0 ((l.filter p).map f) ≤ max0 (l.map f) := by
  induction l with
  | nil => simp [max0]
  | cons a l ih =>
      by_cases hp : p a
      · rw [List.filter_cons_of_pos hp, List.map_cons, List.map_cons, max0_cons, max0_cons]
        omega
      · rw [List.filter_cons_of_neg hp, List.map_cons, max0_cons]
        unfold max0 at *
        omega

private theorem measure_cons (a : Int) (xs : Series) :
    Drawdown.measure (a :: xs) =
      max (Drawdown.measure xs) (max0 (xs.map (fun b => a - b))) := by
  unfold Drawdown.measure
  rw [pairs_cons]
  rw [List.map_append]
  rw [max0_append]
  rw [List.map_map]
  change max (max0 (xs.map (fun b => a - b))) (max0 ((pairs xs).map (fun p => p.1 - p.2))) =
    max (max0 ((pairs xs).map (fun p => p.1 - p.2))) (max0 (xs.map (fun b => a - b)))
  omega

private theorem measure_le_cons (a : Int) (xs : Series) :
    Drawdown.measure xs ≤ Drawdown.measure (a :: xs) := by
  rw [measure_cons]
  unfold Drawdown.measure max0 at *
  omega

theorem sound_Drawdown_DeleteMonotone : Drawdown.DeleteMonotone.Goal := by
  intro x keep
  induction x with
  | nil => simp [Drawdown.measure, pairs, max0]
  | cons a xs ih =>
      by_cases ha : keep a
      · rw [List.filter_cons_of_pos ha]
        rw [measure_cons a xs, measure_cons a (xs.filter keep)]
        have h₁ := ih
        have h₂ := max0_map_filter_le (fun b => a - b) keep xs
        omega
      · rw [List.filter_cons_of_neg ha]
        have h := measure_le_cons a xs
        omega

end TinylambOpt.ResearchProofs.Atlas.Goal_Drawdown_DeleteMonotone
