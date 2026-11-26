import TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

/-!
SQL execution can use cumulative counts or ordered measures to answer range and rank
queries under the modeled bounds.

Proved

- `RunTransitions.CoarseDictionary` — `∀ x (f : Int → Int), measure x - measure (x.map
  f) = (adj x).countP (fun (a,b) => a != b && f a == f b)`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_RunTransitions_CoarseDictionary

open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures
open TinylambOpt.ResearchGoals.Atlas.OrderedMeasures.RunTransitions

private theorem run_nil : RunTransitions.measure [] = 0 := rfl

private theorem run_singleton (a : Int) : RunTransitions.measure [a] = 0 := rfl

private theorem run_cons (a : Int) (xs : Series) :
    RunTransitions.measure (a :: xs) = RunTransitions.measure xs +
      match xs.head? with
      | none => 0
      | some b => if a == b then 0 else 1 := by
  cases xs with
  | nil => simp [run_nil, run_singleton]
  | cons b bs =>
      simp only [RunTransitions.measure, adj, List.drop_one, List.tail_cons]
      rw [List.zip_cons_cons, List.countP_cons]
      by_cases h : a = b
      · subst b
        simp
      · simp [h, beq_iff_eq]

theorem sound_RunTransitions_CoarseDictionary : CoarseDictionary.Goal := by
  intro x f
  induction x with
  | nil => rfl
  | cons a xs ih =>
      cases xs with
      | nil => rfl
      | cons b bs =>
          change RunTransitions.measure (b :: bs) - RunTransitions.measure (f b :: List.map f bs) = _ at ih
          simp only [List.map_cons]
          calc
            RunTransitions.measure (a :: b :: bs) - RunTransitions.measure (f a :: f b :: List.map f bs) =
                (RunTransitions.measure (b :: bs) + (if a == b then (0 : Int) else 1)) -
                (RunTransitions.measure (f b :: List.map f bs) +
                  (if f a == f b then (0 : Int) else 1)) := by
                    simp only [run_cons (a := a) (xs := b :: bs),
                      run_cons (a := f a) (xs := f b :: List.map f bs), List.head?_cons]
            _ = (RunTransitions.measure (b :: bs) - RunTransitions.measure (f b :: List.map f bs)) +
                (if a == b then (0 : Int) else 1) -
                (if f a == f b then (0 : Int) else 1) := by grind
            _ = ((adj (b :: bs)).countP
                    (fun t => t.1 != t.2 && f t.1 == f t.2) : Int) +
                (if a == b then (0 : Int) else 1) -
                (if f a == f b then (0 : Int) else 1) := by rw [ih]
            _ = ((adj (a :: b :: bs)).countP
                    (fun t => t.1 != t.2 && f t.1 == f t.2) : Int) := by
                    simp only [adj, List.drop_one, List.tail_cons]
                    rw [List.zip_cons_cons, List.countP_cons]
                    by_cases hab : a = b
                    · subst b
                      simp
                    · by_cases hfab : f a = f b
                      · simp [hab, hfab, beq_iff_eq]
                      · simp [hab, hfab, beq_iff_eq]

end TinylambOpt.ResearchProofs.Atlas.Goal_RunTransitions_CoarseDictionary
