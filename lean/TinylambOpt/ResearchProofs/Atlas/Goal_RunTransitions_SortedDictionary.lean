import TinylambOpt.ResearchGoals.Atlas.OrderedMeasures

/-!
SQL execution can use cumulative counts or ordered measures to answer range and rank
queries under the modeled bounds.

Proved

- `RunTransitions.SortedDictionary` — `∀ x, x.Pairwise (· ≤ ·) → measure x =
  ((x.eraseDups.length-1:Nat):Int)`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_RunTransitions_SortedDictionary

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

private theorem eraseDups_sorted_cons (a b : Int) (bs : Series)
    (hab : a ≤ b) (hs : (b :: bs).Pairwise (· ≤ ·)) :
    (a :: b :: bs).eraseDups.length =
      if a = b then (b :: bs).eraseDups.length else (b :: bs).eraseDups.length + 1 := by
  by_cases heq : a = b
  · subst b
    simp [List.eraseDups_cons]
  · have hnot : a ∉ b :: bs := by
      intro hm
      rcases List.mem_cons.mp hm with rfl | hm
      · exact heq rfl
      · have hba := (List.pairwise_cons.mp hs).1 a hm
        omega
    have hself : (b :: bs).filter (fun z => !z == a) = b :: bs := by
      apply List.filter_eq_self.mpr
      intro z hz
      have hza : z ≠ a := by
        intro h
        subst z
        exact hnot hz
      simp [hza]
    rw [List.eraseDups_cons, hself]
    simp [heq]

theorem sound_RunTransitions_SortedDictionary : RunTransitions.SortedDictionary.Goal := by
  intro x hs
  induction x with
  | nil => simp [run_nil]
  | cons a xs ih =>
      cases xs with
      | nil => simp [run_singleton, List.eraseDups_cons]
      | cons b bs =>
          have htail : (b :: bs).Pairwise (· ≤ ·) := (List.pairwise_cons.mp hs).2
          have hab : a ≤ b := (List.pairwise_cons.mp hs).1 b (by simp)
          have hih := ih htail
          rw [run_cons]
          simp only [List.head?_cons, hih]
          rw [eraseDups_sorted_cons a b bs hab htail]
          have hlenpos : 0 < (b :: bs).eraseDups.length := by
            simp [List.eraseDups_cons]
          by_cases heq : a = b
          · subst b
            simp
          · have hne : (a == b) = false := by
              apply Bool.eq_false_of_ne_true
              intro h
              exact heq (of_decide_eq_true h)
            simp [hne, heq]
            omega

end TinylambOpt.ResearchProofs.Atlas.Goal_RunTransitions_SortedDictionary
