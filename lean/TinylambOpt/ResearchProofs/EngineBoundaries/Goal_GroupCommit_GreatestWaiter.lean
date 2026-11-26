import TinylambOpt.ResearchGoals.EngineBoundaries.GroupCommit

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.EngineBoundaries.Goal_GroupCommit_GreatestWaiter

open TinylambOpt.ResearchGoals.EngineBoundaries.GroupCommit

private def maxList : List Nat → Nat
  | [] => 0
  | x :: xs => max x (maxList xs)

private theorem maxList_eq (l : List Nat) : l.max?.getD 0 = maxList l := by
  induction l with
  | nil => rfl
  | cons x xs ih =>
      cases hm : xs.max? with
      | none =>
          have hnil : xs = [] := List.max?_eq_none_iff.mp hm
          subst xs
          simp [maxList]
      | some m =>
          rw [hm] at ih
          simp at ih
          rw [List.max?_cons, hm]
          simp [maxList, ih]

private theorem maxBound (l : List Nat) (d : Nat) : (∀ x ∈ l, x ≤ d) ↔ l.max?.getD 0 ≤ d := by
  rw [maxList_eq]
  induction l with
  | nil => simp [maxList]
  | cons x xs ih => simp [maxList, ih, Nat.max_le]

theorem sound : GreatestWaiter.Goal := by
  intro s ws
  rw [ready, List.filter_eq_self]
  constructor
  · intro h
    apply (maxBound ws s.durable).mp
    intro x hx
    have hb := h x hx
    simpa [decide_eq_true_eq] using hb
  · intro h x hx
    have hb := (maxBound ws s.durable).mpr h x hx
    simpa [decide_eq_true_eq] using hb

end TinylambOpt.ResearchProofs.EngineBoundaries.Goal_GroupCommit_GreatestWaiter
