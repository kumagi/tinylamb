import TinylambOpt.ResearchGoals.JitKernels.InListLowering

/-!
A hot bitset partition and a cold residual partition can be evaluated
separately and combined with SQL three-valued OR (`disj`), because
`test` only depends on the probe value through three independent facts about
the key list: emptiness, membership of the probe, and membership of `NULL`.
All three compose over append (`List.isEmpty`, `List.contains_append`), and
`disj` is the pointwise three-valued OR, so evaluating the two partitions
separately and combining agrees with one `test` over the merged list. This
lets execution split an IN-list into a bitset and a residual comparison
chain without changing any query answer.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.JitKernels.InListLowering_InSetChunkedThreeValueOr
open TinylambOpt.ResearchGoals.JitKernels.InListLowering

private theorem disj_false_left (b : Option Bool) : disj (some false) b = b := by
  cases b with
  | none => rfl
  | some c => cases c <;> rfl

private theorem disj_false_right (a : Option Bool) : disj a (some false) = a := by
  cases a with
  | none => rfl
  | some c => cases c <;> rfl

/-- The pointwise three-valued OR of two `test` normal forms is the `test`
normal form of the disjunctive facts (probe membership and `NULL` presence
are the only relevant state). -/
private theorem bool_lem (A B C D : Bool) :
    disj (if A then some true else if C then none else some false)
      (if B then some true else if D then none else some false)
      = if A || B then some true else if C || D then none else some false := by
  revert A B C D
  decide

private theorem test_nil (v : Option Int) : test ([] : Keys) v = some false := rfl

private theorem test_cons_norm (xs : Keys) (v : Option Int) (h : xs ≠ []) :
    test xs v =
      if v.isNone then none
      else if xs.contains v then some true
      else if xs.contains none then none
      else some false := by
  cases xs with
  | nil => exact absurd rfl h
  | cons y ys => simp only [test, List.isEmpty_cons, if_false]; rfl

theorem sound : InSetChunkedThreeValueOr.Goal := by
  intro xs ys v
  cases xs with
  | nil => rw [List.nil_append, test_nil]; exact disj_false_left _
  | cons x xs =>
    cases ys with
    | nil => rw [List.append_nil, test_nil]; exact disj_false_right _
    | cons y ys =>
      have hx : x :: xs ≠ [] := by simp
      have hy : y :: ys ≠ [] := by simp
      have hxy : (x :: xs) ++ (y :: ys) ≠ [] := by simp
      rw [test_cons_norm _ _ hx, test_cons_norm _ _ hy, test_cons_norm _ _ hxy]
      cases v with
      | none => rfl
      | some w =>
        simp only [Option.isNone_some, reduceIte]
        rw [List.contains_append, List.contains_append]
        exact bool_lem _ _ _ _

end TinylambOpt.ResearchProofs.JitKernels.InListLowering_InSetChunkedThreeValueOr
