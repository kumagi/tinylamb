import TinylambOpt.ResearchGoals.JitKernels.InListLowering

/-!
For `WHERE IN` only acceptance (`some true`) is observed, so NULL build rows can
be dropped. Acceptance of a non-NULL probe is exactly membership of that probe
in the build list, and membership is unaffected by filtering out NULL entries;
a NULL probe is never accepted. SQL execution may therefore prune NULL build
keys from the hot set when only the positive IN result is consumed.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.InListLowering.Goal_NullEntryFilterElision

open TinylambOpt.ResearchGoals.JitKernels.InListLowering

theorem keep_some_eq (xs : Keys) (x : Int) :
    keep xs (some x) = decide (some x ∈ xs) := by
  unfold keep test
  by_cases hm : some x ∈ xs
  · have hn : xs ≠ [] := by
      intro he
      subst xs
      simp at hm
    simp [hm, hn]
  · by_cases he : xs = []
    · simp [he]
    · simp [hm, he]

theorem keep_none_false (xs : Keys) : keep xs none = false := by
  unfold keep test
  by_cases h : xs.isEmpty = true
  · simp [h]
  · simp [h]

theorem sound : NullEntryFilterElision.Goal := by
  intro xs v
  cases v with
  | none => rw [keep_none_false, keep_none_false]
  | some x => rw [keep_some_eq, keep_some_eq]; simp

end TinylambOpt.ResearchProofs.JitKernels.InListLowering.Goal_NullEntryFilterElision
