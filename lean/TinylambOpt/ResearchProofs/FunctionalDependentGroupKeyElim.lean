import TinylambOpt.ResearchGoals.FunctionalDependentGroupKeyElim

/-!
Refuted by counterexample

- `Goal` — Functional-dependent group-key elimination (`GROUP BY k, k-1, k-2, k-3` on
  ClickBench Q36): when all inputs satisfy `k ∈ [MIN_I64 + 3, MAX_I64]`, grouping
  only by `k` and materializing `(k-1, k-2, k-3)` after `Top-K` yields exact equality
  with 4-key grouping; deferring `k-1` evaluation past `LIMIT K` without checking all
  inputs silently hides overflow errors on dropped rows.

The reference model is defined in
`TinylambOpt.ResearchGoals.FunctionalDependentGroupKeyElim`.
-/

namespace TinylambOpt.ResearchProofs.FunctionalDependentGroupKeyElim

open TinylambOpt.ResearchGoals.FunctionalDependentGroupKeyElim

/-
  The research goal as stated is FALSE, so `sound` cannot be proved.

  Concrete counterexample to the first conjunct: `lim = 1`, `rows = [int64Max + 1]`.

    refGroupedTopK 1 [int64Max + 1]
      = some [(⟨int64Max + 1, int64Max, int64Max - 1, int64Max - 2⟩, 1)]

    optKeyOnlyTopK 1 [int64Max + 1]
      = none

  Reason: subtracting 1, 2, 3 from a value just above int64Max lands back
  inside [int64Min, int64Max], so `fullKeyOf (int64Max + 1)` succeeds and the
  reference path returns `some`.  The optimized path, however, rejects the row
  with its guard `int64Min + 3 ≤ k ∧ k ≤ int64Max` and returns `none`.

  The second conjunct is true (`lim = 1`, `rows = [10, 10, int64Min]`: the
  unchecked deferral returns `some [(⟨10, 9, 8, 7⟩, 2)]` while the reference
  returns `none`), but the conjunction as a whole is unprovable.
-/

private theorem counterexample :
    optKeyOnlyTopK 1 [int64Max + 1] ≠ refGroupedTopK 1 [int64Max + 1] := by
  decide

theorem disproof : ¬ TinylambOpt.ResearchGoals.FunctionalDependentGroupKeyElim.Goal := by
  intro h
  exact counterexample (h.1 1 [int64Max + 1])

-- Placeholder: the goal is false (see `disproof`), so no `sound` proof exists.
-- theorem sound : TinylambOpt.ResearchGoals.FunctionalDependentGroupKeyElim.Goal := by
--   ...

end TinylambOpt.ResearchProofs.FunctionalDependentGroupKeyElim
