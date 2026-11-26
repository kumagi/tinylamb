import TinylambOpt.ResearchGoals.JitFrontiers.PrefixKernels
import TinylambOpt.ResearchProofs.JitFrontiers.PrefixKernels.Common_PrefixKernels

/-!
Prefix computations can use vector or block summaries when their carried state produces
the same per-row results.

Proved

- `LastPresent.LastValue` — `∀ rs s, apply (whole update rs) s=(rs.filterMap
  id).getLast?.getD s`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitFrontiers.PrefixKernels.Goal_LastPresent_LastValue

open TinylambOpt.ResearchProofs.JitFrontiers.PrefixKernels
open TinylambOpt.ResearchGoals.JitFrontiers.PrefixKernels
open TinylambOpt.ResearchGoals.JitFrontiers.CheckedRewrites (fits limit)

theorem sound : LastPresent.LastValue.Goal := by
  intro rs s
  induction rs generalizing s with
  | nil => simp [whole, apply]
  | cons r rs ih =>
      rw [whole_cons, apply_compose]
      cases r with
      | none =>
          simp [LastPresent.update, apply] at ih ⊢
          exact ih s
      | some x =>
          have hupd : apply (LastPresent.update (some x)) s = x := by
            simp [LastPresent.update, apply]
          rw [hupd, ih x]
          change (List.filterMap id rs).getLast?.getD x =
            (x :: List.filterMap id rs).getLast?.getD s
          rw [List.getLast?_cons, Option.getD_some]


end TinylambOpt.ResearchProofs.JitFrontiers.PrefixKernels.Goal_LastPresent_LastValue
