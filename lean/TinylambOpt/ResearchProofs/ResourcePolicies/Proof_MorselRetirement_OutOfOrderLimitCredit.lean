import Std
import TinylambOpt.ResearchGoals.ResourcePolicies.MorselRetirement

namespace TinylambOpt.ResearchProofs.ResourcePolicies.MorselRetirement.Goal_OutOfOrderLimitCredit

open TinylambOpt.ResearchGoals.ResourcePolicies.MorselRetirement


/-!
Refuted: a row count from finished morsels outside the contiguous retirement
prefix cannot justify LIMIT cancellation. An out-of-order later morsel may
contribute rows at the frontier while the prefix is still empty, so taking k
rows from the eventual ordered output differs from taking k from the reports
available now.
-/
set_option maxHeartbeats 0
set_option maxRecDepth 10000

open TinylambOpt.ResearchGoals.ResourcePolicies.MorselRetirement

theorem refute : ¬ TinylambOpt.ResearchGoals.ResourcePolicies.MorselRetirement.OutOfOrderLimitCredit.Goal := by
  intro h
  let rs : List Report := [(1, [0])]
  let future : List Report := [(0, [])]
  have hc : consistent (rs ++ future) := by
    intro a ha b hb hab
    simp [rs, future] at ha hb
    rcases ha with rfl | rfl <;> rcases hb with rfl | rfl <;> simp_all
  have hb := h rs 0 2 1 future hc (by decide) (by decide)
  have hbad : (emit (rs ++ future) 0 2).take 1 ≠ (emit rs 0 2).take 1 := by decide
  exact hbad hb
end TinylambOpt.ResearchProofs.ResourcePolicies.MorselRetirement.Goal_OutOfOrderLimitCredit
