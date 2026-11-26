import TinylambOpt.ResearchGoals.ResourcePolicies.WaitQueue
/-!
Registers the checked result for `WaitQueue.CanceledHeadNeedsRebatch.Goal`.

The theorem below either proves the stated claim under its assumptions or
refutes it with an explicit counterexample; no `sorry` or new axioms are used.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_WaitQueue_CanceledHeadNeedsRebatch
open TinylambOpt.ResearchGoals.ResourcePolicies.WaitQueue

/-!
Refutes `CanceledHeadNeedsRebatch`: after the head writer of `(t::ts)` is canceled, the
turn of the canceled queue re-admits the newly exposed head (a reader cohort), while the
canceled turn of the original queue is just `[t]` filtered to `[]`. With
`t = ⟨0,0,true⟩` and `ts = [⟨1,1,false⟩]` the left side is `[⟨1,1,false⟩]` and the right
side is `[]`.
-/

theorem refute : ¬ CanceledHeadNeedsRebatch.Goal := by
  intro h
  have key : ¬ (turn (cancel 0 0 ([⟨0, 0, true⟩, ⟨1, 1, false⟩] : List Ticket)) =
      cancel 0 0 (turn [⟨0, 0, true⟩, ⟨1, 1, false⟩])) := by decide
  exact key (h ⟨0, 0, true⟩ [⟨1, 1, false⟩] (by decide) (by decide))

end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_WaitQueue_CanceledHeadNeedsRebatch
