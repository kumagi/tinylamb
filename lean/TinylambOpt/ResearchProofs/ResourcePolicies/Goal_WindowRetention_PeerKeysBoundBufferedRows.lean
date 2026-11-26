import TinylambOpt.ResearchGoals.ResourcePolicies.WindowRetention
/-!
Registers the checked result for `WindowRetention.PeerKeysBoundBufferedRows.Goal`.

The theorem below either proves the stated claim under its assumptions or
refutes it with an explicit counterexample; no `sorry` or new axioms are used.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_WindowRetention_PeerKeysBoundBufferedRows
open TinylambOpt.ResearchGoals.ResourcePolicies.WindowRetention

/-!
Refutes `PeerKeysBoundBufferedRows`: without a distinctness hypothesis on `keys`, the run
of rows sharing key `k` can be longer than the number of distinct keys. Counterexample:
keys = [0, 0], k = 0 — the key filter keeps 2 rows but `eraseDups` has length 1.
-/

theorem refute : ¬ PeerKeysBoundBufferedRows.Goal := by
  intro h
  have key : ¬ ((([0, 0] : List Nat).filter fun x => x == 0).length ≤ ([0, 0]).eraseDups.length) := by
    decide
  exact key (h [0, 0] 0)

end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_WindowRetention_PeerKeysBoundBufferedRows
