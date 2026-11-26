import TinylambOpt.ResearchGoals.ResourcePolicies.WindowRetention
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.ResourcePolicies.WindowRetention.Goal_PeerKeysBoundBufferedRows

open TinylambOpt.ResearchGoals.ResourcePolicies.WindowRetention


/-!
Distinct sort keys do not bound the number of buffered rows: RANGE CURRENT ROW
may buffer arbitrarily many tied rows for one key. The counterexample has two
rows with one key but only one key in the deduplicated list.
-/

theorem refute : ¬ TinylambOpt.ResearchGoals.ResourcePolicies.WindowRetention.PeerKeysBoundBufferedRows.Goal := by
  intro h
  have hle := h [0, 0] 0
  simp at hle
  have hlen : ([0, 0] : List Nat).eraseDups.length = 1 := by decide
  omega
end TinylambOpt.ResearchProofs.ResourcePolicies.WindowRetention.Goal_PeerKeysBoundBufferedRows
