import TinylambOpt.ResearchGoals.ResourcePolicies.VersionVacuum
/-!
Registers the checked result for `VersionVacuum.ShardWatermarkExactConflict.Goal`.

The theorem below either proves the stated claim under its assumptions or
refutes it with an explicit counterexample; no `sorry` or new axioms are used.
-/
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_VersionVacuum_ShardWatermarkExactConflict
open TinylambOpt.ResearchGoals.ResourcePolicies.VersionVacuum
theorem refute : ¬ ShardWatermarkExactConflict.Goal := by
  intro h
  let evicted : List (Nat × Nat) := [(0, 10)]
  have h' := h evicted 5 1 (by decide)
  rcases h' with ⟨t, ht, hlt⟩
  simp [evicted] at ht
end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_VersionVacuum_ShardWatermarkExactConflict
