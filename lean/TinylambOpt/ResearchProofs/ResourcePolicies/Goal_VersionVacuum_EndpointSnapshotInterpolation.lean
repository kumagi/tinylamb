import TinylambOpt.ResearchGoals.ResourcePolicies.VersionVacuum
/-!
Registers the checked result for `VersionVacuum.EndpointSnapshotInterpolation.Goal`.

The theorem below either proves the stated claim under its assumptions or
refutes it with an explicit counterexample; no `sorry` or new axioms are used.
-/
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_VersionVacuum_EndpointSnapshotInterpolation
open TinylambOpt.ResearchGoals.ResourcePolicies.VersionVacuum
theorem refute : ¬ EndpointSnapshotInterpolation.Goal := by
  intro h
  let vs : List Version := [⟨5, some 1⟩, ⟨3, some 2⟩, ⟨1, some 3⟩]
  have h' := h vs 1 5 3 (by simp [ordered, vs]) (by decide) (by decide)
  simp [readAt, sparse, winner, vs] at h'
end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_VersionVacuum_EndpointSnapshotInterpolation
