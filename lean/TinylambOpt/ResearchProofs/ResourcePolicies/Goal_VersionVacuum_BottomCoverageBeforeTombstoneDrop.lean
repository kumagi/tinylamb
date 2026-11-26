import TinylambOpt.ResearchGoals.ResourcePolicies.VersionVacuum
/-!
Registers the checked result for `VersionVacuum.BottomCoverageBeforeTombstoneDrop.Goal`.

The theorem below either proves the stated claim under its assumptions or
refutes it with an explicit counterexample; no `sorry` or new axioms are used.
-/
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_VersionVacuum_BottomCoverageBeforeTombstoneDrop
open TinylambOpt.ResearchGoals.ResourcePolicies.VersionVacuum
theorem refute : ¬ BottomCoverageBeforeTombstoneDrop.Goal := by
  intro h
  let upper : List Version := [⟨5, none⟩]
  let lower : List Version := [⟨2, some 9⟩]
  have h' := h upper lower 5 (by simp [ordered, upper, lower]) (by decide)
  simp [readAt, winner, upper, lower] at h'
end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_VersionVacuum_BottomCoverageBeforeTombstoneDrop
