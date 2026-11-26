import TinylambOpt.ResearchGoals.ResourcePolicies.SpillExtents
/-!
Registers the checked result for `SpillExtents.ExtentGenerationReadEquivalence.Goal`.

The theorem below either proves the stated claim under its assumptions or
refutes it with an explicit counterexample; no `sorry` or new axioms are used.
-/
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_SpillExtents_ExtentGenerationReadEquivalence
open TinylambOpt.ResearchGoals.ResourcePolicies.SpillExtents ExtentGenerationReadEquivalence

theorem sound : Goal := by
  intro directory old new relocate start len h
  have hmap : (directory.map relocate).flatMap new = directory.flatMap old := by
    rw [List.flatMap_map]
    induction directory with
    | nil => simp
    | cons id ids ih =>
      simp only [List.flatMap_cons]
      rw [h id (by simp), ih (fun i hi => h i (by simp [hi]))]
  rw [hmap]
end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_SpillExtents_ExtentGenerationReadEquivalence
