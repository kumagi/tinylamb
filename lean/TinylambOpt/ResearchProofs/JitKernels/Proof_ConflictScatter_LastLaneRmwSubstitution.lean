import TinylambOpt.ResearchGoals.JitKernels.ConflictScatter

/-!
Substituting a last-writer-wins scatter for the sum of colliding lane deltas is
unsound: with two lanes of the same slot, `(0,1) :: (0,2) :: []` and slot `0`,
the RMW keeps only the final delta `2` while the sequential result adds `1 + 2`.
Hence `LastLaneRmwSubstitution.Goal` fails and must not be used to justify
dropping accumulation for colliding nonzero counters.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.JitKernels.ConflictScatter_LastLaneRmwSubstitution
open TinylambOpt.ResearchGoals.JitKernels.ConflictScatter

theorem refute : ¬ LastLaneRmwSubstitution.Goal := by
  intro h
  have hc : (0 : Nat) < (([(0, 1), (0, 2)] : Updates).map Prod.fst).count 0 := by decide
  have h1 : lastLane ([(0, 1), (0, 2)] : Updates) 0 = 2 := by decide
  have h2 : delta ([(0, 1), (0, 2)] : Updates) 0 = 3 := by decide
  have := h ([(0, 1), (0, 2)] : Updates) (fun _ => 0) 0 hc
  unfold sequential at this
  rw [h1, h2] at this
  simp at this

end TinylambOpt.ResearchProofs.JitKernels.ConflictScatter_LastLaneRmwSubstitution
