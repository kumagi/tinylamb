import TinylambOpt.ResearchGoals.JitKernels.ControlByteProbe

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

/-!
# AlignedProbeStartReuse (refuted)

Two keys whose home slots share a SIMD group do not share a probe result:
linear-probe termination depends on where the scan starts, and rotating the
ring can move an empty lane in front of a match that the other start would
have reached.  Counterexample: `xs = [.live 1 9 3, .empty, .live 1 5 7]`,
`w = 2`, homes `a = 0`, `b = 1` (same group, `0/2 = 1/2 = 0`): key `9` probes
to `some 3` from home `0` but to `none` from home `1`, where the ring starts
with the empty lane. -/

namespace TinylambOpt.ResearchProofs.JitKernels.ControlByteProbe_AlignedProbeStartReuse
open TinylambOpt.ResearchGoals.JitKernels.ControlByteProbe

private def xs : List Slot := [.live 1 9 3, .empty, .live 1 5 7]

theorem refute : ¬ AlignedProbeStartReuse.Goal := by
  intro h
  have h0 : probe (ring xs 0) 9 = some 3 := by decide
  have h1 : probe (ring xs 1) 9 = none := by decide
  have hcon := h xs 0 1 2 9 (by decide) (by decide) (by decide) (by decide)
  rw [h0, h1] at hcon
  exact absurd hcon (by simp)
