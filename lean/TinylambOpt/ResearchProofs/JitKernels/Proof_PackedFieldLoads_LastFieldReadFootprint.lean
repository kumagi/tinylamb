import TinylambOpt.ResearchGoals.JitKernels.PackedFieldLoads

/-!
**Refuted: a valid lane index does not license an 8-byte speculative read.**
For `w = 1`, `xs = [0]`, `i = 0` the single row packs into `byteCount 1 = 1`
byte, so `within (pack 1 [0]) 1 0 8` demands `0 * 1 / 8 + 8 ≤ 1`, which is
false. `LastFieldReadFootprint.Goal` is therefore unprovable: a logical lane
guaranteeing decoded correctness says nothing about a fixed 8-byte physical
footprint; padding or a clamped load is a separate obligation.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.JitKernels.PackedFieldLoads_LastFieldReadFootprint
open TinylambOpt.ResearchGoals.JitKernels.PackedFieldLoads
open TinylambOpt.ResearchGoals.Compression.Wire

theorem refute : ¬ LastFieldReadFootprint.Goal := by
  intro h
  have hh := h 1 [0] 0 (by decide) (by decide) (by decide)
  unfold within at hh
  rw [show (pack 1 [0]).length = 1 from by decide] at hh
  omega

end TinylambOpt.ResearchProofs.JitKernels.PackedFieldLoads_LastFieldReadFootprint
