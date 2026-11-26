import TinylambOpt.ResearchGoals.JitKernels.PackedFieldLoads

/-!
A valid logical lane does not license an eight-byte read inside the packed
allocation: a one-bit column with one row occupies a single byte. The
speculative read must therefore be guarded separately.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace Nov26.Proof_PackedFieldLoads_LastFieldReadFootprint
open TinylambOpt.ResearchGoals.JitKernels.PackedFieldLoads
open TinylambOpt.ResearchGoals.Compression.Wire

theorem refute : ¬ LastFieldReadFootprint.Goal := by
  intro h
  have heq := h 1 [0] 0 (by decide) (by decide) (by decide)
  exact (by unfold within; native_decide : ¬ within (pack 1 [0]) 1 0 8) heq

end Nov26.Proof_PackedFieldLoads_LastFieldReadFootprint
