import TinylambOpt.ResearchGoals.JitKernels.SampledEquality

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

/-!
Refutation of "order unequal long strings by their trailing sample windows when the first
window agrees". At length 14 the offsets are `[0, 5, 10]`, so byte 4 — the first byte that
separates the two keys below — is never sampled, while the middle and tail windows do
order the keys in the opposite direction. Take
`a = [0×10, 9, 0,0,0]` and `b = [0,0,0,0,1,0×4,5,0,0,0]`: they have equal length and equal
first windows `[0,0,0,0]`, their signatures differ, `a < b` holds (first difference at
index 4: `0 < 1`), yet the concatenated middle-plus-tail windows of `a` end with `[9,...]`
against `[5,...]` for `b`, so the right-hand side is false. Sorting by raw sample tuples
is not a lexicographic refinement.
-/
namespace TinylambOpt.ResearchProofs.JitKernels.SampledEquality_PrefixFirstWindowOrder
open TinylambOpt.ResearchGoals.JitKernels.SampledEquality

private def A : Bytes := [0,0,0,0,0,0,0,0,0,0,9,0,0,0]
private def B : Bytes := [0,0,0,0,1,0,0,0,0,0,5,0,0,0]

theorem refute : ¬ PrefixFirstWindowOrder.Goal := by
  intro h
  have H := h A B rfl rfl (by decide : sig A ≠ sig B)
  exact absurd H (by decide)

end TinylambOpt.ResearchProofs.JitKernels.SampledEquality_PrefixFirstWindowOrder
