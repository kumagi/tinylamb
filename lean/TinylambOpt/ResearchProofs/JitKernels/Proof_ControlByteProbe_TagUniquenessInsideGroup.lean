import TinylambOpt.ResearchGoals.JitKernels.ControlByteProbe

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

/-!
# TagUniquenessInsideGroup (refuted)

A single fingerprint hit does not pin down the key: the fingerprint is a lossy
function of the key, so the unique tag-matching lane can hold a different key
that merely collides in the fingerprint.  Counterexample: `xs = [.live 0 5 7]`
is coherent for the constant tag function `f = fun _ => 0`; for query key `9`
the masked window has exactly one lane, yet `probe xs 9 = none` because the
full key `5 ≠ 9`. -/

namespace TinylambOpt.ResearchProofs.JitKernels.ControlByteProbe_TagUniquenessInsideGroup
open TinylambOpt.ResearchGoals.JitKernels.ControlByteProbe

private def f : Nat → Nat := fun _ => 0

private theorem coh : coherent f [.live 0 5 7] := by
  intro t k v hm
  cases List.mem_singleton.mp hm
  rfl

theorem refute : ¬ TagUniquenessInsideGroup.Goal := by
  intro h
  have hlen : ((untilEmpty [.live 0 5 7]).filter (tagHit (f 9))).length = 1 := by decide
  have hprobe : (probe [.live 0 5 7] 9).isSome = false := by decide
  have hcon := h [.live 0 5 7] 9 f coh hlen
  rw [hprobe] at hcon
  exact absurd hcon (by decide)
