import TinylambOpt.ResearchGoals.JitKernels.ControlByteProbe

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

/-!
# ProbeGroupSpeculativeReadBudget (refuted)

Loading every tag-matching payload in the whole control group can read more
lanes than the terminating scalar scan: the SIMD width reads the *whole*
group, while the scalar probe stops at the first empty lane.  Counterexample:
`xs = [.empty, .live 0 5 1]` with constant tag function `f = fun _ => 0` and
query key `3`: the full-group tag filter has one matching lane but the
terminating window `untilEmpty xs = []` has none, so `1 ≤ 0` fails. -/

namespace TinylambOpt.ResearchProofs.JitKernels.ControlByteProbe_ProbeGroupSpeculativeReadBudget
open TinylambOpt.ResearchGoals.JitKernels.ControlByteProbe

private def xs : List Slot := [.empty, .live 0 5 1]
private def f : Nat → Nat := fun _ => 0

private theorem coh : coherent f [.empty, .live 0 5 1] := by
  intro t k v hm
  rw [List.mem_cons] at hm
  cases hm with
  | inl he => cases he
  | inr hm2 =>
    rw [List.mem_singleton] at hm2
    cases hm2
    rfl

theorem refute : ¬ ProbeGroupSpeculativeReadBudget.Goal := by
  intro h
  have hle := h xs f 3 coh
  rw [show ((xs.filter (tagHit (f 3))).length) = 1 from by decide,
      show ((untilEmpty xs).filter (tagHit (f 3))).length = 0 from by decide] at hle
  omega
