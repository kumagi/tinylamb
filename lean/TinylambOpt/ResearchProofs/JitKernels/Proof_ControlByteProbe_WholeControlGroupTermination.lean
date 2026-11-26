import TinylambOpt.ResearchGoals.JitKernels.ControlByteProbe

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

/-!
# WholeControlGroupTermination (refuted)

`untilEmpty = takeWhile (· != .empty)` keeps the lanes before the first empty
slot, so a match positioned earlier than the empty byte is still found by the
probe.  Counterexample (the file's own guard): `[.tomb, .live 1 9 7, .empty]`
contains `.empty` yet probes key `9` to `some 7`. -/

namespace TinylambOpt.ResearchProofs.JitKernels.ControlByteProbe_WholeControlGroupTermination
open TinylambOpt.ResearchGoals.JitKernels.ControlByteProbe

private def xs : List Slot := [.tomb, .live 1 9 7, .empty]

theorem refute : ¬ WholeControlGroupTermination.Goal := by
  intro h
  have hmem : Slot.empty ∈ xs := by decide
  have hprobe : probe xs 9 = some 7 := by decide
  have hcon := h xs 9 hmem
  rw [hprobe] at hcon
  exact absurd hcon (by simp)
