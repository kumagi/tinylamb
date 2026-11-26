import TinylambOpt.ResearchGoals.JitKernels.ControlByteProbe

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

/-!
# DeletedSlotAsProbeFence (refuted)

A tombstone inserted after a miss does not fence the linear probe: the scan
window `takeWhile (· != .empty)` skips tombstones, so a key stored after the
tombstone is still reachable.  Counterexample: `a = []` misses key `9`, but
`[] ++ [.tomb] ++ [.live 1 9 7]` probes to `some 7`. -/

namespace TinylambOpt.ResearchProofs.JitKernels.ControlByteProbe_DeletedSlotAsProbeFence
open TinylambOpt.ResearchGoals.JitKernels.ControlByteProbe

theorem refute : ¬ DeletedSlotAsProbeFence.Goal := by
  intro h
  have h0 : probe ([] : List Slot) 9 = none := by decide
  have h1 : probe (([] : List Slot) ++ [.tomb] ++ [.live 1 9 7]) 9 = some 7 := by decide
  have hcon := h [] [.live 1 9 7] 9 h0
  rw [h1] at hcon
  exact absurd hcon (by simp)
