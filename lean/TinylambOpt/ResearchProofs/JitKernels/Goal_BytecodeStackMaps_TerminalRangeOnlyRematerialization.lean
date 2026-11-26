import TinylambOpt.ResearchGoals.JitKernels.BytecodeStackMaps

/-
Refutes `TerminalRangeOnlyRematerialization`: a final value inside the target
width does not certify checked intermediates. The same width-2 witness has an
overflowing intermediate and therefore no successful checked result.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.BytecodeStackMaps.Goal_TerminalRangeOnlyRematerialization

open TinylambOpt.ResearchGoals.JitKernels.BytecodeStackMaps

theorem refute : ¬ TerminalRangeOnlyRematerialization.Goal := by
  intro h
  let r : Recipe := .plus (.plus (.lit 3) (.lit 1)) (.neg (.lit 4))
  have hw : widthOK 2 (eval (fun _ => 0) r) = true := by native_decide
  have hc := h 2 (fun _ => 0) r hw
  have hn : checked 2 (fun _ => 0) r = none := by native_decide
  rw [hn] at hc
  simp at hc

end TinylambOpt.ResearchProofs.JitKernels.BytecodeStackMaps.Goal_TerminalRangeOnlyRematerialization
