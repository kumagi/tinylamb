import TinylambOpt.ResearchGoals.JitKernels.BytecodeStackMaps

/-
Refutes `SignedZeroCostDoubleNegation`: removing paired negations can hide a
width violation at the intermediate negation. For width 2 and value -4, the
double negation faults while the original recipe succeeds.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.BytecodeStackMaps.Goal_SignedZeroCostDoubleNegation

open TinylambOpt.ResearchGoals.JitKernels.BytecodeStackMaps

theorem refute : ¬ SignedZeroCostDoubleNegation.Goal := by
  intro h
  let r : Recipe := .lit (-4)
  have hc : checked 2 (fun _ => 0) r = some (eval (fun _ => 0) r) := by native_decide
  have hw : widthOK 2 (eval (fun _ => 0) r) = true := by native_decide
  have hbad := h 2 (fun _ => 0) r hc hw
  have hn : checked 2 (fun _ => 0) (.neg (.neg r)) = none := by native_decide
  rw [hn] at hbad
  have hs : checked 2 (fun _ => 0) r = some (-4 : Int) := by native_decide
  rw [hs] at hbad
  contradiction

end TinylambOpt.ResearchProofs.JitKernels.BytecodeStackMaps.Goal_SignedZeroCostDoubleNegation
