import TinylambOpt.ResearchGoals.JitKernels.CodeReachability

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

/-!
# StubOnlySpaceBudget (refuted)

Claim: when every compiled body embeds at most one direct call, the bytes kept
by a mark-sweep are bounded by the root bodies' bytes plus one maximum-size
callee charged per root.

Refutation: a single root can transitively keep a long chain of one-call
bodies.  Counterexample: the heap is the chain `(0,0) -> (1,0) -> (2,0)` with
byte sizes `0`, `10`, `10` and the sole root `(0,0)`.  Every object has at most
one call target and the keys are duplicate-free, yet the sweep keeps all three
objects (`20` bytes) while the budget is `0 + 1 * 10 = 10` bytes. -/

namespace TinylambOpt.ResearchProofs.JitKernels.CodeReachability_StubOnlySpaceBudget
open TinylambOpt.ResearchGoals.JitKernels.CodeReachability

private def heap : List Obj :=
  [⟨(0,0), [(1,0)], 0⟩, ⟨(1,0), [(2,0)], 10⟩, ⟨(2,0), [], 10⟩]
private def roots : List Key := [(0,0)]

theorem refute : ¬StubOnlySpaceBudget.Goal := by
  intro h
  exact absurd (h heap roots (by decide) (by decide)) (by decide)

end TinylambOpt.ResearchProofs.JitKernels.CodeReachability_StubOnlySpaceBudget
