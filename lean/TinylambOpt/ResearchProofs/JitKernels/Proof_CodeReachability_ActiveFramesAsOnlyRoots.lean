import TinylambOpt.ResearchGoals.JitKernels.CodeReachability

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

/-!
# ActiveFramesAsOnlyRoots (refuted)

Claim: a key that is not itself a root can never be marked live.  This lets an
evictor keep exactly the bodies whose active-frame count is nonzero, discarding
everything a live body directly calls.

Refutation: a live compiled body embeds direct calls to bodies that have no
active frame of their own (e.g. cold callees).  Counterexample: the heap holds
one object with key `(0,0)` calling `(1,0)`, and the root set is `[(0,0)]`.
Then `(1,0) ∉ roots`, yet `mark heap roots heap.length = [(0,0), (1,0)]`
contains `(1,0)`, so the goal's conclusion fails. -/

namespace TinylambOpt.ResearchProofs.JitKernels.CodeReachability_ActiveFramesAsOnlyRoots
open TinylambOpt.ResearchGoals.JitKernels.CodeReachability

private def heap : List Obj := [⟨(0,0), [(1,0)], 0⟩]
private def roots : List Key := [(0,0)]
private def k : Key := (1,0)

theorem refute : ¬ActiveFramesAsOnlyRoots.Goal := by
  intro h
  have hnot : k ∉ roots := by decide
  have hmem : k ∈ mark heap roots heap.length := by
    show k ∈ [(0,0), (1,0)]
    decide
  exact h heap roots k hnot hmem

end TinylambOpt.ResearchProofs.JitKernels.CodeReachability_ActiveFramesAsOnlyRoots
