import TinylambOpt.ResearchGoals.JitKernels.CodeReachability

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

/-!
# UnrootedRecursionRefcount (refuted)

Claim: with the full root set accounted for, a heap object is live exactly
when it is a root or appears as someone's call target — so the call-target
multiset alone could replace the tracing mark.

Refutation: a call target is only live if the caller itself is live.
Counterexample: the heap holds `c = (0,0)` calling `b = (1,0)` and the object
`b` itself, but the root set is empty.  Nothing is marked (`mark heap [] _ = []`),
yet `b` occurs in `heap.flatMap Obj.calls`.  Then `b.key ∈ mark ...` is false
while `b.key ∈ roots ∨ b.key ∈ heap.flatMap Obj.calls` is true, so the claimed
equivalence fails for `o = b`. -/

namespace TinylambOpt.ResearchProofs.JitKernels.CodeReachability_UnrootedRecursionRefcount
open TinylambOpt.ResearchGoals.JitKernels.CodeReachability

private def b : Key := (1,0)
private def ob : Obj := ⟨b, [], 0⟩
private def heap : List Obj := [⟨(0,0), [b], 0⟩, ob]

theorem refute : ¬UnrootedRecursionRefcount.Goal := by
  intro h
  exact absurd (h heap [] ob (by decide)) (by decide)

end TinylambOpt.ResearchProofs.JitKernels.CodeReachability_UnrootedRecursionRefcount
