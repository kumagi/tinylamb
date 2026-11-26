import TinylambOpt.ResearchGoals.JitKernels.InListLowering

/-!
If the hot partition of the IN list accepts a probe, the unpartitioned list also
accepts it: the hot partition is a sublist, and acceptance of a non-NULL probe
is membership. A NULL probe is never accepted by either list. This licenses
short-circuiting the cold residual scan after a hot hit without changing the
WHERE result.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.InListLowering.Goal_SkipColdResidualOnHotHit

open TinylambOpt.ResearchGoals.JitKernels.InListLowering

theorem test_some (xs : Keys) (x : Int) :
    test xs (some x) =
      if some x ∈ xs then some true else if none ∈ xs then none else some false := by
  unfold test
  by_cases h : xs = []
  · simp [h]
  · simp [h]

theorem sound : SkipColdResidualOnHotHit.Goal := by
  intro xs lo hi v h
  cases v with
  | none =>
      unfold keep test at h
      simp at h
  | some x =>
      have hx : some x ∈ rangeKeys xs lo hi := by
        unfold keep at h
        rw [test_some] at h
        by_cases hm : some x ∈ rangeKeys xs lo hi
        · exact hm
        · simp [hm] at h
      have hx' : some x ∈ xs := by
        have hx2 : some x ∈ xs ∧ lo ≤ x ∧ x < hi := by simpa [rangeKeys] using hx
        exact hx2.1
      rw [test_some]
      simp [hx']

end TinylambOpt.ResearchProofs.JitKernels.InListLowering.Goal_SkipColdResidualOnHotHit
