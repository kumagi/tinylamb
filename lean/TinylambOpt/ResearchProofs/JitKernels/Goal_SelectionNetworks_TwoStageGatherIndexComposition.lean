import TinylambOpt.ResearchGoals.JitKernels.SelectionNetworks

/-! When every first-stage ordinal is in range, composing the two selection vectors is equivalent to composing the two gathers. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_SelectionNetworks_TwoStageGatherIndexComposition

open TinylambOpt.ResearchGoals.JitKernels.SelectionNetworks
open TinylambOpt.ResearchGoals.JitKernels.SelectionNetworks.TwoStageGatherIndexComposition

theorem getElem_gather_valid (xs a : List Nat) (hvalid : ∀ i ∈ a, i < xs.length) :
    ∀ (j : Nat), (gather xs a)[j]? = (a[j]?).bind (fun i => xs[i]?) := by
  induction a with
  | nil => intro j; simp [gather]
  | cons i a ih =>
      have hi_lt : i < xs.length := hvalid i (by simp)
      have hi : xs[i]? = some (xs[i]'hi_lt) := List.getElem?_eq_getElem hi_lt
      have htail : ∀ j ∈ a, j < xs.length := by
        intro j hj
        exact hvalid j (by simp [hj])
      intro j
      cases j with
      | zero => simp [gather, hi]
      | succ j =>
          simpa [gather, hi] using ih htail j

theorem sound : TwoStageGatherIndexComposition.Goal := by
  intro xs a b hvalid
  have hget := getElem_gather_valid xs a hvalid
  have hget' : ∀ (j : Nat), (List.filterMap (fun i => xs[i]?) a)[j]? = (a[j]?).bind (fun i => xs[i]?) := by
    intro j
    simpa [gather] using hget j
  apply List.ext_getElem?
  intro j
  simp [gather, List.filterMap_filterMap, hget']

end TinylambOpt.ResearchProofs.JitKernels.Goal_SelectionNetworks_TwoStageGatherIndexComposition
