import TinylambOpt.ResearchGoals.JitKernels.SelectionNetworks

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace Nov26.Proof_SelectionNetworks_TwoStageGatherIndexComposition
open TinylambOpt.ResearchGoals.JitKernels.SelectionNetworks
open TinylambOpt.ResearchGoals.JitKernels.SelectionNetworks.TwoStageGatherIndexComposition
open TinylambOpt.ResearchGoals.JitKernels.SelectionNetworks.TwoStageGatherIndexComposition
open TinylambOpt.ResearchGoals.JitKernels.SelectionNetworks.TwoStageGatherIndexComposition
open TinylambOpt.ResearchGoals.JitKernels.SelectionNetworks.TwoStageGatherIndexComposition

/--
Lane-wise view of a gathered vector: when every ordinal in `a` is in range for `xs`, lane `j` of
`gather xs a` is exactly `xs` looked up at `a[j]`. Out-of-range lanes would shift the outputs, so
the hypothesis is used at each recursive step.
-/
private theorem get?_gather (xs : List Nat) : ∀ (a : List Nat),
    (∀ i ∈ a, i < xs.length) →
    ∀ (j : Nat), (gather xs a)[j]? = (a[j]? : Option Nat) >>= fun i => xs[i]? := by
  intro a
  induction a with
  | nil => intro _ j; simp [gather]
  | cons x as ih =>
      intro hall j
      have hx : x < xs.length := hall x (by simp)
      have has : ∀ i ∈ as, i < xs.length := fun i m => hall i (by simp [m])
      have ho : xs[x]? = some (xs[x]) := List.getElem?_eq_getElem hx
      simp only [gather, List.filterMap_cons, ho]
      cases j with
      | zero =>
          simp [List.getElem?_cons_zero]
      | succ k =>
          rw [List.getElem?_cons_succ]
          show (gather xs as)[k]? = (as[k]? : Option Nat) >>= fun i => xs[i]?
          exact ih has k

/--
Fusing two gathers by composing their selection vectors: if every first-stage ordinal is in range
for `xs`, then gathering `xs` by `a` and then by `b` equals gathering `xs` by the composed vector
`gather a b`. Each fused lane satisfies `(gather xs a)[j]? = a[j]? >>= xs[·]?`, so both sides are
the same `filterMap` over `b`. This lets the JIT emit one gather kernel instead of two, with no
intermediate buffer allocation.
-/
theorem sound : Goal := by
  intro xs a b hall
  show b.filterMap (fun j => (gather xs a)[j]?)
      = (b.filterMap (fun j => (a[j]? : Option Nat))).filterMap (fun i => xs[i]?)
  rw [List.filterMap_filterMap]
  congr 1
  funext j
  exact get?_gather xs a hall j

end Nov26.Proof_SelectionNetworks_TwoStageGatherIndexComposition