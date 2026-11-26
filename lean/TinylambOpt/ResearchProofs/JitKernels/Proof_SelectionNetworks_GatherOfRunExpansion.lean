import TinylambOpt.ResearchGoals.JitKernels.SelectionNetworks

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace Nov26.Proof_SelectionNetworks_GatherOfRunExpansion
open TinylambOpt.ResearchGoals.JitKernels.SelectionNetworks
open TinylambOpt.ResearchGoals.JitKernels.SelectionNetworks.GatherOfRunExpansion
open TinylambOpt.ResearchGoals.JitKernels.SelectionNetworks.GatherOfRunExpansion
open TinylambOpt.ResearchGoals.JitKernels.SelectionNetworks.GatherOfRunExpansion
open TinylambOpt.ResearchGoals.JitKernels.SelectionNetworks.GatherOfRunExpansion

/-- Gathering a run of `k` copies of one ordinal broadcasts the referred value `k` times. -/
private theorem gather_replicate (xs : List Nat) (i : Nat) :
    ∀ k, List.filterMap (fun j => xs[j]?) (List.replicate k i)
      = (xs[i]? : Option Nat).toList.flatMap (List.replicate k) := by
  intro k
  induction k with
  | zero => simp
  | succ k ih =>
      show List.filterMap (fun j => xs[j]?) (i :: List.replicate k i)
          = (xs[i]? : Option Nat).toList.flatMap (List.replicate (k+1))
      rw [List.filterMap_cons, ih]
      cases h : xs[i]? with
      | none => simp [h]
      | some v => simp [List.replicate_succ, h]

/--
A gather over a run-expanded ordinal vector equals loading each referenced value once and
broadcasting it over its run length. This justifies the run-length gather kernel: the shared row
fetch is hoisted out of the replicated lanes, saving redundant loads for sorted or repeated row
references.
-/
theorem sound : Goal := by
  intro xs runs
  unfold gather
  induction runs with
  | nil => rfl
  | cons r t ih =>
      obtain ⟨i, k⟩ := r
      rw [List.flatMap_cons, List.flatMap_cons, List.filterMap_append, ih, gather_replicate]

end Nov26.Proof_SelectionNetworks_GatherOfRunExpansion