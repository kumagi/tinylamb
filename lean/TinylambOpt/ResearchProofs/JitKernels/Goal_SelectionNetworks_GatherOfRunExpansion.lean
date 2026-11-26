import TinylambOpt.ResearchGoals.JitKernels.SelectionNetworks

/-! A run of repeated row references can be loaded once and broadcast; invalid references contribute no lanes. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.SelectionNetworks.Goal_SelectionNetworks_GatherOfRunExpansion

open TinylambOpt.ResearchGoals.JitKernels.SelectionNetworks
open TinylambOpt.ResearchGoals.JitKernels.SelectionNetworks.GatherOfRunExpansion

theorem filterMap_replicate {α β : Type} (f : α → Option β) (i : α) (n : Nat) :
    (List.replicate n i).filterMap f = (f i).toList.flatMap (fun v => List.replicate n v) := by
  cases hfi : f i with
  | none =>
      induction n with
      | zero => simp
      | succ n ih => simp [List.replicate_succ, hfi, ih]
  | some v =>
      induction n with
      | zero => simp
      | succ n ih => simp [List.replicate_succ, hfi, ih]

theorem sound : GatherOfRunExpansion.Goal := by
  intro xs runs
  induction runs with
  | nil => simp [gather]
  | cons run runs ih =>
      rcases run with ⟨i, n⟩
      have ih' : (List.flatMap (fun x : Nat × Nat => List.replicate x.snd x.fst) runs).filterMap (fun j => xs[j]?) =
          runs.flatMap (fun x : Nat × Nat => xs[x.fst]?.toList.flatMap (List.replicate x.snd)) := by
        simpa [gather] using ih
      simp [gather, List.filterMap_append, ih', filterMap_replicate]

end TinylambOpt.ResearchProofs.JitKernels.SelectionNetworks.Goal_SelectionNetworks_GatherOfRunExpansion
