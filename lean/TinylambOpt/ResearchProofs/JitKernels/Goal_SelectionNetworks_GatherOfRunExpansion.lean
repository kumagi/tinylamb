import TinylambOpt.ResearchGoals.JitKernels.SelectionNetworks

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_SelectionNetworks_GatherOfRunExpansion

open TinylambOpt.ResearchGoals.JitKernels.SelectionNetworks

theorem sound : TinylambOpt.ResearchGoals.JitKernels.SelectionNetworks.GatherOfRunExpansion.Goal := by
  intro xs runs
  induction runs with
  | nil => rfl
  | cons run runs ih =>
      rcases run with ⟨i, n⟩
      have hrep : ∀ m, List.filterMap (fun j => xs[j]?) (List.replicate m i) =
          List.flatMap (List.replicate m) xs[i]?.toList := by
        intro m
        induction m with
        | zero => simp
        | succ m ihm =>
            cases hi : xs[i]? with
            | none => simp [hi, ihm]
            | some v => simp [hi, ihm]
      simpa [gather, hrep] using congrArg (fun ys => List.filterMap (fun j => xs[j]?) (List.replicate n i) ++ ys) ih

end TinylambOpt.ResearchProofs.JitKernels.Goal_SelectionNetworks_GatherOfRunExpansion
