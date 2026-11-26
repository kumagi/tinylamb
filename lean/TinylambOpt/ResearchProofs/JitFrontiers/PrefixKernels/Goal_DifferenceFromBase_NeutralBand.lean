import TinylambOpt.ResearchGoals.JitFrontiers.PrefixKernels
import TinylambOpt.ResearchProofs.JitFrontiers.PrefixKernels.Common_PrefixKernels

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.JitFrontiers.PrefixKernels.Goal_DifferenceFromBase_NeutralBand

open TinylambOpt.ResearchProofs.JitFrontiers.PrefixKernels
open TinylambOpt.ResearchGoals.JitFrontiers.PrefixKernels
open TinylambOpt.ResearchGoals.JitFrontiers.CheckedRewrites (fits limit)

theorem sound : DifferenceFromBase.NeutralBand.Goal := by
  intro rs n
  have hrep : ∀ n, whole DifferenceFromBase.update (List.replicate n (some 100)) = (1, 0) := by
    intro n
    induction n with
    | zero => rfl
    | succ n ih =>
        rw [List.replicate_succ]
        have hw := whole_cons DifferenceFromBase.update (some 100) (List.replicate n (some 100))
        rw [hw, ih]
        simp [DifferenceFromBase.update, compose]
  rw [whole_append, hrep n]
  rw [compose_id_right]


end TinylambOpt.ResearchProofs.JitFrontiers.PrefixKernels.Goal_DifferenceFromBase_NeutralBand
