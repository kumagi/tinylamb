import TinylambOpt.ResearchGoals.StorageExecution.PostingJoins

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_PostingJoins_Annulus_TwoSlices

open TinylambOpt.ResearchGoals.StorageExecution.PostingJoins
open TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.Annulus

abbrev opt := TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.optional

private theorem opt_length_le_one (a b : Nat) : (opt a b).length ≤ 1 := by
  unfold opt TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.optional
  split <;> simp

theorem sound_TwoSlices : Annulus.TwoSlices.Goal := by
  intro u t x
  change (opt (x - (2 * t + 1)) (min u (x - t)) ++
          opt (x + t + 1) (min u (x + 2 * t + 2))).length ≤ 2
  rw [List.length_append]
  have h1 := opt_length_le_one (x - (2 * t + 1)) (min u (x - t))
  have h2 := opt_length_le_one (x + t + 1) (min u (x + 2 * t + 2))
  omega

end TinylambOpt.ResearchProofs.StorageExecution.Goal_PostingJoins_Annulus_TwoSlices
