import TinylambOpt.ResearchGoals.JitKernels.DecimalChunks

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_DecimalChunks_DecimalBlockMultiplyAccumulate

open TinylambOpt.ResearchGoals.JitKernels.DecimalChunks

theorem sound : TinylambOpt.ResearchGoals.JitKernels.DecimalChunks.DecimalBlockMultiplyAccumulate.Goal := by
  have hfold : ∀ (s : List Nat) (acc : Nat),
      s.foldl (fun n c => 10 * n + (c - 48)) acc = acc * 10 ^ s.length + magnitude s := by
    intro s
    induction s with
    | nil => intro acc; simp [magnitude]
    | cons c s ih =>
        intro acc
        dsimp [magnitude]
        change s.foldl (fun n c => 10 * n + (c - 48)) (10 * acc + (c - 48)) =
          acc * 10 ^ (s.length + 1) + s.foldl (fun n c => 10 * n + (c - 48)) (0 + (c - 48))
        rw [ih (10 * acc + (c - 48)), ih (0 + (c - 48))]
        rw [Nat.add_mul, Nat.pow_succ]
        ac_rfl
  intro a b
  rw [show magnitude (a ++ b) = b.foldl (fun n c => 10 * n + (c - 48)) (magnitude a) by
    unfold magnitude
    rw [List.foldl_append]]
  rw [hfold b (magnitude a)]

end TinylambOpt.ResearchProofs.JitKernels.Goal_DecimalChunks_DecimalBlockMultiplyAccumulate
