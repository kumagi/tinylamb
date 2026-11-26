import TinylambOpt.ResearchGoals.StorageExecution.RankedMasks

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_RankedMasks_PrefixOnes_CutOrdinal

open TinylambOpt.ResearchGoals.StorageExecution.RankedMasks
open TinylambOpt.ResearchGoals.StorageExecution.RankedMasks.PrefixOnes

-- Truth check on small inputs (k ≤ n).
private def check : Bool :=
  (List.range 10).all fun n =>
  (List.range (n + 1)).all fun k =>
    decide (rank model 0 (List.replicate k true ++ List.replicate (n - k) false) = some k)

#eval check

private theorem step_false (q : Nat) : model.step q false = some 1 := by
  simp [model]

private theorem step_true_zero : model.step 0 true = some 0 := by
  simp [model]

private theorem step_true_one : model.step 1 true = none := by
  simp [model]

private theorem count_one : ∀ n : Nat, count model n 1 = 1 := by
  intro n
  induction n with
  | zero => simp [count, model]
  | succ n ih => simp [count, step_false, step_true_one, ih]

private theorem lowCount_one (n q : Nat) : lowCount model n q = 1 := by
  simp [lowCount, step_false, count_one]

-- A run of false bits has rank zero from any state: `false` never contributes
-- an interval shift, and every state accepts.
private theorem rank_all_false : ∀ (n q : Nat),
    rank model q (List.replicate n false) = some 0 := by
  intro n
  induction n with
  | zero => intro q; simp [rank, model]
  | succ n ih =>
      intro q
      change rank model q (false :: List.replicate n false) = some 0
      simp [rank, step_false, ih]

theorem sound_CutOrdinal : PrefixOnes.CutOrdinal.Goal := by
  intro n k hk
  induction k generalizing n with
  | zero =>
      simpa using rank_all_false n 0
  | succ k ih =>
      have hkn : k ≤ n - 1 := by omega
      have hsplit : List.replicate (k + 1) true ++ List.replicate (n - (k + 1)) false =
          true :: (List.replicate k true ++ List.replicate (n - 1 - k) false) := by
        simp [List.replicate_succ, Nat.sub_sub, Nat.add_comm]
      rw [hsplit]
      simp [rank, step_true_zero, lowCount_one, ih (n - 1) hkn]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_RankedMasks_PrefixOnes_CutOrdinal
