import TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_DeltaBytes_RepeatedConstant

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters
open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.DeltaBytes

private def probe : List Bool :=
  [0, 1, 2, 3, 5, 9].map (fun n =>
    agg.reference (7 :: List.replicate n 7) == some (7, 7, n))

#eval probe

private theorem sbytes_zero : sbytes ((0 : Int)) = 1 := by
  native_decide

private theorem sbytes_self (x : Nat) : sbytes ((x : Int) - x) = 1 := by
  have : (x : Int) - x = 0 := by omega
  rw [this, sbytes_zero]

private theorem sum_replicate_one (n : Nat) : (List.replicate n 1).sum = n := by
  induction n with
  | zero => simp
  | succ n ih =>
      simp [List.replicate_succ, ih]
      omega

private theorem edges_replicate (x n : Nat) :
    edges (x :: List.replicate n x) = List.replicate n (x, x) := by
  simp [edges, ← List.replicate_succ, List.zip_replicate]

private theorem getLast?_cons_replicate (x n : Nat) :
    (x :: List.replicate n x).getLast? = some x := by
  by_cases h : n = 0
  · simp [h]
  · simp [List.getLast?_cons, List.getLast?_replicate, h]

theorem sound_RepeatedConstant : DeltaBytes.RepeatedConstant.Goal := by
  intro x n
  simp only [agg, List.head?_cons, Option.map_some]
  rw [getLast?_cons_replicate]
  simp only [Option.getD_some]
  rw [edges_replicate]
  simp only [List.map_replicate]
  rw [sbytes_self, sum_replicate_one]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_DeltaBytes_RepeatedConstant
