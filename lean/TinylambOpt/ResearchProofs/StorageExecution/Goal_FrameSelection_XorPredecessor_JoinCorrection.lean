import TinylambOpt.ResearchGoals.StorageExecution.FrameSelection
import TinylambOpt.ResearchProofs.Compression.Wire

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_FrameSelection_XorPredecessor_JoinCorrection

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.FrameSelection
open TinylambOpt.ResearchGoals.StorageExecution.FrameSelection.XorPredecessor

private theorem uleb_len_eq_ubytes (n : Nat) : (uleb n).length = ubytes n :=
  TinylambOpt.ResearchProofs.Compression.Wire.sound_VarintCharge n

private theorem zip_append_pred (a b : List Nat) (p : Nat) :
    (a ++ b).zip (p :: (a ++ b)) =
      (a.zip (p :: a)) ++ (b.zip (a.getLast?.getD p :: b)) := by
  induction a generalizing p with
  | nil => simp
  | cons x xs ih =>
      simp [List.getLast?_cons, ih]

private theorem write_length (xs : List Nat) :
    (write xs).length = ((xs.zip (0 :: xs)).map (fun q => ubytes (q.1 ^^^ q.2))).sum := by
  simp [write, List.length_flatMap, uleb_len_eq_ubytes]

theorem sound_JoinCorrection : JoinCorrection.Goal := by
  intro a b hb
  dsimp
  cases b with
  | nil => simp at hb
  | cons x bs =>
      rw [write_length (a ++ x :: bs), write_length a, write_length (x :: bs)]
      have hzip : ((a ++ x :: bs).zip (0 :: (a ++ x :: bs))) =
          (a.zip (0 :: a)) ++ ((x :: bs).zip (a.getLast?.getD 0 :: x :: bs)) := by
        simpa using (zip_append_pred a (x :: bs) 0)
      rw [hzip]
      simp [List.map_append, List.sum_append, List.zip_cons_cons, Nat.xor_zero]
      rw [Nat.xor_comm x (a.getLast?.getD 0)]
      omega

end TinylambOpt.ResearchProofs.StorageExecution.Goal_FrameSelection_XorPredecessor_JoinCorrection
