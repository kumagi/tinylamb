import TinylambOpt.ResearchGoals.StorageExecution.FrameSelection

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_FrameSelection_SplitHigh_StrictSavings

open TinylambOpt.ResearchGoals.StorageExecution.FrameSelection
open TinylambOpt.ResearchGoals.StorageExecution.FrameSelection.SplitHigh

-- 小さい入力で実際の符号化・計算により真偽を確認する。
#guard (charge write 3 [[1,2,3],[256,257]] +
          ([[1,2,3],[256,257]].map (saved write)).sum =
        ([[1,2,3],[256,257]].map (fun p => (raw p).length)).sum + 3*2)
#guard (charge write 0 [[],[0],[1,1,1],[65536,0,255]] +
          ([[],[0],[1,1,1],[65536,0,255]].map (saved write)).sum =
        ([[],[0],[1,1,1],[65536,0,255]].map (fun p => (raw p).length)).sum)

private theorem min_add_sub_eq (a b : Nat) : min a b + (b - a) = b := by
  omega

private theorem chosen_add_saved (f : Writer) (xs : List Nat) :
    chosen f xs + saved f xs = (raw xs).length := by
  unfold chosen saved
  simpa using min_add_sub_eq (frame f xs).length (raw xs).length

private theorem charge_add_saved (f : Writer) (fee : Nat) (ps : List (List Nat)) :
    charge f fee ps + (ps.map (saved f)).sum =
      (ps.map (fun p => fee + chosen f p + saved f p)).sum := by
  induction ps with
  | nil => simp [charge]
  | cons p ps ih =>
      simp only [charge, List.map_cons, List.sum_cons] at ih ⊢
      omega

theorem sound_StrictSavings : SplitHigh.StrictSavings.Goal := by
  intro ps fee
  rw [charge_add_saved]
  have hpoint : ∀ p : List Nat, fee + chosen write p + saved write p = (raw p).length + fee := by
    intro p
    have hp := chosen_add_saved write p
    omega
  induction ps with
  | nil => simp
  | cons p ps ih =>
      simp only [List.map_cons, List.sum_cons, List.length_cons]
      rw [hpoint p, ih]
      simp [Nat.mul_succ, Nat.add_assoc, Nat.add_left_comm, Nat.add_comm]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_FrameSelection_SplitHigh_StrictSavings
