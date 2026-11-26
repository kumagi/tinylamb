import TinylambOpt.ResearchGoals.StorageExecution.FrameSelection

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_FrameSelection_PatchedOctet_EscapeCost

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.FrameSelection
open TinylambOpt.ResearchGoals.StorageExecution.FrameSelection.PatchedOctet

private theorem log2_div_128 (n : Nat) (hn : 128 ≤ n) :
    Nat.log2 n = Nat.log2 (n / 128) + 7 := by
  let q := n / 128
  have hq : q ≠ 0 := by dsimp [q]; omega
  have hlow : 2 ^ (Nat.log2 q + 7) ≤ n := by
    rw [Nat.pow_add]
    change 2 ^ Nat.log2 q * 2 ^ 7 ≤ n
    rw [show (2 : Nat) ^ 7 = 128 by decide]
    have hqle : 2 ^ Nat.log2 q ≤ q := Nat.log2_self_le hq
    have hmul : q * 128 ≤ n := by dsimp [q]; exact Nat.div_mul_le_self n 128
    exact Nat.le_trans (Nat.mul_le_mul_right 128 hqle) hmul
  have hloglow : Nat.log2 q + 7 ≤ Nat.log2 n :=
    (Nat.le_log2 (by omega : n ≠ 0)).mpr hlow
  have hhigh : n < 2 ^ (Nat.log2 q + 8) := by
    have hqbound : q < 2 ^ (Nat.log2 q + 1) := Nat.lt_log2_self
    have hq1 : q + 1 ≤ 2 ^ (Nat.log2 q + 1) := Nat.succ_le_of_lt hqbound
    have hnlt : n < 128 * (q + 1) := by
      dsimp [q]
      simpa [Nat.mul_comm] using Nat.lt_mul_div_succ n (by decide : 0 < 128)
    have hmul : 128 * (q + 1) ≤ 128 * 2 ^ (Nat.log2 q + 1) := Nat.mul_le_mul_left 128 hq1
    have hp : 128 * 2 ^ (Nat.log2 q + 1) = 2 ^ (Nat.log2 q + 8) := by
      calc
        128 * 2 ^ (Nat.log2 q + 1) = 2 ^ 7 * 2 ^ (Nat.log2 q + 1) := by
          rw [show (128 : Nat) = 2 ^ 7 by decide]
        _ = 2 ^ (7 + (Nat.log2 q + 1)) := (Nat.pow_add 2 7 (Nat.log2 q + 1)).symm
        _ = 2 ^ (Nat.log2 q + 8) := by congr 1; omega
    exact Nat.lt_of_lt_of_le hnlt (by simpa [hp] using hmul)
  have hloghigh : Nat.log2 n < Nat.log2 q + 8 :=
    (Nat.log2_lt (by omega : n ≠ 0)).mpr (by simpa [q] using hhigh)
  change Nat.log2 n = Nat.log2 q + 7
  omega

private theorem uleb_length (n : Nat) : (uleb n).length = ubytes n := by
  induction n using Nat.strongRecOn with
  | ind n ih =>
      by_cases hlt : n < 128
      · rw [uleb]
        simp [hlt]
        unfold ubytes width
        by_cases hn : n == 0
        · simp [hn]
        · simp [hn]
          have hn0 : n ≠ 0 := by simpa using hn
          have hloglt : Nat.log2 n < 7 :=
            (Nat.log2_lt hn0).mpr (by
              change n < 2 ^ 7
              rw [show (2 : Nat) ^ 7 = 128 by decide]
              exact hlt)
          omega
      · have hn128 : 128 ≤ n := by omega
        have hdivlt : n / 128 < n := by omega
        have hdivlen : (uleb (n / 128)).length = ubytes (n / 128) := ih (n / 128) hdivlt
        rw [uleb]
        simp [hlt, hdivlen]
        unfold ubytes width
        have hn0 : n ≠ 0 := by omega
        have hnq : n / 128 ≠ 0 := by omega
        simp [hn0, hnq]
        have hlogeq := log2_div_128 n hn128
        rw [hlogeq]
        omega

theorem sound_EscapeCost : EscapeCost.Goal := by
  intro xs
  induction xs with
  | nil => simp [write]
  | cons x xs ih =>
      change ((if x < 255 then [x] else 255 :: uleb (x - 255)) ++ write xs).length =
        (x :: xs).length + (((x :: xs).filter (fun v => v ≥ 255)).map (fun v => ubytes (v - 255))).sum
      by_cases hx : x < 255
      · simp [hx, Nat.not_le.mpr hx, ih] <;> omega
      · have hxge : x ≥ 255 := by omega
        simp [hx, hxge, ih, uleb_length] <;> omega

end TinylambOpt.ResearchProofs.StorageExecution.Goal_FrameSelection_PatchedOctet_EscapeCost
