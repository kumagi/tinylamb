import TinylambOpt.ResearchGoals.StorageExecution.FrameSelection

/-!
A scan can skip or partially decode compressed frames while preserving selected values
and their positions.

Proved

- `SeparatedLengths.FourControls` — `∀ a b, a.length%4=0 → admissible (a++b) → (write
  (a++b)).length = (write a).length+(write b).length`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_FrameSelection_SeparatedLengths_FourControls

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.FrameSelection
open TinylambOpt.ResearchGoals.StorageExecution.FrameSelection.SeparatedLengths

-- 小さい入力での真偽確認（全部で 513*85 Case、反例 0 件）。
private def vals4 : List Nat := [0, 1, 255, 16777216]
private def vals2 : List Nat := [0, 256]

private def gen (vals : List Nat) : Nat → List (List Nat)
  | 0 => [[]]
  | n + 1 => (gen vals n).flatMap (fun xs => vals.map (fun v => v :: xs))

#eval let as := gen vals4 0 ++ gen vals4 4 ++ gen vals2 8
      let bs := gen vals4 0 ++ gen vals4 1 ++ gen vals4 2 ++ gen vals4 3
      ((as.flatMap (fun a => bs.map (fun b => (a, b)))).filter (fun (a, b) =>
        decide (a.length % 4 == 0) && (a ++ b).all (fun x => x < 2^32) &&
        decide ((write (a ++ b)).length == (write a).length + (write b).length) == false)).length

private theorem zip_snd_sum (f : Nat → Nat) (xs : List Nat) :
    ((xs.zip (xs.map f)).map (fun p => p.snd)).sum = (xs.map f).sum := by
  induction xs with
  | nil => simp
  | cons x xs ih => simp [ih]

private theorem write_len (xs : List Nat) :
    (write xs).length = byteCount (2 * xs.length) + (sizes xs).sum := by
  simp [write, pack, little, sizes, zip_snd_sum, Nat.mul_comm]

private theorem byteCount_add_mul8 (k m : Nat) : byteCount (8 * k + m) = k + byteCount m := by
  unfold byteCount
  omega

private theorem byteCount_double_append (n m : Nat) (h : n % 4 = 0) :
    byteCount (2 * (n + m)) = byteCount (2 * n) + byteCount (2 * m) := by
  have hn : n = 4 * (n / 4) := by omega
  rw [hn]
  rw [show 2 * (4 * (n / 4) + m) = 8 * (n / 4) + 2 * m by omega]
  rw [byteCount_add_mul8]
  rw [show 2 * (4 * (n / 4)) = 8 * (n / 4) + 0 by omega]
  rw [byteCount_add_mul8]
  simp [byteCount]

theorem sound_FourControls : SeparatedLengths.FourControls.Goal := by
  intro a b hmod hadm
  rw [write_len (a ++ b), write_len a, write_len b]
  rw [List.length_append]
  rw [byteCount_double_append a.length b.length hmod]
  simp only [sizes, List.map_append, List.sum_append]
  omega

end TinylambOpt.ResearchProofs.StorageExecution.Goal_FrameSelection_SeparatedLengths_FourControls
