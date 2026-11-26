import TinylambOpt.ResearchGoals.StorageExecution.FrameSelection

/-!
A scan can skip or partially decode compressed frames while preserving selected values
and their positions.

Proved

- `SeparatedLengths.ControlBytes` — `∀ xs, admissible xs → (write xs).length =
  byteCount (2*xs.length) + (sizes xs).sum`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_FrameSelection_SeparatedLengths_ControlBytes

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution
open TinylambOpt.ResearchGoals.StorageExecution.FrameSelection
open TinylambOpt.ResearchGoals.StorageExecution.FrameSelection.SeparatedLengths

private theorem little_len (n k : Nat) : (little n k).length = k := by
  simp [little]

private theorem pack_len (w : Nat) (ys : List Nat) :
    (pack w ys).length = byteCount (ys.length * w) := by
  simp [pack, little_len]

private theorem data_len_general (xs ws : List Nat) (h : ws.length = xs.length) :
    ((xs.zip ws).flatMap (fun (v, w) => little v w)).length = ws.sum := by
  induction xs generalizing ws with
  | nil =>
      simp at h
      subst h
      simp
  | cons x xs ih =>
      cases ws with
      | nil => simp at h
      | cons w ws =>
          simp at h
          simp [little_len, ih ws h]

private theorem data_len (xs : List Nat) :
    ((xs.zip (sizes xs)).flatMap (fun (v, w) => little v w)).length = (sizes xs).sum := by
  apply data_len_general
  simp [sizes]

theorem sound_ControlBytes : SeparatedLengths.ControlBytes.Goal := by
  intro xs hadm
  rw [write, List.length_append, pack_len, data_len]
  simp [sizes, Nat.mul_comm]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_FrameSelection_SeparatedLengths_ControlBytes
