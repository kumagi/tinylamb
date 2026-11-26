import TinylambOpt.ResearchGoals.StorageExecution.FrameSelection
import TinylambOpt.ResearchProofs.Compression.Wire

/-!
A scan can skip or partially decode compressed frames while preserving selected values
and their positions.

Proved

- `XorPredecessor.EqualTail` — `∀ x n, (write (x::List.replicate n x)).length = ubytes
  x+n`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_FrameSelection_XorPredecessor_EqualTail

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.FrameSelection
open TinylambOpt.ResearchGoals.StorageExecution.FrameSelection.XorPredecessor

-- Small-input sanity checks, including ULEB length boundaries.
#guard (write ([0] : List Nat)).length == ubytes 0
#guard (write ([0,0,0] : List Nat)).length == ubytes 0 + 2
#guard (write ([300,300,300,300] : List Nat)).length == ubytes 300 + 3
#guard (write ([2^40] ++ List.replicate 100 (2^40) : List Nat)).length == ubytes (2^40) + 100

private theorem uleb_len_eq_ubytes (n : Nat) : (uleb n).length = ubytes n :=
  TinylambOpt.ResearchProofs.Compression.Wire.sound_VarintCharge n

private theorem ubytes_zero : ubytes 0 = 1 := by
  simp [ubytes, width]

-- After the first lane, every predecessor equals the current value, so each
-- residual is zero and costs exactly one byte.
private theorem tail_sum (x : Nat) : ∀ n : Nat,
    (List.map (fun p : Nat × Nat => ubytes (p.1 ^^^ p.2))
      ((List.replicate n x).zip (x :: List.replicate n x))).sum = n
  | 0 => by simp
  | n + 1 => by
      simp [List.replicate, ubytes_zero, tail_sum]
      omega

theorem sound_EqualTail : EqualTail.Goal := by
  intro x n
  simp [XorPredecessor.write, uleb_len_eq_ubytes, tail_sum]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_FrameSelection_XorPredecessor_EqualTail
