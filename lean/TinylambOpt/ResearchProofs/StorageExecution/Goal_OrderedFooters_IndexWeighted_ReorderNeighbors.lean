import TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters

/-!
A footer can guide sorted scans and key-range probes when its indexed summary is
consistent with the stored blocks.

Proved

- `IndexWeighted.ReorderNeighbors` — Two neighboring states may be scheduled in reverse
  if they commute on their reachable values.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_IndexWeighted_ReorderNeighbors

open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters
open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.IndexWeighted

/-- The third component of `agg.reference xs`: value times physical ordinal. -/
private def wsum (xs : List Nat) : Nat :=
  (xs.zipIdx.map (fun (v, i) => i * v)).sum

/-- Starting the indices at `k` adds `k` times the total sum. -/
private theorem wsum_shift (k : Nat) (xs : List Nat) :
    ((xs.zipIdx k).map (fun (v, i) => i * v)).sum =
      ((xs.zipIdx).map (fun (v, i) => i * v)).sum + k * xs.sum := by
  induction xs generalizing k with
  | nil => simp
  | cons x xs ih =>
      change k * x + ((xs.zipIdx (k + 1)).map (fun (v, i) => i * v)).sum =
        (0 * x + ((xs.zipIdx 1).map (fun (v, i) => i * v)).sum) + k * (x + xs.sum)
      rw [ih (k + 1), ih 1]
      simp [Nat.succ_mul, Nat.mul_add, Nat.add_assoc, Nat.add_comm, Nat.add_left_comm]

/-- Appending shifts every index of the right list by the left list's length. -/
private theorem wsum_append (a b : List Nat) :
    wsum (a ++ b) = wsum a + wsum b + a.length * b.sum := by
  simp only [wsum, List.zipIdx_append, List.map_append, List.sum_append]
  rw [wsum_shift (0 + a.length) b]
  simp [Nat.add_comm, Nat.add_left_comm]

theorem sound_ReorderNeighbors : IndexWeighted.ReorderNeighbors.Goal := by
  intro pre a b suf h
  have hcross : a.length * b.sum = b.length * a.sum := by
    have h3 := congrArg (fun t : Nat × Nat × Nat => t.2.2) h
    simp [agg] at h3
    omega
  apply Prod.ext
  · simp [agg]
    omega
  · apply Prod.ext
    · simp [agg]
      omega
    · change wsum (pre ++ a ++ b ++ suf) = wsum (pre ++ b ++ a ++ suf)
      simp only [wsum_append, List.length_append, Nat.add_mul]
      omega

end TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_IndexWeighted_ReorderNeighbors
