import TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters

/-!
A footer can guide sorted scans and key-range probes when its indexed summary is
consistent with the stored blocks.

Proved

- `IndexWeighted.Stitch` — Compute a page boundary correction using only the two
  serialized states.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_IndexWeighted_Stitch

open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters
open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.IndexWeighted

/-- The index-weighted moment: sum of each value times its physical ordinal. -/
private def wsum (xs : List Nat) : Nat :=
  (xs.zipIdx.map (fun (v, i) => i * v)).sum

/-- Starting the ordinal numbering at `k` adds `k` to every index. -/
private theorem wsum_offset (k : Nat) (xs : List Nat) :
    ((xs.zipIdx k).map (fun (v, i) => i * v)).sum = wsum xs + k * xs.sum := by
  induction xs generalizing k with
  | nil => simp [wsum]
  | cons x xs ih =>
      simp only [List.zipIdx_cons, List.map_cons, List.sum_cons]
      rw [ih (k + 1)]
      rw [show wsum (x :: xs) = wsum xs + xs.sum by
        simp only [wsum, List.zipIdx_cons, List.map_cons, List.sum_cons, Nat.zero_mul, Nat.zero_add]
        rw [ih 1]
        simp [wsum]]
      rw [Nat.mul_add]
      rw [show k + 1 = Nat.succ k by rfl, Nat.succ_mul]
      omega

/-- Appending shifts all indices of the right list by the left list's length. -/
private theorem wsum_append (a b : List Nat) :
    wsum (a ++ b) = wsum a + wsum b + a.length * b.sum := by
  simp only [wsum, List.zipIdx_append, List.map_append, List.sum_append]
  rw [wsum_offset (0 + a.length) b]
  simp [wsum]
  omega

theorem sound_Stitch : IndexWeighted.Stitch.Goal := by
  intro a b
  simp only [agg, List.length_append, List.sum_append]
  refine Prod.ext rfl (Prod.ext rfl ?_)
  simpa [wsum] using (wsum_append a b).symm

end TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_IndexWeighted_Stitch
