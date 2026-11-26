import TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters

/-!
A footer can guide sorted scans and key-range probes when its indexed summary is
consistent with the stored blocks.

Proved

- `PairSubsequence.ReorderNeighbors` — Two neighboring states may be scheduled in
  reverse if they commute on their reachable values.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_PairSubsequence_ReorderNeighbors

open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters
open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.PairSubsequence

-- The third component of `agg.reference xs` counts ordered pairs (i < j) with
-- xs[i] = 1 and xs[j] = 2.  Splitting a list in two adds exactly one cross
-- term, `count 1` of the left part times `count 2` of the right part.
private def cross (xs : List Nat) : Nat :=
  (xs.zipIdx.map (fun (v, i) => if v == 1 then (xs.drop (i + 1)).count 2 else 0)).sum

private theorem zipIdx_shift (xs : List Nat) (n m : Nat) :
    xs.zipIdx (n + m) = (xs.zipIdx n).map (fun (v, i) => (v, i + m)) := by
  induction xs generalizing n with
  | nil => simp
  | cons x xs ih =>
      simp only [List.zipIdx_cons, List.map_cons]
      congr 1
      simpa [Nat.add_assoc, Nat.add_comm, Nat.add_left_comm] using ih (n + 1)

private theorem cross_cons (x : Nat) (xs : List Nat) :
    cross (x :: xs) = (if x == 1 then xs.count 2 else 0) + cross xs := by
  unfold cross
  rw [List.zipIdx_cons]
  simp only [List.map_cons, List.sum_cons, Nat.zero_add]
  rw [show xs.zipIdx 1 = (xs.zipIdx).map (fun (v, i) => (v, i + 1)) by
    simpa using zipIdx_shift xs 0 1]
  simp [List.map_map, Function.comp_def]

private theorem cross_append (a b : List Nat) :
    cross (a ++ b) = cross a + cross b + a.count 1 * b.count 2 := by
  induction a with
  | nil => simp [cross]
  | cons x xs ih =>
      simp only [List.cons_append, cross_cons, List.count_append, ih, List.count_cons]
      by_cases hx : x == 1
      · simp [hx, Nat.add_mul, Nat.add_assoc, Nat.add_left_comm, Nat.add_comm]
      · simp [hx, Nat.add_comm]

theorem sound_ReorderNeighbors : PairSubsequence.ReorderNeighbors.Goal := by
  intro pre a b suf h
  have h3 : cross a + cross b + a.count 1 * b.count 2 =
      cross b + cross a + b.count 1 * a.count 2 := by
    simpa [agg, cross] using congrArg (fun t : Nat × Nat × Nat => t.2.2) h
  have hswap : a.count 1 * b.count 2 = b.count 1 * a.count 2 := by
    omega
  apply Prod.ext
  · simp [agg, List.count_append, Nat.add_assoc, Nat.add_left_comm]
  apply Prod.ext
  · simp [agg, List.count_append, Nat.add_assoc, Nat.add_left_comm]
  · change cross (pre ++ a ++ b ++ suf) = cross (pre ++ b ++ a ++ suf)
    simp [cross_append, List.count_append, Nat.mul_add,
      Nat.add_assoc, Nat.add_comm, Nat.add_left_comm]
    omega

end TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_PairSubsequence_ReorderNeighbors
