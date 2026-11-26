import TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters

/-!
A footer can guide sorted scans and key-range probes when its indexed summary is
consistent with the stored blocks.

Proved

- `PairSubsequence.BatchedMarkers` — `∀ a b, agg.reference (List.replicate a 1 ++
  List.replicate b 2)=(a,b,a*b)`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_PairSubsequence_BatchedMarkers

open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters
open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.PairSubsequence

-- `agg.reference xs`.2.2 counts ordered pairs (i < j) with xs[i] = 1 and xs[j] = 2.
-- We use a recursive formulation of that count so the batched case can be
-- computed by induction, together with the cross term of a concatenation.
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

private theorem cross_nil : cross [] = 0 := by
  simp [cross]

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
  | nil => simp [cross_nil]
  | cons x xs ih =>
      simp only [List.cons_append, cross_cons, List.count_append, ih, List.count_cons]
      by_cases hx : x == 1
      · simp [hx, Nat.add_mul, Nat.add_assoc, Nat.add_left_comm, Nat.add_comm]
      · simp [hx, Nat.add_comm]

private theorem cross_replicate_one (a : Nat) : cross (List.replicate a 1) = 0 := by
  induction a with
  | zero => simp [cross_nil]
  | succ a ih =>
      rw [List.replicate_succ, cross_cons]
      simp [List.count_replicate, ih]

private theorem cross_replicate_two (b : Nat) : cross (List.replicate b 2) = 0 := by
  induction b with
  | zero => simp [cross_nil]
  | succ b ih =>
      rw [List.replicate_succ, cross_cons]
      simp [ih]

-- Sanity checks on small inputs before relying on the proof.
#eval (List.range 6).all (fun a =>
  (List.range 6).all (fun b =>
    decide (agg.reference (List.replicate a 1 ++ List.replicate b 2) = (a, b, a * b))))
#guard agg.reference (List.replicate 3 1 ++ List.replicate 4 2) == (3, 4, 12)
#guard agg.reference (List.replicate 0 1 ++ List.replicate 5 2) == (0, 5, 0)
#guard agg.reference (List.replicate 5 1 ++ List.replicate 0 2) == (5, 0, 0)

theorem sound_BatchedMarkers : PairSubsequence.BatchedMarkers.Goal := by
  intro a b
  change ((List.replicate a 1 ++ List.replicate b 2).count 1,
    (List.replicate a 1 ++ List.replicate b 2).count 2,
    cross (List.replicate a 1 ++ List.replicate b 2)) = (a, b, a * b)
  rw [List.count_append, List.count_append, cross_append]
  simp [List.count_replicate, cross_replicate_one, cross_replicate_two]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_PairSubsequence_BatchedMarkers
