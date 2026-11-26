import TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_PairSubsequence_ReversalComplement

open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters
open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.PairSubsequence

-- The third component of `agg.reference xs` counts ordered pairs (i < j)
-- with xs[i] = 1 and xs[j] = 2.  Reversing the list turns those into the
-- pairs (i > j) with xs[i] = 1 and xs[j] = 2, so the two counts add up to
-- every choice of a 1-position and a 2-position.
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

private theorem cross_append_single (xs : List Nat) (x : Nat) :
    cross (xs ++ [x]) = cross xs + xs.count 1 * (if x == 2 then 1 else 0) := by
  rw [cross_append]
  simp [cross, List.count_singleton]

-- Small-input sanity checks (lists over {0,1,2} of length at most 6).
private def gen : Nat → List (List Nat)
  | 0 => [[]]
  | n + 1 => let p := gen n; p ++ p.flatMap (fun xs => [0 :: xs, 1 :: xs, 2 :: xs])

private def probe (n : Nat) : Bool :=
  (gen n).all (fun xs => cross xs + cross xs.reverse == xs.count 1 * xs.count 2)

#eval probe 6
#guard probe 6

theorem sound_ReversalComplement : PairSubsequence.ReversalComplement.Goal := by
  intro xs
  change cross xs + cross xs.reverse = xs.count 1 * xs.count 2
  induction xs with
  | nil => simp [cross]
  | cons x xs ih =>
      rw [List.reverse_cons, cross_cons, cross_append_single, List.count_reverse]
      by_cases hx1 : x = 1
      · subst x
        simp [ih, Nat.add_mul, Nat.add_assoc, Nat.add_comm]
      · by_cases hx2 : x = 2
        · subst x
          simp [ih, Nat.mul_add, Nat.add_comm, Nat.add_left_comm]
        · simp [hx1, hx2, ih]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_PairSubsequence_ReversalComplement
