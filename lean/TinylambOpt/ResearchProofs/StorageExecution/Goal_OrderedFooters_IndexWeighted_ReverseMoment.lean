import TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters

/-!
A footer can guide sorted scans and key-range probes when its indexed summary is
consistent with the stored blocks.

Proved

- `IndexWeighted.ReverseMoment` — `∀ xs, (agg.reference xs.reverse).2.2 +
  (agg.reference xs).2.2 = (xs.length-1)*xs.sum`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_IndexWeighted_ReverseMoment

open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters
open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.IndexWeighted

-- Small-input truth checks (all reduce to `true`/equal pairs).
#eval (agg.reference [1,2,3]).2.2 + (agg.reference [1,2,3].reverse).2.2 == ([1,2,3].length-1)*[1,2,3].sum
#eval (agg.reference [3,2,1]).2.2 + (agg.reference [3,2,1].reverse).2.2 == ([3,2,1].length-1)*[3,2,1].sum
#eval (agg.reference [7,0,4,9,2]).2.2 + (agg.reference [7,0,4,9,2].reverse).2.2 == ([7,0,4,9,2].length-1)*[7,0,4,9,2].sum

private def momentAt (k : Nat) (xs : List Nat) : Nat :=
  (xs.zipIdx k).map (fun (v, i) => i * v) |>.sum

private def moment (xs : List Nat) : Nat := momentAt 0 xs

private theorem momentAt_shift (xs : List Nat) (k : Nat) :
    momentAt k xs = momentAt 0 xs + k * xs.sum := by
  induction xs generalizing k with
  | nil => simp [momentAt]
  | cons x xs ih =>
      calc
        momentAt k (x :: xs) = k * x + momentAt (k + 1) xs := by
          simp [momentAt, List.zipIdx_cons]
        _ = k * x + (momentAt 0 xs + (k + 1) * xs.sum) := by rw [ih]
        _ = momentAt 0 (x :: xs) + k * (x :: xs).sum := by
          have h0 : momentAt 0 (x :: xs) = momentAt 0 xs + xs.sum := by
            simpa [momentAt, List.zipIdx_cons] using ih 1
          rw [h0]
          simp only [List.sum_cons, Nat.succ_mul, Nat.mul_add]
          omega

private theorem moment_cons (a : Nat) (xs : List Nat) :
    moment (a :: xs) = moment xs + xs.sum := by
  simp [moment, momentAt, List.zipIdx_cons]
  simpa [momentAt] using momentAt_shift xs 1

private theorem moment_append_single (xs : List Nat) (a : Nat) :
    moment (xs ++ [a]) = moment xs + xs.length * a := by
  simp [moment, momentAt, List.zipIdx_append]

theorem sound_ReverseMoment : ReverseMoment.Goal := by
  intro xs
  change moment xs.reverse + moment xs = (xs.length - 1) * xs.sum
  induction xs with
  | nil => simp [moment, momentAt]
  | cons a ys ih =>
      rw [List.reverse_cons, moment_append_single, moment_cons]
      simp only [List.length_cons, List.length_reverse, List.sum_cons, Nat.add_sub_cancel_right]
      have hmul : (ys.length - 1) * ys.sum + ys.sum = ys.length * ys.sum := by
        cases ys with
        | nil => simp
        | cons y yt => simp [Nat.succ_mul]
      rw [show (moment ys.reverse + ys.length * a) + (moment ys + ys.sum) =
            (moment ys.reverse + moment ys) + ys.length * a + ys.sum by omega]
      rw [ih]
      rw [show (ys.length - 1) * ys.sum + ys.length * a + ys.sum =
            ((ys.length - 1) * ys.sum + ys.sum) + ys.length * a by omega]
      rw [hmul, Nat.mul_add]
      omega

end TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_IndexWeighted_ReverseMoment
