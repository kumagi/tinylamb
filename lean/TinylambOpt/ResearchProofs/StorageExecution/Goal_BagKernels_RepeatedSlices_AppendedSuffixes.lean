import TinylambOpt.ResearchGoals.StorageExecution.BagKernels

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_RepeatedSlices_AppendedSuffixes

open TinylambOpt.ResearchGoals.StorageExecution.BagKernels
open TinylambOpt.ResearchGoals.StorageExecution.BagKernels.RepeatedSlices

-- Sanity checks on small inputs, including empty parts and repeated keys.
#eval bag.mass ([1, 2, 1] ++ [3, 1]) 1
#eval bag.mass ([1, 2, 1] : List Nat) 1 + bag.mass ([3, 1] : List Nat) 1 + ([1, 2, 1] : List Nat).length * ([3, 1] : List Nat).count 1
example : bag.mass (([1, 2, 1] : List Nat) ++ [3, 1]) 1 =
    bag.mass ([1, 2, 1] : List Nat) 1 + bag.mass ([3, 1] : List Nat) 1 + ([1, 2, 1] : List Nat).length * ([3, 1] : List Nat).count 1 := by decide
example : bag.mass (([] : List Nat) ++ [3, 1]) 1 =
    bag.mass ([] : List Nat) 1 + bag.mass ([3, 1] : List Nat) 1 + ([] : List Nat).length * ([3, 1] : List Nat).count 1 := by decide
example : bag.mass (([1, 2, 1] : List Nat) ++ []) 1 =
    bag.mass ([1, 2, 1] : List Nat) 1 + bag.mass ([] : List Nat) 1 + ([1, 2, 1] : List Nat).length * ([] : List Nat).count 1 := by decide
example : bag.mass (([7, 7] : List Nat) ++ [7, 7]) 7 =
    bag.mass ([7, 7] : List Nat) 7 + bag.mass ([7, 7] : List Nat) 7 + ([7, 7] : List Nat).length * ([7, 7] : List Nat).count 7 := by decide

/-- Starting the ordinal stream at `o` adds exactly `o` to every occurrence. -/
private theorem massOffset (xs : List Nat) (k o : Nat) :
    (((xs.zipIdx o).filter (fun e => e.1 == k)).map (fun e => e.2 + 1)).sum =
      (((xs.zipIdx).filter (fun e => e.1 == k)).map (fun e => e.2 + 1)).sum + o * xs.count k := by
  induction xs generalizing o with
  | nil => simp
  | cons x xs ih =>
      rw [List.zipIdx_cons, List.zipIdx_cons]
      by_cases hx : x == k
      · simp [hx, List.count_cons, ih (o + 1), ih 1, Nat.add_mul, Nat.mul_add]
        omega
      · simp [hx, List.count_cons, ih (o + 1), ih 1, Nat.add_mul]
        omega

theorem sound_AppendedSuffixes : RepeatedSlices.AppendedSuffixes.Goal := by
  intro a b k
  unfold bag
  dsimp only
  rw [List.zipIdx_append]
  simp only [List.filter_append, List.map_append, List.sum_append, Nat.zero_add]
  rw [massOffset b k a.length]
  omega

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_RepeatedSlices_AppendedSuffixes
