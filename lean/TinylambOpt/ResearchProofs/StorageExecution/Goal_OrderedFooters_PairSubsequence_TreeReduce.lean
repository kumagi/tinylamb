import TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_PairSubsequence_TreeReduce

open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters
open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.PairSubsequence

-- The third component of `agg.reference xs` counts pairs (i < j) with
-- xs[i] = 1 and xs[j] = 2.  We use this recursive formulation to prove
-- that the join splits a concatenation into within-part counts plus the
-- cross term `count 1 a * count 2 b` (i.e. `stitch agg` holds).
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

private theorem cross_nil : cross [] = 0 := by simp [cross]

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

private theorem stitch_agg : stitch agg := by
  intro a b
  change (a.count 1 + b.count 1, a.count 2 + b.count 2,
    cross a + cross b + a.count 1 * b.count 2) =
    ((a ++ b).count 1, (a ++ b).count 2, cross (a ++ b))
  rw [List.count_append, List.count_append, cross_append]

private theorem treeReduce_of_stitch {σ : Type} (f : Fold σ) (h : stitch f) : treeReduce f := by
  intro t
  induction t with
  | leaf xs => rfl
  | node a b iha ihb =>
      change f.join (rootState f a) (rootState f b) = f.reference (plain a ++ plain b)
      rw [iha, ihb, h]

-- Sanity checks on small inputs before relying on the proof.
#eval (agg.join (agg.reference [1, 2, 1]) (agg.reference [2, 1, 2]) ==
  agg.reference ([1, 2, 1] ++ [2, 1, 2]))
#eval (rootState agg (Tree.node (Tree.leaf [1, 2]) (Tree.node (Tree.leaf [2]) (Tree.leaf [1, 2]))) ==
  agg.reference (plain (Tree.node (Tree.leaf [1, 2]) (Tree.node (Tree.leaf [2]) (Tree.leaf [1, 2])))))
#guard agg.join (agg.reference [1, 2, 1]) (agg.reference [2, 1, 2]) ==
  agg.reference ([1, 2, 1] ++ [2, 1, 2])
#guard rootState agg (Tree.node (Tree.leaf [1, 2]) (Tree.node (Tree.leaf [2]) (Tree.leaf [1, 2]))) ==
  agg.reference (plain (Tree.node (Tree.leaf [1, 2]) (Tree.node (Tree.leaf [2]) (Tree.leaf [1, 2]))))

theorem sound_TreeReduce : PairSubsequence.TreeReduce.Goal := by
  exact treeReduce_of_stitch agg stitch_agg

end TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_PairSubsequence_TreeReduce
