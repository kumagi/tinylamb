import TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_IndexWeighted_TreeReduce

open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters
open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.IndexWeighted

-- `agg.reference xs = (length, sum, moment xs)` where `moment xs` is the sum of
-- `i * xs[i]` over the physical ordinals `i`.  We prove that the join splits the
-- moment of a concatenation into the two within-part moments plus the cross
-- term `length a * sum b`; this is `stitch agg`, which then lifts to trees.

private def moment (xs : List Nat) : Nat := (xs.zipIdx.map (fun (v, i) => i * v)).sum
private def momentFrom (n : Nat) (xs : List Nat) : Nat := (xs.zipIdx n).map (fun (v, i) => i * v) |>.sum

private theorem momentFrom_cons (n : Nat) (x : Nat) (xs : List Nat) :
    momentFrom n (x :: xs) = n * x + momentFrom (n + 1) xs := by
  simp [momentFrom, List.zipIdx_cons]

private theorem momentFrom_shift (n : Nat) (xs : List Nat) :
    momentFrom n xs = moment xs + n * xs.sum := by
  induction xs generalizing n with
  | nil => simp [momentFrom, moment]
  | cons x xs ih =>
      have hm : moment (x :: xs) = moment xs + xs.sum := by
        change momentFrom 0 (x :: xs) = moment xs + xs.sum
        rw [momentFrom_cons, ih 1]
        simp
      rw [momentFrom_cons, hm, ih (n + 1)]
      simp [Nat.add_mul, Nat.mul_add, Nat.add_assoc, Nat.add_comm, Nat.add_left_comm]

private theorem momentFrom_append (n : Nat) (a b : List Nat) :
    momentFrom n (a ++ b) = momentFrom n a + momentFrom (n + a.length) b := by
  induction a generalizing n with
  | nil => simp [momentFrom]
  | cons x xs ih =>
      change momentFrom n (x :: (xs ++ b)) =
        momentFrom n (x :: xs) + momentFrom (n + (x :: xs).length) b
      rw [momentFrom_cons, momentFrom_cons, ih (n + 1)]
      simp [Nat.add_assoc, Nat.add_comm, Nat.add_left_comm]

private theorem stitch_agg : stitch agg := by
  intro a b
  change (a.length + b.length, a.sum + b.sum,
    moment a + moment b + a.length * b.sum) =
    ((a ++ b).length, (a ++ b).sum, moment (a ++ b))
  rw [List.length_append, List.sum_append]
  congr 1
  rw [show moment (a ++ b) = moment a + momentFrom a.length b by
    simpa [moment, momentFrom] using (momentFrom_append 0 a b)]
  rw [momentFrom_shift]
  simp [Nat.add_assoc]

private theorem treeReduce_of_stitch {σ : Type} (f : Fold σ) (h : stitch f) : treeReduce f := by
  intro t
  induction t with
  | leaf xs => rfl
  | node a b iha ihb =>
      change f.join (rootState f a) (rootState f b) = f.reference (plain a ++ plain b)
      rw [iha, ihb, h]

-- Sanity checks on small inputs before relying on the proof.
#eval (agg.join (agg.reference [1, 2, 3]) (agg.reference [4, 5]) ==
  agg.reference ([1, 2, 3] ++ [4, 5]))
#eval (rootState agg (Tree.node (Tree.leaf [1, 2]) (Tree.node (Tree.leaf [3]) (Tree.leaf [4, 5]))) ==
  agg.reference (plain (Tree.node (Tree.leaf [1, 2]) (Tree.node (Tree.leaf [3]) (Tree.leaf [4, 5])))))
#guard agg.join (agg.reference [1, 2, 3]) (agg.reference [4, 5]) ==
  agg.reference ([1, 2, 3] ++ [4, 5])
#guard rootState agg (Tree.node (Tree.leaf [1, 2]) (Tree.node (Tree.leaf [3]) (Tree.leaf [4, 5]))) ==
  agg.reference (plain (Tree.node (Tree.leaf [1, 2]) (Tree.node (Tree.leaf [3]) (Tree.leaf [4, 5]))))

theorem sound_TreeReduce : IndexWeighted.TreeReduce.Goal := by
  exact treeReduce_of_stitch agg stitch_agg

end TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_IndexWeighted_TreeReduce
