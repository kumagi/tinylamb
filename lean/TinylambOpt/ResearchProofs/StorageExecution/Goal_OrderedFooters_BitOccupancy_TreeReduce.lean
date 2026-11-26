import TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters

/-!
A footer can guide sorted scans and key-range probes when its indexed summary is
consistent with the stored blocks.

Proved

- `BitOccupancy.TreeReduce` — Merge an arbitrary spill tree without visiting the raw
  rows again.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_BitOccupancy_TreeReduce

open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters
open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.BitOccupancy

-- Folding `a ||| 2^(x%64)` accumulates an OR of all the observed bits, so an
-- arbitrary starting mask is simply OR-ed with the mask obtained from 0.
private theorem foldl_or (xs : List Nat) (init : Nat) :
    xs.foldl (fun a x => a ||| 2 ^ (x % 64)) init =
      init ||| xs.foldl (fun a x => a ||| 2 ^ (x % 64)) 0 := by
  induction xs generalizing init with
  | nil => simp
  | cons x xs ih =>
      simp only [List.foldl_cons]
      rw [ih (init ||| 2 ^ (x % 64))]
      simp [ih (2 ^ (x % 64)), Nat.or_assoc]

private theorem stitch_agg : stitch agg := by
  intro a b
  change (a.length + b.length,
      a.foldl (fun acc x => acc ||| 2 ^ (x % 64)) 0 |||
        b.foldl (fun acc x => acc ||| 2 ^ (x % 64)) 0) =
    ((a ++ b).length, (a ++ b).foldl (fun acc x => acc ||| 2 ^ (x % 64)) 0)
  simp only [List.length_append]
  congr 1
  rw [List.foldl_append]
  exact (foldl_or b (a.foldl (fun acc x => acc ||| 2 ^ (x % 64)) 0)).symm

private theorem treeReduce_of_stitch {σ : Type} (f : Fold σ) (h : stitch f) : treeReduce f := by
  intro t
  induction t with
  | leaf xs => rfl
  | node a b iha ihb =>
      change f.join (rootState f a) (rootState f b) = f.reference (plain a ++ plain b)
      rw [iha, ihb, h]

-- Sanity checks on small inputs before relying on the proof.
#eval (agg.join (agg.reference [1, 2, 65]) (agg.reference [2, 130, 3]) ==
  agg.reference ([1, 2, 65] ++ [2, 130, 3]))
#eval (rootState agg (Tree.node (Tree.leaf [1, 2]) (Tree.node (Tree.leaf [65]) (Tree.leaf [1, 3]))) ==
  agg.reference (plain (Tree.node (Tree.leaf [1, 2]) (Tree.node (Tree.leaf [65]) (Tree.leaf [1, 3])))))
#guard agg.join (agg.reference [1, 2, 65]) (agg.reference [2, 130, 3]) ==
  agg.reference ([1, 2, 65] ++ [2, 130, 3])

theorem sound_TreeReduce : BitOccupancy.TreeReduce.Goal := by
  exact treeReduce_of_stitch agg stitch_agg

end TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_BitOccupancy_TreeReduce
