import TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_DeltaBytes_TreeReduce

open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters
open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.DeltaBytes
open TinylambOpt.ResearchGoals.Compression.Wire

private def gen : Nat → List (List Nat)
  | 0 => [[]]
  | n + 1 => let p := gen n; p ++ p.flatMap (fun xs => [0 :: xs, 1 :: xs, 2 :: xs])

private def probe (n : Nat) : Bool :=
  let ls := gen n
  ls.all (fun a => ls.all (fun b =>
    agg.join (agg.reference a) (agg.reference b) == agg.reference (a ++ b)))

private def probeTree (n : Nat) : Bool :=
  let ls := gen n
  let leaves := ls.map Tree.leaf
  (leaves ++ (leaves.flatMap (fun a => leaves.map (fun b => Tree.node a b)))).all
    (fun t => rootState agg t == agg.reference (plain t))

#eval (probe 3, probe 4)
#eval (probeTree 3, probeTree 4)

private theorem last_getD (x d : Nat) (xs : List Nat) :
    (x :: xs).getLast?.getD d = xs.getLast?.getD x := by
  simp [List.getLast?_cons]

private theorem last_append (a b : List Nat) (d : Nat) (hb : b ≠ []) :
    (a ++ b).getLast?.getD d = b.getLast?.getD d := by
  rw [List.getLast?_append]
  cases h : b.getLast? with
  | none =>
      have : b = [] := by simpa using h
      contradiction
  | some z => simp

private theorem edges_cons (x : Nat) (zs : List Nat) (hzs : zs ≠ []) :
    edges (x :: zs) = (x, zs.head?.getD 0) :: edges zs := by
  cases zs with
  | nil => simp at hzs
  | cons y ys => simp [edges]

private theorem edges_append_nonempty (a b : List Nat) (ha : a ≠ []) (hb : b ≠ []) :
    edges (a ++ b) = edges a ++ ((a.getLast?.getD 0, b.head?.getD 0) :: edges b) := by
  induction a with
  | nil => simp at ha
  | cons x xs ih =>
      cases xs with
      | nil =>
          cases b with
          | nil => simp at hb
          | cons y ys => simp [edges, List.getLast?_cons]
      | cons y ys =>
          have hne : (y :: ys) ++ b ≠ [] := by simp
          have hhead : (((y :: ys) ++ b).head?.getD 0) = y := by simp
          rw [show (x :: y :: ys) ++ b = x :: ((y :: ys) ++ b) by rfl]
          rw [edges_cons x ((y :: ys) ++ b) hne, hhead]
          rw [edges_cons x (y :: ys) (by simp)]
          rw [ih (by simp)]
          simp [List.getLast?_cons]

private def edgeCost (xs : List Nat) :=
  ((edges xs).map (fun (x, y) => sbytes ((y : Int) - x))).sum

private theorem edgeCost_append (a b : List Nat) (ha : a ≠ []) (hb : b ≠ []) :
    edgeCost (a ++ b) =
      edgeCost a + sbytes ((b.head?.getD 0 : Int) - (a.getLast?.getD 0)) + edgeCost b := by
  unfold edgeCost
  rw [edges_append_nonempty a b ha hb]
  simp [List.map_append, List.sum_append, List.sum_cons]
  omega

private theorem edgeCost_append_cons (x y : Nat) (xs ys : List Nat) :
    edgeCost (x :: (xs ++ (y :: ys))) =
      edgeCost (x :: xs) + sbytes ((y : Int) - ((x :: xs).getLast?.getD 0)) +
        edgeCost (y :: ys) := by
  rw [show x :: (xs ++ (y :: ys)) = (x :: xs) ++ (y :: ys) by rfl]
  exact edgeCost_append (x :: xs) (y :: ys) (by simp) (by simp)

private theorem stitch_agg : stitch agg := by
  intro a b
  cases a with
  | nil => simp [agg, optionalJoin]
  | cons x xs =>
      cases b with
      | nil => simp [agg, optionalJoin]
      | cons y ys =>
          have hA : (x :: xs).getLast?.getD x = (x :: xs).getLast?.getD 0 := by
            rw [last_getD x x xs, last_getD x 0 xs]
          have hApp : (x :: (xs ++ (y :: ys))).getLast?.getD x =
              (y :: ys).getLast?.getD y := by
            rw [last_getD]
            rw [last_append xs (y :: ys) x (by simp)]
            rw [last_getD y x ys, last_getD y y ys]
          simp [agg, optionalJoin]
          change (y :: ys).getLast?.getD y = (x :: (xs ++ (y :: ys))).getLast?.getD x ∧
            edgeCost (x :: xs) + edgeCost (y :: ys) +
                sbytes ((y : Int) - ((x :: xs).getLast?.getD x)) =
              edgeCost (x :: (xs ++ (y :: ys)))
          constructor
          · exact hApp.symm
          · rw [edgeCost_append_cons, hA]
            omega

private theorem treeReduce_of_stitch {σ : Type} (f : Fold σ) (h : stitch f) : treeReduce f := by
  intro t
  induction t with
  | leaf xs => rfl
  | node a b iha ihb =>
      change f.join (rootState f a) (rootState f b) = f.reference (plain a ++ plain b)
      rw [iha, ihb, h]

theorem sound_TreeReduce : DeltaBytes.TreeReduce.Goal := by
  exact treeReduce_of_stitch agg stitch_agg

end TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_DeltaBytes_TreeReduce
