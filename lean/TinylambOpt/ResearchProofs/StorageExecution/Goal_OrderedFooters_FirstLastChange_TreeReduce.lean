import TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_FirstLastChange_TreeReduce

open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters
open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.FirstLastChange

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
          rw [show (x :: y :: ys) ++ b = x :: ((y :: ys) ++ b) by simp]
          rw [edges_cons x ((y :: ys) ++ b) hne, hhead]
          rw [edges_cons x (y :: ys) (by simp)]
          rw [ih (by simp)]
          simp [List.getLast?_cons]

private theorem changes_append (a b : List Nat) (ha : a ≠ []) (hb : b ≠ []) :
    (edges (a ++ b)).countP (fun p : Nat × Nat => p.1 != p.2) =
      (edges a).countP (fun p : Nat × Nat => p.1 != p.2) +
      (edges b).countP (fun p : Nat × Nat => p.1 != p.2) +
      (decide (a.getLast?.getD 0 ≠ b.head?.getD 0)).toNat := by
  rw [edges_append_nonempty a b ha hb, List.countP_append]
  by_cases h : a.getLast?.getD 0 ≠ b.head?.getD 0 <;> simp [h] <;> omega

private theorem stitch_agg : stitch agg := by
  intro a b
  cases a with
  | nil => simp [agg, optionalJoin]
  | cons x xs =>
      cases b with
      | nil => simp [agg, optionalJoin]
      | cons y ys =>
          have hch := changes_append (x :: xs) (y :: ys) (by simp) (by simp)
          simp [agg, optionalJoin, List.getLast?_append, List.getLast?_cons]
          have hch' :
              List.countP (fun p : Nat × Nat => p.1 != p.2) (edges (x :: (xs ++ y :: ys))) =
                List.countP (fun p : Nat × Nat => p.1 != p.2) (edges (x :: xs)) +
                List.countP (fun p : Nat × Nat => p.1 != p.2) (edges (y :: ys)) +
                (decide (y ≠ xs.getLast?.getD x)).toNat := by
            simpa [List.cons_append, List.getLast?_cons, eq_comm] using hch
          rw [hch']
          have hdec :
              (decide (xs.getLast?.getD x = y)).toNat +
                (decide (y ≠ xs.getLast?.getD x)).toNat = 1 := by
            by_cases h : xs.getLast?.getD x = y
            · simp [h]
            · have hy : y ≠ xs.getLast?.getD x := by
                intro he
                exact h he.symm
              simp [h, hy]
          omega

theorem sound_TreeReduce : FirstLastChange.TreeReduce.Goal := by
  intro t
  induction t with
  | leaf xs => simp [rootState, plain]
  | node a b iha ihb =>
      simp only [rootState, plain, iha, ihb]
      exact stitch_agg _ _

end TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_FirstLastChange_TreeReduce
