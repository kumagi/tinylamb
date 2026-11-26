import TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters

/-!
A footer can guide sorted scans and key-range probes when its indexed summary is
consistent with the stored blocks.

Proved

- `FirstLastChange.PrefixCheckpoint` — A saved prefix plus page states yields a scan
  checkpoint at each boundary.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_FirstLastChange_PrefixCheckpoint

open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters
open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.FirstLastChange

private def gen : Nat → List (List Nat)
  | 0 => [[]]
  | n + 1 => let p := gen n; p ++ p.flatMap (fun xs => [0 :: xs, 1 :: xs, 2 :: xs])

private def pageSets : List (List (List Nat)) :=
  [ [], [[]], [[0]], [[1,1]], [[],[2,0]], [[0],[0,1],[1,1,2]], [[2,2],[0],[1,0,1]] ]

private def probe (n : Nat) : Bool :=
  let ls := gen n
  ls.all (fun init => pageSets.all (fun ps =>
    (ps.map agg.reference).scanl agg.join (agg.reference init) =
      (List.range (ps.length+1)).map (fun i => agg.reference (init ++ (ps.take i).flatten))))

#eval probe 4

private theorem getLast_cons_eq_getD (x : Nat) (xs : List Nat) :
    (x :: xs).getLast (by simp) = xs.getLast?.getD x := by
  have h := List.getLast?_eq_some_getLast (l := x :: xs) (by simp)
  rw [List.getLast?_cons] at h
  simpa using h.symm

private theorem getLast?_append_right (a b : List Nat) (hb : b ≠ []) :
    (a ++ b).getLast? = b.getLast? := by
  rw [List.getLast?_append]
  cases b with
  | nil => contradiction
  | cons y ys => simp [List.getLast?_cons]

private theorem edges_append (a b : List Nat) (ha : a ≠ []) (hb : b ≠ []) :
    edges (a ++ b) = edges a ++ ((a.getLast ha, b.head hb) :: edges b) := by
  induction a with
  | nil => contradiction
  | cons x xs ih =>
      cases xs with
      | nil =>
          cases b with
          | nil => contradiction
          | cons y ys => simp [edges]
      | cons y ys =>
          have ha' : y :: ys ≠ [] := by simp
          have hih := ih ha'
          cases b with
          | nil => contradiction
          | cons z zs =>
              simp [edges] at hih ⊢
              simpa [List.getLast_cons] using hih

private theorem stitchAgg : stitch agg := by
  intro a b
  cases a with
  | nil => simp [agg, optionalJoin]
  | cons x xs =>
      cases b with
      | nil => simp [agg, optionalJoin]
      | cons y ys =>
          simp only [agg, List.head?_cons, Option.map_some, optionalJoin]
          have hlast : (x :: xs ++ y :: ys).getLast? = (y :: ys).getLast? :=
            getLast?_append_right (x :: xs) (y :: ys) (by simp)
          have hlastB : (y :: ys).getLast? = some ((y :: ys).getLast (by simp)) :=
            List.getLast?_eq_some_getLast (by simp)
          have hedges := edges_append (x :: xs) (y :: ys) (by simp) (by simp)
          simp only [List.head_cons, getLast_cons_eq_getD] at hedges
          simp only [hlast, hlastB, List.getLast?_cons, Option.getD_some]
          simp only [hedges, List.countP_append, List.countP_cons]
          by_cases h : xs.getLast?.getD x = y
          · simp [h]
            omega
          · simp [h]
            omega

theorem sound_PrefixCheckpoint : FirstLastChange.PrefixCheckpoint.Goal := by
  intro init ps
  induction ps generalizing init with
  | nil => simp
  | cons p ps ih =>
      simp only [List.map_cons, List.scanl_cons]
      rw [stitchAgg init p]
      rw [ih (init ++ p)]
      simp [List.range_succ_eq_map, Function.comp_def, List.append_assoc]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_FirstLastChange_PrefixCheckpoint
