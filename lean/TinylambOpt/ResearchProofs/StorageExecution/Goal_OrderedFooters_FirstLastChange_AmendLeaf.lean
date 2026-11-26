import TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_FirstLastChange_AmendLeaf

open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters
open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.FirstLastChange

private def gen : Nat → List (List Nat)
  | 0 => [[]]
  | n + 1 => let p := gen n; p ++ p.flatMap (fun xs => [0 :: xs, 1 :: xs, 2 :: xs])

private def pages (n : Nat) : List (List (List Nat)) :=
  let ls := gen n
  [[]] ++ ls.map (fun a => [a]) ++ ls.flatMap (fun a => ls.map (fun b => [a, b]))

private def probe (n : Nat) : Bool :=
  let psys := pages n
  let reps : List (List Nat) := [[], [0], [1, 2], [3, 4, 5]]
  psys.all (fun ps => (List.range ps.length).all (fun i =>
    reps.all (fun xs =>
      combine agg (ps.set i xs) ==
        agg.join (combine agg (ps.take i))
          (agg.join (agg.reference xs) (combine agg (ps.drop (i + 1)))))))

#eval probe 3

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

private theorem join_ref_ref (a b : List Nat) :
    agg.join (agg.reference a) (agg.reference b) = agg.reference (a ++ b) := by
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

private theorem foldl_ref_append (s : List Nat) (ps : List (List Nat)) :
    List.foldl agg.join (agg.reference s) (ps.map agg.reference) =
      agg.reference (s ++ ps.flatten) := by
  induction ps generalizing s with
  | nil => simp
  | cons p ps ih =>
      simp only [List.map_cons, List.foldl_cons]
      rw [join_ref_ref]
      rw [ih (s ++ p)]
      simp [List.append_assoc]

private theorem combine_eq_ref (ps : List (List Nat)) :
    combine agg ps = agg.reference ps.flatten := by
  unfold combine
  simpa using foldl_ref_append [] ps

private theorem flatten_set (ps : List (List Nat)) (i : Nat) (xs : List Nat) (hi : i < ps.length) :
    (ps.set i xs).flatten = (ps.take i).flatten ++ xs ++ (ps.drop (i + 1)).flatten := by
  induction ps generalizing i with
  | nil => simp at hi
  | cons p ps ih =>
      cases i with
      | zero => simp
      | succ j =>
          have hj : j < ps.length := by simpa using hi
          simp [ih j hj, List.append_assoc]

theorem sound_AmendLeaf : FirstLastChange.AmendLeaf.Goal := by
  intro ps i xs hi
  simp only [combine_eq_ref]
  rw [flatten_set ps i xs hi]
  simp only [join_ref_ref]
  simp [List.append_assoc]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_FirstLastChange_AmendLeaf
