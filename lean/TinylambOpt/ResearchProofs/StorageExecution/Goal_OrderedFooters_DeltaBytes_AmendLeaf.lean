import TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_DeltaBytes_AmendLeaf

open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters
open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.DeltaBytes

private def gen : Nat → List (List Nat)
  | 0 => [[]]
  | n + 1 => let p := gen n; p ++ p.flatMap (fun xs => [0 :: xs, 1 :: xs, 2 :: xs, 7 :: xs])

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

abbrev State := Option (Nat × Nat × Nat)

private theorem join_left_id (a : State) : agg.join none a = a := by
  cases a <;> simp [agg, optionalJoin]

private theorem join_right_id (a : State) : agg.join a none = a := by
  cases a <;> simp [agg, optionalJoin]

private theorem join_assoc (a b c : State) :
    agg.join (agg.join a b) c = agg.join a (agg.join b c) := by
  cases a with
  | none => simp [join_left_id]
  | some a =>
      cases b with
      | none => simp [join_right_id, join_left_id]
      | some b =>
          cases c with
          | none => simp [join_right_id]
          | some c =>
              simp [agg, optionalJoin]
              omega

private theorem foldl_eq_join (l : List State) (a : State) :
    List.foldl agg.join a l = agg.join a (List.foldl agg.join none l) := by
  induction l generalizing a with
  | nil => simp [join_right_id]
  | cons x xs ih =>
      simp only [List.foldl_cons]
      rw [ih (agg.join a x), join_assoc, join_left_id, ih x]

private theorem foldl_append_join (l₁ l₂ : List State) (a : State) :
    List.foldl agg.join a (l₁ ++ l₂) =
      agg.join (List.foldl agg.join a l₁) (List.foldl agg.join none l₂) := by
  rw [List.foldl_append, foldl_eq_join]

private theorem combine_eq_fold (ps : List (List Nat)) :
    combine agg ps = List.foldl agg.join none (ps.map agg.reference) := by
  simp [combine, agg]

private theorem map_set (ps : List (List Nat)) (i : Nat) (xs : List Nat) (hi : i < ps.length) :
    (ps.set i xs).map agg.reference =
      (ps.take i).map agg.reference ++ [agg.reference xs] ++ (ps.drop (i + 1)).map agg.reference := by
  induction ps generalizing i with
  | nil => simp at hi
  | cons p ps ih =>
      cases i with
      | zero => simp
      | succ j =>
          have hj : j < ps.length := by simpa using hi
          simp [ih j hj, List.append_assoc]

theorem sound_AmendLeaf : DeltaBytes.AmendLeaf.Goal := by
  intro ps i xs hi
  rw [combine_eq_fold, map_set ps i xs hi]
  rw [combine_eq_fold, combine_eq_fold]
  rw [foldl_append_join]
  rw [foldl_append_join]
  simp [join_left_id]
  exact join_assoc _ _ _

end TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_DeltaBytes_AmendLeaf
