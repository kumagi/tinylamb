import TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters

/-!
A footer can guide sorted scans and key-range probes when its indexed summary is
consistent with the stored blocks.

Proved

- `BitOccupancy.AmendLeaf` — Update one cached block using the left and right partial
  states.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_BitOccupancy_AmendLeaf

open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters
open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.BitOccupancy

/-! Small-input truth check: `combine` is a pure fold of the site summary, so
replacing one cached page must decompose as prefix join page join suffix. -/
private def gen : Nat → List (List Nat)
  | 0 => [[]]
  | n + 1 => let p := gen n; p ++ p.flatMap (fun xs => [0 :: xs, 1 :: xs, 63 :: xs, 64 :: xs, 65 :: xs])

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

#eval probe 2
#eval probe 3

private def mask (xs : List Nat) : Nat :=
  xs.foldl (fun a x => a ||| 2 ^ (x % 64)) 0

private theorem fold_or_shift (xs : List Nat) (p q : Nat) :
    xs.foldl (fun a x => a ||| 2 ^ (x % 64)) (p ||| q) =
      p ||| xs.foldl (fun a x => a ||| 2 ^ (x % 64)) q := by
  induction xs generalizing p q with
  | nil => simp
  | cons x xs ih =>
      simp only [List.foldl_cons]
      rw [show (p ||| q) ||| 2 ^ (x % 64) = p ||| (q ||| 2 ^ (x % 64)) by
        simp [Nat.or_assoc]]
      exact ih p (q ||| 2 ^ (x % 64))

private theorem mask_append (a b : List Nat) : mask (a ++ b) = mask a ||| mask b := by
  unfold mask
  rw [List.foldl_append]
  rw [show (List.foldl (fun a x => a ||| 2 ^ (x % 64)) 0 a) =
      (List.foldl (fun a x => a ||| 2 ^ (x % 64)) 0 a ||| 0) by simp]
  rw [fold_or_shift]
  simp

private theorem agg_stitch : stitch agg := by
  intro a b
  simp only [agg, List.length_append]
  exact Prod.ext rfl (mask_append a b).symm

private theorem foldl_ref_append (ps : List (List Nat)) (acc : List Nat) :
    (ps.map agg.reference).foldl agg.join (agg.reference acc) =
      agg.reference (acc ++ ps.flatten) := by
  induction ps generalizing acc with
  | nil => simp
  | cons p ps ih =>
      rw [List.map_cons, List.foldl_cons]
      rw [agg_stitch acc p]
      simpa [List.append_assoc] using ih (acc ++ p)

private theorem combine_eq_reference_flatten (ps : List (List Nat)) :
    combine agg ps = agg.reference ps.flatten := by
  simpa [combine] using foldl_ref_append ps []

private theorem flatten_set (ps : List (List Nat)) (i : Nat) (xs : List Nat)
    (hi : i < ps.length) :
    (ps.set i xs).flatten = (ps.take i).flatten ++ (xs ++ (ps.drop (i + 1)).flatten) := by
  induction ps generalizing i with
  | nil => simp at hi
  | cons p ps ih =>
      cases i with
      | zero => simp
      | succ i =>
          simp at hi ⊢
          simpa [List.append_assoc] using ih i hi

theorem sound_AmendLeaf : BitOccupancy.AmendLeaf.Goal := by
  intro ps i xs hi
  rw [combine_eq_reference_flatten (ps.set i xs)]
  rw [combine_eq_reference_flatten (ps.take i)]
  rw [combine_eq_reference_flatten (ps.drop (i + 1))]
  rw [flatten_set ps i xs hi]
  rw [← agg_stitch (ps.take i).flatten (xs ++ (ps.drop (i + 1)).flatten)]
  rw [← agg_stitch xs (ps.drop (i + 1)).flatten]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_BitOccupancy_AmendLeaf
