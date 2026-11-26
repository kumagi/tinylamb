import TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters

/-!
A footer can guide sorted scans and key-range probes when its indexed summary is
consistent with the stored blocks.

Proved

- `BitOccupancy.ReorderNeighbors` — Two neighboring states may be scheduled in reverse
  if they commute on their reachable values.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_BitOccupancy_ReorderNeighbors

open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters
open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.BitOccupancy

/-! ## Small-input truth checks -/

private def gen : Nat → List (List Nat)
  | 0 => [[]]
  | n + 1 => let p := gen n; p ++ p.flatMap (fun xs => [0 :: xs, 1 :: xs, 65 :: xs])

/-- Exhaustive small-input check of the implication (the join of this model is
commutative, so the premise always holds and the conclusion is a permutation
invariance of the length/OR summary). -/
private def probe (n : Nat) : Bool :=
  let ls := gen n
  ls.all (fun pre => ls.all (fun a => ls.all (fun b => ls.all (fun suf =>
    let sa := agg.join (agg.reference a) (agg.reference b)
    let sb := agg.join (agg.reference b) (agg.reference a)
    if sa == sb then agg.reference (pre ++ a ++ b ++ suf) == agg.reference (pre ++ b ++ a ++ suf) else true))))

#eval probe 1
#eval probe 2
#guard probe 2

/-! ## Stitch for BitOccupancy -/

-- Folding `a ||| 2^(x%64)` accumulates an OR of all observed bits, so folding
-- from an arbitrary starting mask just ORs it with the mask obtained from 0.
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
  simp only [agg, List.length_append]
  refine Prod.ext rfl ?_
  rw [List.foldl_append]
  exact (foldl_or b (a.foldl (fun acc x => acc ||| 2 ^ (x % 64)) 0)).symm

/-! ## Goal -/

theorem sound_ReorderNeighbors : BitOccupancy.ReorderNeighbors.Goal := by
  intro pre a b suf hcomm
  have step : ∀ x y : List Nat, agg.reference (pre ++ x ++ y ++ suf) =
      agg.join (agg.join (agg.reference pre) (agg.join (agg.reference x) (agg.reference y)))
        (agg.reference suf) := by
    intro x y
    rw [show pre ++ x ++ y ++ suf = (pre ++ (x ++ y)) ++ suf by simp [List.append_assoc]]
    rw [← stitch_agg, ← stitch_agg, ← stitch_agg]
  rw [step a b, step b a, hcomm]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_BitOccupancy_ReorderNeighbors
