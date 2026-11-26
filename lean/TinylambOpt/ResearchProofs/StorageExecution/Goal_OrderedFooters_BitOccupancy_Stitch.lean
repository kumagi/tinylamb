import TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_BitOccupancy_Stitch

open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters
open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.BitOccupancy

private def gen : Nat → List (List Nat)
  | 0 => [[]]
  | n + 1 => let p := gen n; p ++ p.flatMap (fun xs => [0 :: xs, 1 :: xs, 63 :: xs, 64 :: xs, 65 :: xs])

private def probe (n : Nat) : Bool :=
  let ls := gen n
  ls.all (fun a => ls.all (fun b =>
    agg.join (agg.reference a) (agg.reference b) == agg.reference (a ++ b)))

-- Small-input truth check: both enumerations return `true`.
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

theorem sound_Stitch : BitOccupancy.Stitch.Goal := by
  intro a b
  simp only [agg, List.length_append]
  exact Prod.ext rfl (mask_append a b).symm

end TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_BitOccupancy_Stitch
