import TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters

/-!
A footer can guide sorted scans and key-range probes when its indexed summary is
consistent with the stored blocks.

Proved

- `BitOccupancy.FrameSlice` — Use only the two ragged boundaries and whole-page
  summaries for a window.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_BitOccupancy_FrameSlice

open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters
open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.BitOccupancy

/-! Ground check on small inputs (both probes must return `true`). -/

private def gen : Nat → List (List Nat)
  | 0 => [[]]
  | n + 1 => let p := gen n; p ++ p.flatMap (fun xs => [0 :: xs, 1 :: xs, 65 :: xs])

private def probe (n : Nat) : Bool :=
  let ls := gen n
  ls.all (fun a => ls.all (fun p => ls.all (fun z =>
    let ps := [p, a]
    (List.range (a.length + 1)).all (fun lo =>
      (List.range 4).all (fun hi =>
        agg.join (agg.reference (a.drop lo))
            (agg.join (combine agg ps) (agg.reference (z.take hi))) ==
          agg.reference (((a ++ ps.flatten ++ z).drop lo).take
            (a.length - lo + ps.flatten.length + hi)))))))

#eval probe 2
#eval probe 3

/-! FrameSlice is a generic consequence of stitch: page summaries fold into the
summary of their concatenation, and the two ragged boundary pieces concatenate
to exactly the requested window. -/

section Generic

variable {σ : Type} (f : Fold σ)

private theorem foldl_reference (h : stitch f) (l : List (List Nat)) (acc : List Nat) :
    List.foldl f.join (f.reference acc) (l.map f.reference) =
      f.reference (acc ++ l.flatten) := by
  induction l generalizing acc with
  | nil => simp
  | cons p ps ih =>
      simp only [List.map_cons, List.foldl, List.flatten_cons]
      rw [h acc p, ih (acc ++ p)]
      simp only [List.append_assoc]

private theorem combine_reference (h : stitch f) (ps : List (List Nat)) :
    combine f ps = f.reference ps.flatten := by
  simpa [combine] using foldl_reference f h ps []

private theorem slice_concat (a p z : List Nat) (lo hi : Nat) (hlo : lo ≤ a.length) :
    a.drop lo ++ p ++ z.take hi =
      ((a ++ p ++ z).drop lo).take (a.length - lo + p.length + hi) := by
  rw [show a ++ p ++ z = a ++ (p ++ z) by simp only [List.append_assoc],
    List.drop_append_of_le_length hlo, List.take_append]
  have hd : (a.drop lo).length = a.length - lo := by simp
  rw [hd]
  have hN : a.length - lo + p.length + hi - (a.length - lo) = p.length + hi := by omega
  rw [hN, List.take_append]
  have hp : p.length + hi - p.length = hi := by omega
  rw [hp]
  have htakeA : List.take (a.length - lo + p.length + hi) (a.drop lo) = a.drop lo :=
    List.take_of_length_le (by simp [hd]; omega)
  have htakeP : List.take (p.length + hi) p = p :=
    List.take_of_length_le (by omega)
  rw [htakeA, htakeP]
  exact List.append_assoc _ _ _

private theorem frameSlice_of_stitch (h : stitch f) : frameSlice f := by
  intro a ps z lo hi hlo
  rw [combine_reference f h, h, h]
  refine congrArg f.reference ?_
  simpa only [List.append_assoc] using slice_concat a ps.flatten z lo hi hlo

end Generic

/-! Stitch for BitOccupancy: the length adds and folding the OR-of-bits update
over a concatenation just ORs together the two accumulated masks. -/

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

theorem sound_FrameSlice : BitOccupancy.FrameSlice.Goal := by
  exact frameSlice_of_stitch agg stitch_agg

end TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_BitOccupancy_FrameSlice
