import TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters

/-!
A footer can guide sorted scans and key-range probes when its indexed summary is
consistent with the stored blocks.

Proved

- `IndexWeighted.FrameSlice` — Use only the two ragged boundaries and whole-page
  summaries for a window.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_IndexWeighted_FrameSlice

open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters
open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.IndexWeighted

/-! Ground check on small inputs (both probes must return `true`). -/

private def gen : Nat → List (List Nat)
  | 0 => [[]]
  | n + 1 => let p := gen n; p ++ p.flatMap (fun xs => [0 :: xs, 1 :: xs, 2 :: xs])

private def checkOne (a p z : List Nat) : Bool :=
  let ps := [p, a]
  (List.range (a.length + 1)).all (fun lo =>
    (List.range 4).all (fun hi =>
      agg.join (agg.reference (a.drop lo))
          (agg.join (combine agg ps) (agg.reference (z.take hi))) ==
        agg.reference (((a ++ ps.flatten ++ z).drop lo).take
          (a.length - lo + ps.flatten.length + hi))))

private def probe (n : Nat) : Bool :=
  let ls := gen n
  ls.all (fun a => ls.all (fun p => ls.all (fun z => checkOne a p z)))

#eval probe 2
#eval probe 3

/-! FrameSlice is a generic consequence of stitch: the page summaries fold
into the summary of their concatenation, and the two ragged boundary pieces
concatenate to exactly the requested window. -/

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

/-! Stitch for the IndexWeighted fold. Its third component is the moment
`momentFrom 0 xs := Σ i * xs[i]`; `zipIdx` shift and append give the shift law
`momentFrom k xs = momentFrom 0 xs + k * xs.sum`, from which the join law
follows immediately. -/

private def momentFrom (k : Nat) (xs : List Nat) : Nat :=
  (xs.zipIdx k).map (fun (v, i) => i * v) |>.sum

private theorem momentFrom_cons (k : Nat) (x : Nat) (xs : List Nat) :
    momentFrom k (x :: xs) = k * x + momentFrom (k + 1) xs := by
  simp [momentFrom, List.zipIdx_cons]

private theorem momentFrom_shift (k : Nat) (xs : List Nat) :
    momentFrom k xs = momentFrom 0 xs + k * xs.sum := by
  induction xs generalizing k with
  | nil => simp [momentFrom]
  | cons x xs ih =>
      rw [momentFrom_cons (k := k), momentFrom_cons (k := 0)]
      rw [show 0 + 1 = 1 by omega]
      rw [ih (k + 1), ih 1, List.sum_cons]
      simp only [Nat.add_mul, Nat.mul_add, Nat.one_mul, Nat.zero_mul, Nat.zero_add]
      omega

private theorem momentFrom_append (a b : List Nat) :
    momentFrom 0 (a ++ b) = momentFrom 0 a + momentFrom 0 b + a.length * b.sum := by
  rw [momentFrom, List.zipIdx_append, List.map_append, List.sum_append]
  change momentFrom 0 a + momentFrom (0 + a.length) b =
    momentFrom 0 a + momentFrom 0 b + a.length * b.sum
  rw [Nat.zero_add, momentFrom_shift (k := a.length) (xs := b)]
  omega

private theorem stitch_agg : stitch agg := by
  intro a b
  dsimp [agg]
  refine Prod.ext ?_ ?_
  · simp
  · refine Prod.ext ?_ ?_
    · simp [List.sum_append]
    · simpa [momentFrom] using (momentFrom_append a b).symm

theorem sound_FrameSlice : FrameSlice.Goal := by
  exact frameSlice_of_stitch agg stitch_agg

end TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_IndexWeighted_FrameSlice
