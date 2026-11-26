import TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_BalanceFloor_FrameSlice

open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters
open TinylambOpt.ResearchGoals.StorageExecution.OrderedFooters.BalanceFloor

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

/-! Stitch for the BalanceFloor fold. The reference state is the total
balance plus the minimum prefix balance. Writing
`mpFrom s l := min over prefixes of l, starting from s` gives
`reference xs = (sum steps, mpFrom 0 (steps xs))`, and `mpFrom` satisfies a
shift law and an append law; those imply the join law. -/

private def mpFrom : Int → List Int → Int
  | s, [] => s
  | s, x :: xs => min s (mpFrom (s + x) xs)

private theorem mpFrom_nonpos (l : List Int) (s : Int) : mpFrom s l ≤ s := by
  induction l generalizing s with
  | nil => simp [mpFrom]
  | cons x xs ih => simp only [mpFrom]; omega

private theorem mpFrom_shift (l : List Int) (s : Int) : mpFrom s l = min s (s + mpFrom 0 l) := by
  induction l generalizing s with
  | nil => simp [mpFrom]
  | cons x xs ih =>
      simp only [mpFrom]
      simp at ih ⊢
      rw [ih (s + x), ih x]
      have hp := mpFrom_nonpos xs 0
      omega

private theorem mpFrom_append (s : Int) (a b : List Int) :
    mpFrom s (a ++ b) = min (mpFrom s a) (mpFrom (s + a.sum) b) := by
  induction a generalizing s with
  | nil =>
      simp only [List.nil_append, List.sum_nil]
      rw [show s + 0 = s by omega]
      have hp := mpFrom_nonpos b s
      change mpFrom s b = min s (mpFrom s b)
      omega
  | cons x xs ih =>
      simp only [List.cons_append, List.sum_cons, mpFrom]
      rw [ih (s + x)]
      rw [show s + (x + xs.sum) = s + x + xs.sum by omega]
      omega

private theorem M_eq_mp (l : List Int) (s : Int) :
    (l.scanl (· + ·) s).min?.getD s = mpFrom s l := by
  induction l generalizing s with
  | nil => simp [List.scanl_nil, mpFrom]
  | cons x xs ih =>
      rw [List.scanl_cons]
      cases h : (xs.scanl (· + ·) (s + x)).min? with
      | none =>
          have hempty := List.min?_eq_none_iff.mp h
          have hne : xs.scanl (· + ·) (s + x) ≠ [] := by
            cases xs <;> simp [List.scanl_cons, List.scanl_nil]
          contradiction
      | some v =>
          have hi := ih (s + x)
          simp [List.min?_cons, h] at hi ⊢
          rw [hi]
          rfl

private def steps (xs : List Nat) : List Int :=
  xs.map (fun x => if x % 2 == 0 then ((x / 2 : Nat) : Int) else -((x / 2 + 1 : Nat) : Int))

private theorem reference_eq (xs : List Nat) :
    agg.reference xs = ((steps xs).sum, mpFrom 0 (steps xs)) := by
  simp only [agg, steps, M_eq_mp]

private theorem join_second (sa sb : List Int) :
    min (mpFrom 0 sa) (sa.sum + mpFrom 0 sb) = mpFrom 0 (sa ++ sb) := by
  rw [mpFrom_append]
  rw [show 0 + sa.sum = sa.sum by omega]
  rw [mpFrom_shift sb sa.sum]
  have hp := mpFrom_nonpos sb 0
  omega

private theorem stitch_agg : stitch agg := by
  intro a b
  rw [reference_eq, reference_eq, reference_eq]
  dsimp [agg]
  refine Prod.ext ?_ ?_
  · simp [steps, List.map_append, List.sum_append]
  · simp only [steps, List.map_append]
    exact join_second (steps a) (steps b)

theorem sound_FrameSlice : FrameSlice.Goal := by
  exact frameSlice_of_stitch agg stitch_agg

end TinylambOpt.ResearchProofs.StorageExecution.Goal_OrderedFooters_BalanceFloor_FrameSlice
