import TinylambOpt.ResearchGoals.JitKernels.DeltaLaneScan

/-!
A morsel-boundary continuation needs only one carry value: decoding the
concatenation `a ++ c` from base `b` equals decoding `a` from `b` followed by
decoding `c` from the final accumulator `b + a.sum`:
`values b (a ++ c) = values b a ++ values (b + a.sum) c`. Cross-chunk SIMD
prefix scans may therefore be restarted from a single carried scalar.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.JitKernels.DeltaLaneScan_HorizontalScanChunkSeed
open TinylambOpt.ResearchGoals.JitKernels.DeltaLaneScan

private theorem scanl_head (acc : Int) (l : List Int) :
    l.scanl (·+·) acc = acc :: (l.scanl (·+·) acc).drop 1 := by
  induction l with
  | nil => rfl
  | cons d t ih => rw [List.scanl_cons]; rfl

private theorem values_cons (b d : Int) (t : List Int) :
    values b (d::t) = (b+d) :: values (b+d) t := by
  unfold values
  rw [List.scanl_cons]
  rw [show List.drop 1 (b :: List.scanl (·+·) (b+d) t) = List.scanl (·+·) (b+d) t from rfl,
    ← scanl_head]

theorem sound : HorizontalScanChunkSeed.Goal := by
  intro b a
  induction a generalizing b with
  | nil => intro c; simp [values]
  | cons d t ih =>
      intro c
      rw [show (d :: t) ++ c = d :: (t ++ c) from rfl]
      rw [values_cons, ih, values_cons, List.cons_append, List.sum_cons, Int.add_assoc]

end TinylambOpt.ResearchProofs.JitKernels.DeltaLaneScan_HorizontalScanChunkSeed
