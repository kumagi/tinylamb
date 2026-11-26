import TinylambOpt.ResearchGoals.JitKernels.DeltaLaneScan

/-!
Inside a run of `n` identical deltas `d`, row `i` can be addressed in constant
time without any prefix scan: `get b (replicate n d) i = b + (i+1)*d`.
Direct-select addressing of uniform delta runs is therefore exact for every
selected index below the run length.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.JitKernels.DeltaLaneScan_RepeatedDeltaRunDirectSelect
open TinylambOpt.ResearchGoals.JitKernels.DeltaLaneScan

private theorem sum_rep (n : Nat) (d : Int) : (List.replicate n d).sum = (n:Int)*d := by
  induction n with
  | zero => simp
  | succ n ih =>
      rw [List.replicate_succ, List.sum_cons, ih]
      rw [show ((n+1 : Nat) : Int) = (n : Int) + 1 from by simp]
      rw [Int.add_mul, Int.one_mul]
      omega

theorem sound : RepeatedDeltaRunDirectSelect.Goal := by
  intro b d n i hi
  unfold TinylambOpt.ResearchGoals.JitKernels.DeltaLaneScan.get
  rw [List.take_replicate]
  rw [show min (i + 1) n = i + 1 from Nat.min_eq_left (by omega : i + 1 ≤ n)]
  rw [sum_rep]

end TinylambOpt.ResearchProofs.JitKernels.DeltaLaneScan_RepeatedDeltaRunDirectSelect
