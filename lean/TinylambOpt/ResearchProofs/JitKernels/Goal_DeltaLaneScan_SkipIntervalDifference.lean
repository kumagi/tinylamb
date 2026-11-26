import TinylambOpt.ResearchGoals.JitKernels.DeltaLaneScan

/-! The difference between two materialized rows is the sum of the deltas
strictly between them. Splitting the row prefix at the lower row gives exactly
that intervening slice, so SQL can compute the difference without decoding
either row in full. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.DeltaLaneScan.Goal_SkipIntervalDifference

open TinylambOpt.ResearchGoals.JitKernels.DeltaLaneScan

theorem sound : SkipIntervalDifference.Goal := by
  intro b ds i j hij hj
  have h1 : i + (j - i) = j := Nat.add_sub_of_le hij
  have hsumij : i + 1 + (j - i) = j + 1 := by omega
  have hsplit : ds.take (j + 1) = ds.take (i + 1) ++ (ds.drop (i + 1)).take (j - i) := by
    symm
    calc
      ds.take (i + 1) ++ (ds.drop (i + 1)).take (j - i)
          = ds.take (i + 1) ++ (ds.take ((i + 1) + (j - i))).drop (i + 1) := by
              rw [List.drop_take]
              rw [Nat.add_sub_cancel_left]
      _ = ds.take (i + 1) ++ (ds.take (j + 1)).drop (i + 1) := by rw [hsumij]
      _ = (ds.take (j + 1)).take (i + 1) ++ (ds.take (j + 1)).drop (i + 1) := by
              rw [List.take_take]
              rw [Nat.min_eq_left (by omega)]
      _ = ds.take (j + 1) := by rw [List.take_append_drop]
  unfold TinylambOpt.ResearchGoals.JitKernels.DeltaLaneScan.get
  rw [hsplit, List.sum_append]
  let p : Int := (ds.take (i + 1)).sum
  let s : Int := ((ds.drop (i + 1)).take (j - i)).sum
  change b + (p + s) - (b + p) = s
  omega

end TinylambOpt.ResearchProofs.JitKernels.DeltaLaneScan.Goal_SkipIntervalDifference
