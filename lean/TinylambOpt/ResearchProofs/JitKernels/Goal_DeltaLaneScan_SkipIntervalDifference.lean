import TinylambOpt.ResearchGoals.JitKernels.DeltaLaneScan

/-! A selected row difference is read directly from the intervening delta slice.
    The proof only re-associates a prefix sum of signed deltas, so the compiled
    kernel may skip decoding every row between the two ordinals. -/

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_DeltaLaneScan_SkipIntervalDifference

open TinylambOpt.ResearchGoals.JitKernels.DeltaLaneScan

theorem sound : TinylambOpt.ResearchGoals.JitKernels.DeltaLaneScan.SkipIntervalDifference.Goal := by
  intro b ds i j hij hj
  have hi_len : i + 1 ≤ ds.length := by omega
  have htake : ds.take (j + 1) = ds.take (i + 1) ++ (ds.drop (i + 1)).take (j - i) := by
    calc
      ds.take (j + 1) = (ds.take (i + 1) ++ ds.drop (i + 1)).take (j + 1) := by
        rw [List.take_append_drop]
      _ = (ds.take (i + 1)).take (j + 1) ++
          (ds.drop (i + 1)).take (j + 1 - (ds.take (i + 1)).length) := by
        rw [List.take_append]
      _ = ds.take (i + 1) ++ (ds.drop (i + 1)).take (j - i) := by
        rw [List.take_take]
        have hmin : min (j + 1) (i + 1) = i + 1 := Nat.min_eq_right (by omega)
        rw [hmin]
        have hlen : (ds.take (i + 1)).length = i + 1 := by
          rw [List.length_take, Nat.min_eq_left hi_len]
        have hsub : j + 1 - (ds.take (i + 1)).length = j - i := by
          rw [hlen]
          omega
        rw [hsub]
  unfold TinylambOpt.ResearchGoals.JitKernels.DeltaLaneScan.get
  rw [htake, List.sum_append]
  omega

end TinylambOpt.ResearchProofs.JitKernels.Goal_DeltaLaneScan_SkipIntervalDifference
