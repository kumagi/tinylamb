import TinylambOpt.ResearchGoals.QueryRewrites.KeyRoutedJoinStates

namespace TinylambOpt.ResearchProofs.Batch5.QueryRewrites.KeyRoutedJoinStates.Goal_VolumeNeutralDimensionRejection

open TinylambOpt.ResearchGoals.QueryRewrites.KeyRoutedJoinStates


/-! Rejecting keys with a zero volume factor may be pushed into both volume
    sums, since every rejected row contributes exactly zero to either sum. The
    executor may therefore filter those keys before either country-masked or
    unmasked volume aggregation. -/

open TinylambOpt.ResearchGoals.QueryRewrites

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.KeyRoutedJoinStates.VolumeNeutralDimensionRejection.Goal := by
  intro rs factor peru
  dsimp only
  unfold total
  congr 1
  · induction rs with
    | nil => simp
    | cons r rs ih =>
        by_cases hz : factor r.1 = 0
        · simp [hz, ih]
        · simp [hz, ih]
  · induction rs with
    | nil => simp
    | cons r rs ih =>
        by_cases hz : factor r.1 = 0
        · simp [hz, ih]
        · simp [hz, ih]
end TinylambOpt.ResearchProofs.Batch5.QueryRewrites.KeyRoutedJoinStates.Goal_VolumeNeutralDimensionRejection
