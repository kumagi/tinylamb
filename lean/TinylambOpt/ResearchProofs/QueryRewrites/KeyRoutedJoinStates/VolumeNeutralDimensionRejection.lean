import TinylambOpt.ResearchGoals.QueryRewrites.KeyRoutedJoinStates

/-!
# VolumeNeutralDimensionRejection

A rejected key contributes zero to both volume states; guarding the country
lookup by exact join membership preserves both sums.
-/

set_option maxHeartbeats 0

namespace TinylambOpt.ResearchProofs.QueryRewrites.KeyRoutedJoinStates.VolumeNeutralDimensionRejection
open TinylambOpt.ResearchGoals.QueryRewrites

private theorem total_filter_zero {α : Type} (xs : List α) (p : α → Bool) (f : α → Int)
    (hz : ∀ a, p a = false → f a = 0) :
    total (xs.filter p) f = total xs f := by
  simp only [total]
  induction xs with
  | nil => simp
  | cons x xs ih =>
      by_cases hp : p x = true
      · simp [hp, ih]
      · have hp' : p x = false := by cases h : p x <;> simp_all
        have hf : f x = 0 := hz x hp'
        simp [hp', hf, ih]

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.KeyRoutedJoinStates.VolumeNeutralDimensionRejection.Goal := by
  intro rs factor peru
  dsimp only [TinylambOpt.ResearchGoals.QueryRewrites.KeyRoutedJoinStates.VolumeNeutralDimensionRejection.Goal]
  apply Prod.ext
  · apply total_filter_zero
    intro r hr
    have h0 : factor r.1 = 0 := by
      revert hr; simp [bne]
    simp [h0]
  · apply total_filter_zero
    intro r hr
    have h0 : factor r.1 = 0 := by
      revert hr; simp [bne]
    simp [h0]

end TinylambOpt.ResearchProofs.QueryRewrites.KeyRoutedJoinStates.VolumeNeutralDimensionRejection

#print axioms TinylambOpt.ResearchProofs.QueryRewrites.KeyRoutedJoinStates.VolumeNeutralDimensionRejection.sound
