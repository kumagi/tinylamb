import TinylambOpt.ResearchGoals.QueryRewrites.KeyRoutedJoinStates

/-!
# Goal_KeyRoutedJoinStates_VolumeNeutralDimensionRejection

Registers the proof of the VolumeNeutralDimensionRejection goal in KeyRoutedJoinStates: the modeled rewrite
preserves the exact result, so SQL execution may apply it.
-/
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace TinylambOpt.ResearchProofs.QueryRewrites.Goal_KeyRoutedJoinStates_VolumeNeutralDimensionRejection
open TinylambOpt.ResearchGoals.QueryRewrites
open TinylambOpt.ResearchGoals.QueryRewrites.KeyRoutedJoinStates.VolumeNeutralDimensionRejection

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

theorem sound : Goal := by
  intro rs factor peru
  simp only [Goal]
  apply Prod.ext
  · apply total_filter_zero
    intro r hr
    have hne : (factor r.1 != 0) = false := by
      revert hr; cases h : (factor r.1 != 0) <;> simp
    have h0 : factor r.1 = 0 := by simpa [bne_iff_ne, ne_eq] using hne
    simp [h0]
  · apply total_filter_zero
    intro r hr
    have hne : (factor r.1 != 0) = false := by
      revert hr; cases h : (factor r.1 != 0) <;> simp
    have h0 : factor r.1 = 0 := by simpa [bne_iff_ne, ne_eq] using hne
    simp [h0]

end TinylambOpt.ResearchProofs.QueryRewrites.Goal_KeyRoutedJoinStates_VolumeNeutralDimensionRejection
