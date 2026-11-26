import TinylambOpt.ResearchGoals.QueryRewrites.KeyRoutedJoinStates

/-!
# Goal_KeyRoutedJoinStates_StarGroupSelectiveYearSlots

Within the six retained years, subtracting the first year gives unique bounded
slots: slot `i` holds exactly the rows of year `2011 + i`.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.QueryRewrites.Goal_KeyRoutedJoinStates_StarGroupSelectiveYearSlots

open TinylambOpt.ResearchGoals.QueryRewrites

theorem sound :
    TinylambOpt.ResearchGoals.QueryRewrites.KeyRoutedJoinStates.StarGroupSelectiveYearSlots.Goal := by
  intro sales i hi
  dsimp only [TinylambOpt.ResearchGoals.QueryRewrites.KeyRoutedJoinStates.StarGroupSelectiveYearSlots.Goal]
  induction sales with
  | nil => simp [forKey]
  | cons r rs ih =>
      rcases r with ⟨y, v⟩
      by_cases hy : 2011 ≤ y && y ≤ 2016
      · have hy' : 2011 ≤ y ∧ y ≤ 2016 := by simpa using hy
        have hshift : y - 2011 = i ↔ y = 2011 + i := by omega
        have hbeq : (y - 2011 == i) = (y == 2011 + i) := by
          by_cases h : y - 2011 = i
          · have heq : y = 2011 + i := hshift.mp h
            have h1 : (y - 2011 == i) = true := by simp [h]
            have h2 : (y == 2011 + i) = true := by simp [heq]
            rw [h1, h2]
          · have hn : y ≠ 2011 + i := by
              intro heq
              exact h (hshift.mpr heq)
            have h1 : (y - 2011 == i) = false := by simp [h]
            have h2 : (y == 2011 + i) = false := by simp [hn]
            rw [h1, h2]
        by_cases hyeq : (y == 2011 + i) = true
        · simp [hy, hbeq, hyeq, forKey]
          simpa [forKey] using ih
        · have hyeq' : (y == 2011 + i) = false := by simpa using hyeq
          simp [hy, hbeq, hyeq', forKey]
          simpa [forKey] using ih
      · have hy' : ¬ (2011 ≤ y ∧ y ≤ 2016) := by simpa using hy
        have hyne : y ≠ 2011 + i := by
          intro heq
          apply hy'
          constructor <;> omega
        have hright : (y == 2011 + i) = false := by simp [hyne]
        simp [hy, hright, forKey]
        simpa [forKey] using ih

end TinylambOpt.ResearchProofs.QueryRewrites.Goal_KeyRoutedJoinStates_StarGroupSelectiveYearSlots
