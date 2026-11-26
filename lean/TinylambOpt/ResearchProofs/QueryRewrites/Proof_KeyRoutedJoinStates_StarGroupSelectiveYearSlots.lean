import TinylambOpt.ResearchGoals.QueryRewrites.KeyRoutedJoinStates

namespace TinylambOpt.ResearchProofs.QueryRewrites.KeyRoutedJoinStates.Goal_StarGroupSelectiveYearSlots

open TinylambOpt.ResearchGoals.QueryRewrites.KeyRoutedJoinStates


/-! Subtracting the base year maps the retained years bijectively onto the
    bounded slot indices. Each sale in the range lands in its own slot, and a
    sale outside the range cannot match the corresponding original year, so the
    nullable revenue sequence in each slot is unchanged. -/

open TinylambOpt.ResearchGoals.QueryRewrites

theorem sound : TinylambOpt.ResearchGoals.QueryRewrites.KeyRoutedJoinStates.StarGroupSelectiveYearSlots.Goal := by
  intro sales i hi
  dsimp only
  unfold forKey
  induction sales with
  | nil => simp
  | cons r rs ih =>
      rcases r with ⟨y, v⟩
      by_cases hy : 2011 ≤ y ∧ y ≤ 2016
      · have hrange : (decide (2011 ≤ y) && decide (y ≤ 2016)) = true := by simp [hy]
        by_cases hsub : y - 2011 = i
        · have heq : y = 2011 + i := by omega
          subst y
          have hle : 2011 + i ≤ 2016 := by omega
          have hslot : (2011 + i - 2011 == i) = true := by simp [hsub]
          simp [hrange, hle, hslot, ih]
        · have hne : y ≠ 2011 + i := by omega
          have hslot : (y - 2011 == i) = false := by simp [hsub]
          have hkey : (y == 2011 + i) = false := by simp [hne]
          simp [hrange, hslot, hkey, ih]
      · have hrange : (decide (2011 ≤ y) && decide (y ≤ 2016)) = false := by
          by_cases h1 : 2011 ≤ y
          · have h2 : ¬ y ≤ 2016 := (not_and.mp hy) h1
            simp [h1, h2]
          · simp [h1]
        have hne : y ≠ 2011 + i := by
          intro heq
          apply hy
          constructor <;> omega
        have hkey : (y == 2011 + i) = false := by simp [hne]
        simp [hrange, hkey, ih]
end TinylambOpt.ResearchProofs.QueryRewrites.KeyRoutedJoinStates.Goal_StarGroupSelectiveYearSlots
