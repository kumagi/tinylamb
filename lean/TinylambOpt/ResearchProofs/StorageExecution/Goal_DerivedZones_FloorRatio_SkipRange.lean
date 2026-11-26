import TinylambOpt.ResearchGoals.StorageExecution.DerivedZones

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_FloorRatio_SkipRange

open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones
open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.FloorRatio

/-- Independent column min/max certify an interval for the scaled ratio:
    division is monotone in the numerator and antitone in the denominator. -/
private theorem soundInterval_floorRatio : soundInterval kernel := by
  intro t a b x y hax hby
  simp only [inside] at hax hby ⊢
  have hdenLo : y + 1 ≤ b.hi + 1 := by omega
  have hdenHi : b.lo + 1 ≤ y + 1 := by omega
  constructor
  · have hnum : a.lo * t ≤ x * t := Nat.mul_le_mul_right t hax.1
    exact Nat.div_le_div hnum hdenLo (by omega)
  · have hnum : x * t ≤ a.hi * t := Nat.mul_le_mul_right t hax.2
    exact Nat.div_le_div hnum hdenHi (by omega)

/-- A sound scalar interval justifies dropping the entire page for a range test. -/
private theorem skipRange_of_sound {f : Kernel} (hs : soundInterval f) : skipRange f := by
  intro t p a b lo hi hc hn
  unfold selected
  apply List.filter_eq_nil_iff.mpr
  intro v hv hpass
  have hpred : (lo ≤ v && v < hi) = true := hpass
  simp only [Bool.and_eq_true, decide_eq_true_eq] at hpred
  rcases hpred with ⟨hlo, hhi'⟩
  rcases List.mem_filterMap.mp hv with ⟨r, hr, hmap⟩
  rcases Option.map_eq_some_iff.mp hmap with ⟨xy, hrxy, hscore⟩
  rcases xy with ⟨x, y⟩
  have hcov := hc r hr x y hrxy
  have hb : inside (f.bounds t a b) (f.score t (x, y)) :=
    hs t a b x y hcov.1 hcov.2
  simp only [inside] at hb
  have hl : lo ≤ (f.bounds t a b).hi :=
    Nat.le_trans hlo (by simpa [hscore] using hb.2)
  have hr' : (f.bounds t a b).lo < hi :=
    Nat.lt_of_le_of_lt (by simpa [hscore] using hb.1) hhi'
  have hboth : lo < hi := Nat.lt_of_le_of_lt hlo hhi'
  have hallows : allows (f.bounds t a b) lo hi = true := by
    simp [allows, hl, hr', hboth]
  rw [hallows] at hn
  cases hn

theorem sound_SkipRange : FloorRatio.SkipRange.Goal :=
  skipRange_of_sound soundInterval_floorRatio

#guard selected kernel 2 [some (1, 2), none, some (3, 5)] 5 10 == []
#guard allows (kernel.bounds 2 ⟨1, 3⟩ ⟨2, 5⟩) 5 10 == false

end TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_FloorRatio_SkipRange
