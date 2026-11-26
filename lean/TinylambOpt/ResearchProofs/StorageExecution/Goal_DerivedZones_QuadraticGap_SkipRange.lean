import TinylambOpt.ResearchGoals.StorageExecution.DerivedZones

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_QuadraticGap_SkipRange

open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones
open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.QuadraticGap

/-- Squaring is monotone on naturals, so the separated-interval bounds certify
    an interval for `(gap x y)^2 + t` whenever `x` and `y` lie in their
    respective column intervals. -/
private theorem soundInterval_quadraticGap : soundInterval kernel := by
  intro t a b x y hax hby
  rcases a with ⟨alo, ahi⟩
  rcases b with ⟨blo, bhi⟩
  simp only [inside] at hax hby
  simp only [kernel, gapMin, gapMax, gap]
  have hgapMin : max (alo - bhi) (blo - ahi) ≤ (x - y) + (y - x) := by
    apply Nat.max_le.mpr
    constructor <;> omega
  have hgapMax : (x - y) + (y - x) ≤ max (ahi - blo) (bhi - alo) := by
    by_cases hyx : y ≤ x
    · have hzero : y - x = 0 := Nat.sub_eq_zero_of_le hyx
      rw [hzero, Nat.add_zero]
      exact Nat.le_trans (by omega : x - y ≤ ahi - blo) (Nat.le_max_left _ _)
    · have hxy : x ≤ y := by omega
      have hzero : x - y = 0 := Nat.sub_eq_zero_of_le hxy
      rw [hzero, Nat.zero_add]
      exact Nat.le_trans (by omega : y - x ≤ bhi - alo) (Nat.le_max_right _ _)
  constructor
  · exact Nat.add_le_add_right (Nat.pow_le_pow_left hgapMin 2) t
  · exact Nat.add_le_add_right (Nat.pow_le_pow_left hgapMax 2) t

/-- Every score that passes the range predicate forces the certified interval
    to overlap the requested range, so a disallowed footer cannot skip a page. -/
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

theorem sound_SkipRange : QuadraticGap.SkipRange.Goal :=
  skipRange_of_sound soundInterval_quadraticGap

#guard selected kernel 0 [some (1, 8), none, some (8, 1)] 50 100 == []

end TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_QuadraticGap_SkipRange
