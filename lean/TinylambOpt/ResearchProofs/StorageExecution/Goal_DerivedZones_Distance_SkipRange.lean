import TinylambOpt.ResearchGoals.StorageExecution.DerivedZones

/-!
Scan pruning can exploit derived bounds only when every matching row remains covered by
the block summary.

Proved

- `Distance.SkipRange` — Drop a whole page for a range test, preserving empty and NULL
  pages.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_Distance_SkipRange

open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones
open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.Distance

/-- The certified lower bound does not exceed `gap x y`: truncated subtraction
is monotone in the minuend and antitone in the subtrahend. -/
private theorem lo_le_gap {a b : Bounds} {x y : Nat}
    (hxl : a.lo ≤ x) (hyh : y ≤ b.hi) (hyl : b.lo ≤ y) (hxh : x ≤ a.hi) :
    (kernel.bounds 0 a b).lo ≤ kernel.score 0 (x, y) := by
  dsimp [kernel, gapMin, gap]
  apply Nat.max_le.mpr
  constructor
  · have h1 : a.lo - b.hi ≤ x - b.hi := Nat.sub_le_sub_right hxl b.hi
    have h2 : x - b.hi ≤ x - y := Nat.sub_le_sub_left hyh x
    omega
  · have h1 : b.lo - a.hi ≤ y - a.hi := Nat.sub_le_sub_right hyl a.hi
    have h2 : y - a.hi ≤ y - x := Nat.sub_le_sub_left hxh y
    omega

/-- The certified upper bound is at least `gap x y`; split on which timestamp
is larger so that the gap collapses to a single subtraction. -/
private theorem gap_le_hi {a b : Bounds} {x y : Nat}
    (hxl : a.lo ≤ x) (hyh : y ≤ b.hi) (hyl : b.lo ≤ y) (hxh : x ≤ a.hi) :
    kernel.score 0 (x, y) ≤ (kernel.bounds 0 a b).hi := by
  dsimp [kernel, gapMax, gap]
  by_cases hxy : x ≤ y
  · have hzero : x - y = 0 := Nat.sub_eq_zero_of_le hxy
    rw [hzero, Nat.zero_add]
    have h1 : y - x ≤ y - a.lo := Nat.sub_le_sub_left hxl y
    have h2 : y - a.lo ≤ b.hi - a.lo := Nat.sub_le_sub_right hyh a.lo
    exact Nat.le_trans (Nat.le_trans h1 h2) (Nat.le_max_right _ _)
  · have hyx : y ≤ x := Nat.le_of_lt (Nat.not_le.mp hxy)
    have hzero : y - x = 0 := Nat.sub_eq_zero_of_le hyx
    rw [hzero, Nat.add_zero]
    have h1 : x - y ≤ x - b.lo := Nat.sub_le_sub_left hyl x
    have h2 : x - b.lo ≤ a.hi - b.lo := Nat.sub_le_sub_right hxh b.lo
    exact Nat.le_trans (Nat.le_trans h1 h2) (Nat.le_max_left _ _)

/-- Independent column intervals certify an interval for the time skew. -/
private theorem soundDistance : soundInterval kernel := by
  intro t a b x y hax hby
  simp only [inside] at hax hby ⊢
  exact ⟨lo_le_gap hax.1 hby.2 hby.1 hax.2, gap_le_hi hax.1 hby.2 hby.1 hax.2⟩

/-- A certified scalar interval justifies dropping a page for a range test. -/
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

theorem sound_SkipRange : Distance.SkipRange.Goal :=
  skipRange_of_sound soundDistance

#guard selected kernel 0 [some (1, 8), none, some (8, 1)] 0 3 == []

end TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_Distance_SkipRange
