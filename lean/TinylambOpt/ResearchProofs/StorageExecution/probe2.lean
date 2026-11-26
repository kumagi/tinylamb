import TinylambOpt.ResearchGoals.StorageExecution.DerivedZones
set_option maxHeartbeats 0
open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones
#check @Product.SoundInterval.Goal
#check @gap
#check @gapMin
#check @covers
theorem t1 : Product.SoundInterval.Goal := by
  intro t a b x y hx hy
  obtain ⟨ha1, ha2⟩ := hx
  obtain ⟨hb1, hb2⟩ := hy
  show a.lo * b.lo ≤ x * y ∧ x * y ≤ a.hi * b.hi
  exact ⟨Nat.mul_le_mul ha1 hb1, Nat.mul_le_mul ha2 hb2⟩
theorem t2 : Distance.SoundInterval.Goal := by
  intro t a b x y hx hy
  obtain ⟨ha1, ha2⟩ := hx
  obtain ⟨hb1, hb2⟩ := hy
  show max (a.lo - b.hi) (b.lo - a.hi) ≤ (x - y) + (y - x) ∧
       (x - y) + (y - x) ≤ max (a.hi - b.lo) (b.hi - a.lo)
  omega
theorem t3 : SaturatingLoss.SoundInterval.Goal := by
  intro t a b x y hx hy
  obtain ⟨ha1, ha2⟩ := hx
  obtain ⟨hb1, hb2⟩ := hy
  show a.lo - (b.hi + t) ≤ x - (y + t) ∧ x - (y + t) ≤ a.hi - (b.lo + t)
  omega
