import TinylambOpt.ResearchGoals.StorageExecution.DerivedZones

/-!
Scan pruning can exploit derived bounds only when every matching row remains covered by
the block summary.

Proved

- `WrappedSum.SkipRange` — Drop a whole page for a range test, preserving empty and
  NULL pages.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_WrappedSum_SkipRange

open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones
open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.WrappedSum

/-- The wrap-aware interval covers every score obtainable from the rectangle.
When the two corners lie in the same quotient block the residue is monotone
in the sum; otherwise the conservative interval `[0, t]` is used. -/
private theorem soundInterval_wrappedSum : soundInterval kernel := by
  intro t a b x y hx hy
  obtain ⟨ha1, ha2⟩ := hx
  obtain ⟨hb1, hb2⟩ := hy
  by_cases hq : (a.lo + b.lo) / (t + 1) = (a.hi + b.hi) / (t + 1)
  · have hb : kernel.bounds t a b =
        Bounds.mk ((a.lo + b.lo) % (t + 1)) ((a.hi + b.hi) % (t + 1)) := ite_eq_left hq
    have hs : kernel.score t (x, y) = (x + y) % (t + 1) := rfl
    rw [hb, hs]
    simp only [inside]
    have hl : (a.lo + b.lo) / (t + 1) ≤ (x + y) / (t + 1) :=
      Nat.div_le_div_right (by omega)
    have hr : (x + y) / (t + 1) ≤ (a.hi + b.hi) / (t + 1) :=
      Nat.div_le_div_right (by omega)
    have hqd : (x + y) / (t + 1) = (a.lo + b.lo) / (t + 1) := by omega
    have hq3 : ((x + y) / (t + 1)) * (t + 1) = ((a.lo + b.lo) / (t + 1)) * (t + 1) :=
      congrArg (fun z => z * (t + 1)) hqd
    have hq4 : ((a.hi + b.hi) / (t + 1)) * (t + 1) = ((a.lo + b.lo) / (t + 1)) * (t + 1) :=
      congrArg (fun z => z * (t + 1)) hq.symm
    have e1 : ((a.lo + b.lo) / (t + 1)) * (t + 1) + (a.lo + b.lo) % (t + 1) = a.lo + b.lo :=
      by rw [Nat.mul_comm, Nat.div_add_mod]
    have e2 : ((x + y) / (t + 1)) * (t + 1) + (x + y) % (t + 1) = x + y :=
      by rw [Nat.mul_comm, Nat.div_add_mod]
    have e3 : ((a.hi + b.hi) / (t + 1)) * (t + 1) + (a.hi + b.hi) % (t + 1) = a.hi + b.hi :=
      by rw [Nat.mul_comm, Nat.div_add_mod]
    have m1 : (a.lo + b.lo) % (t + 1) < t + 1 := Nat.mod_lt _ (Nat.succ_pos t)
    have m2 : (x + y) % (t + 1) < t + 1 := Nat.mod_lt _ (Nat.succ_pos t)
    have m3 : (a.hi + b.hi) % (t + 1) < t + 1 := Nat.mod_lt _ (Nat.succ_pos t)
    refine ⟨?_, ?_⟩ <;> omega
  · have hb : kernel.bounds t a b = Bounds.mk 0 t := ite_eq_right hq
    have hs : kernel.score t (x, y) = (x + y) % (t + 1) := rfl
    rw [hb, hs]
    simp only [inside]
    exact ⟨Nat.zero_le _, Nat.lt_succ_iff.mp (Nat.mod_lt _ (Nat.succ_pos t))⟩

/-- A certified interval justifies dropping a whole page for a range test. -/
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

theorem sound_SkipRange : WrappedSum.SkipRange.Goal :=
  skipRange_of_sound soundInterval_wrappedSum

end TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_WrappedSum_SkipRange
