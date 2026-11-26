import TinylambOpt.ResearchGoals.StorageExecution.DerivedZones

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_BucketSum_SkipRange

open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones
open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.BucketSum

/-! Small-input sanity check: whenever the certified interval of the bucket-sum
kernel cannot overlap the range test, a single covered row is not selected. -/
abbrev checkSmall : Prop :=
  ∀ (t : Fin 3) (al ah bl bh x y lo hi : Fin 5),
    al ≤ ah → bl ≤ bh → al ≤ x → x ≤ ah → bl ≤ y → y ≤ bh →
    allows (kernel.bounds (t : Nat) ⟨(al : Nat), (ah : Nat)⟩ ⟨(bl : Nat), (bh : Nat)⟩)
      (lo : Nat) (hi : Nat) = false →
    selected kernel (t : Nat) [some ((x : Nat), (y : Nat))] (lo : Nat) (hi : Nat) = []

#eval decide checkSmall
example : checkSmall := by native_decide

/-! The independently certified column floors/ceilings bound the bucket key:
integer division is monotone in the numerator. -/
private theorem soundBucketSum : soundInterval kernel := by
  intro t a b x y hax hby
  simp only [inside] at hax hby ⊢
  constructor
  · dsimp [kernel]
    exact Nat.div_le_div_right (Nat.add_le_add hax.1 hby.1)
  · dsimp [kernel]
    exact Nat.div_le_div_right (Nat.add_le_add hax.2 hby.2)

/-! Any kernel with a sound interval satisfies the page-skipping rule. -/
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

theorem sound_SkipRange : BucketSum.SkipRange.Goal :=
  skipRange_of_sound soundBucketSum

#guard selected kernel 0 [some (1, 8), none, some (8, 1)] 0 1 == []

end TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_BucketSum_SkipRange
