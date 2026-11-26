import TinylambOpt.ResearchGoals.JitKernels.ControlByteProbe

/-! A negative fingerprint ballot over the whole control array rules out every
exact key match under coherence. No payload read is needed: the probe result is
`none`, so SQL can skip the payload page entirely. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_ControlByteProbe_TagNegativeSkipsPayloadPage

open TinylambOpt.ResearchGoals.JitKernels.ControlByteProbe

theorem findSomeEqNoneMem {α β : Type} (f : α → Option β) (xs : List α)
    (h : ∀ a ∈ xs, f a = none) :
    ∀ (ys : List α), (∀ a ∈ ys, a ∈ xs) → ys.findSome? f = none := by
  intro ys hys
  induction ys with
  | nil => simp
  | cons a ys ih =>
      have ha : a ∈ xs := hys a (by simp)
      have hy : ∀ b ∈ ys, b ∈ xs := by
        intro b hb
        exact hys b (by simp [hb])
      simp [h a ha, ih hy]

theorem takeWhileMem (p : Slot → Bool) :
    ∀ (xs : List Slot) (a : Slot), a ∈ xs.takeWhile p → a ∈ xs := by
  intro xs
  induction xs with
  | nil => simp
  | cons x xs ih =>
      intro a ha
      by_cases hp : p x = true
      · simp [hp] at ha
        rcases ha with rfl | ha
        · simp
        · simp [ih a ha]
      · simp [hp] at ha

theorem sound : TinylambOpt.ResearchGoals.JitKernels.ControlByteProbe.TagNegativeSkipsPayloadPage.Goal := by
  intro xs f k hc hneg
  have hno : ¬ ∃ s ∈ xs, tagHit (f k) s = true := by
    intro hex
    rcases hex with ⟨s, hs, ht⟩
    have hany : xs.any (tagHit (f k)) = true := List.any_eq_true.mpr ⟨s, hs, ht⟩
    rw [hneg] at hany
    contradiction
  have hval : ∀ s ∈ xs, value k s = none := by
    intro s hs
    have hsneg : tagHit (f k) s = false := by
      cases htag : tagHit (f k) s <;> simp_all
    cases s with
    | empty => simp [value]
    | tomb => simp [value]
    | live t kt v =>
        by_cases hkeq : k = kt
        · subst kt
          have ht : t = f k := hc t k v hs
          have hhit : tagHit (f k) (.live t k v) = true := by simp [tagHit, ht]
          simp [hsneg] at hhit
        · simp [value, hkeq]
  unfold probe
  exact findSomeEqNoneMem (value k) xs hval (untilEmpty xs) (takeWhileMem _ xs)

end TinylambOpt.ResearchProofs.JitKernels.Goal_ControlByteProbe_TagNegativeSkipsPayloadPage