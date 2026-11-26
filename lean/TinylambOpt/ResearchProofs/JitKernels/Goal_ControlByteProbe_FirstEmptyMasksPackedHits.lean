import TinylambOpt.ResearchGoals.JitKernels.ControlByteProbe

/-! Under tag coherence every slot whose payload is returned by an exact key
comparison also passes the fingerprint ballot. Filtering the probe prefix by
the fingerprint therefore cannot remove the answer: SQL may narrow candidate
payload loads to the tag hits without changing the probed value. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.ControlByteProbe.Goal_FirstEmptyMasksPackedHits

open TinylambOpt.ResearchGoals.JitKernels.ControlByteProbe

/-- Filtering a list by a predicate that is true for every lane with a value
does not change the first value found. -/
theorem findSomeFilterOfSomeKeptMem {α β : Type} (f : α → Option β) (p : α → Bool)
    (xs : List α) (h : ∀ a ∈ xs, (f a).isSome → p a = true) :
    ∀ (ys : List α), (∀ a ∈ ys, a ∈ xs) → (ys.filter p).findSome? f = ys.findSome? f := by
  intro ys hys
  induction ys with
  | nil => simp
  | cons a ys ih =>
      have ha : a ∈ xs := hys a (by simp)
      have hy : ∀ b ∈ ys, b ∈ xs := by
        intro b hb
        exact hys b (by simp [hb])
      cases hfa : f a with
      | none =>
          by_cases hp : p a = true <;> simp [hfa, hp, ih hy]
      | some b =>
          have hp : p a = true := h a ha (by simp [hfa])
          simp [hfa, hp]

/-- Every element of a `takeWhile` prefix belongs to the original list. -/
theorem takeWhile_mem {α : Type} (p : α → Bool) :
    ∀ (xs : List α) (a : α), a ∈ xs.takeWhile p → a ∈ xs := by
  intro xs
  induction xs with
  | nil => simp
  | cons x xs ih =>
      intro a ha
      by_cases hp : p x = true
      · simp [hp] at ha
        rcases ha with rfl | ha
        · simp
        · exact by simp [ih a ha]
      · simp [hp] at ha

theorem sound : FirstEmptyMasksPackedHits.Goal := by
  intro xs k f hc
  unfold probe
  apply Eq.symm
  apply findSomeFilterOfSomeKeptMem (value k) (tagHit (f k)) xs
  · intro s hs hv
    cases s with
    | empty => simp [value] at hv
    | tomb => simp [value] at hv
    | live t kt v =>
        simp only [value] at hv
        split at hv
        · rename_i hkey
          have hkeq : k = kt := by simpa using hkey
          have ht : t = f kt := hc t kt v hs
          subst kt
          simp [tagHit, ht]
        · simp at hv
  · exact takeWhile_mem (fun s : Slot => s != .empty) xs

end TinylambOpt.ResearchProofs.JitKernels.ControlByteProbe.Goal_FirstEmptyMasksPackedHits
