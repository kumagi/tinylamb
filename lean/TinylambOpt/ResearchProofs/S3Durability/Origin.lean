import TinylambOpt.ResearchGoals.S3Durability.Origin

/-!
Proofs for the retained-origin goals. The walk consumes one key per step and
moves only forward, so a stored chain at the start can be peeled off one chunk
at a time, and keys below the starting key are never read.
-/

namespace TinylambOpt.ResearchProofs.S3Durability.Origin

open TinylambOpt.ResearchGoals.S3Durability.History
open TinylambOpt.ResearchGoals.S3Durability.Origin

/-- The walk from the start of a stored chain reads the chain, then the walk
from the key after it. -/
theorem walk_from_origin : WalkFromOrigin.Goal := by
  unfold WalkFromOrigin.Goal
  intro cs
  induction cs with
  | nil => intro st s lo fuel _ _; simp [nextLo]
  | cons c cs ih =>
    intro st s lo fuel hch hpl
    simp only [Chained] at hch
    simp only [Placed] at hpl
    obtain ⟨hlo, hch'⟩ := hch
    obtain ⟨hst, hpl'⟩ := hpl
    have h1 : walk (fuel + cs.length + 1) st s lo =
        c.recs ++ walk (fuel + cs.length) st (s + 1) (c.hi + 1) := by
      simp [walk, hst, hlo]
    have e1 : fuel + (c :: cs).length = fuel + cs.length + 1 := by
      simp only [List.length_cons]; omega
    have e2 : s + (c :: cs).length = s + 1 + cs.length := by
      simp only [List.length_cons]; omega
    rw [e1, e2, h1, ih st (s + 1) (c.hi + 1) fuel hch' hpl']
    simp [nextLo]

/-- The walk from `s` does not depend on keys below `s`. -/
theorem walk_ignores_below : WalkIgnoresBelow.Goal := by
  unfold WalkIgnoresBelow.Goal
  intro fuel
  induction fuel with
  | zero => intro st st' s lo _; simp [walk]
  | succ n ih =>
    intro st st' s lo h
    have hrest : ∀ j, s + 1 ≤ j → st' j = st j := fun j hj => h j (by omega)
    cases hx : st s with
    | none =>
      have hx' : st' s = none := by rw [h s (Nat.le_refl s), hx]
      simp [walk, hx, hx']
    | some x =>
      have hx' : st' s = some x := by rw [h s (Nat.le_refl s), hx]
      cases x with
      | terminal => simp [walk, hx, hx']
      | chunk c =>
        by_cases hl : c.lo = lo
        · simp [walk, hx, hx', hl, ih st st' (s + 1) (c.hi + 1) hrest]
        · simp [walk, hx, hx', hl]

end TinylambOpt.ResearchProofs.S3Durability.Origin
