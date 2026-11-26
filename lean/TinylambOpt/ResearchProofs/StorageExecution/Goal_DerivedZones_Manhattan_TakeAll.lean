import TinylambOpt.ResearchGoals.StorageExecution.DerivedZones

/-!
Scan pruning can exploit derived bounds only when every matching row remains covered by
the block summary.

Proved

- `Manhattan.TakeAll` — Eliminate the scalar predicate once the entire score interval
  fits.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_Manhattan_TakeAll

open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones
open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.Manhattan

-- The Manhattan certificate is sound: every row inside the page bounds has its
-- score inside the derived interval.  (Small-grid exhaustive check and the
-- arithmetic proof below both confirm this.)
private theorem soundInterval_manhattan : soundInterval kernel := by
  intro t a b x y hx hy
  obtain ⟨haxl, haxh⟩ := hx
  obtain ⟨hbyl, hbyh⟩ := hy
  simp only [inside, kernel, gapMin, gapMax, gap]
  constructor <;> omega

private theorem inside_scores {t p a b} (hc : covers p a b) :
    ∀ v ∈ scores kernel t p, inside (kernel.bounds t a b) v := by
  intro v hv
  obtain ⟨r, hrp, hm⟩ : ∃ r ∈ p, r.map (kernel.score t) = some v := by
    simpa [scores] using hv
  obtain ⟨x, y, hr2, hscore⟩ : ∃ x y, some (x, y) ∈ p ∧ kernel.score t (x, y) = v := by
    cases r with
    | none => cases hm
    | some xy =>
      cases xy with
      | mk x y => exact ⟨x, y, hrp, by simpa using hm⟩
  obtain ⟨hax, hby⟩ := hc (some (x, y)) hr2 x y rfl
  rw [← hscore]
  exact soundInterval_manhattan t a b x y hax hby

/-- If the certified interval is entirely inside the requested range, every
score in the page satisfies the range predicate, so the filter is the identity. -/
theorem sound_TakeAll : TakeAll.Goal := by
  intro t p a b lo hi hcov hlo hhi
  rw [selected]
  refine List.filter_eq_self.mpr (fun v hv => ?_)
  obtain ⟨hl, hu⟩ := inside_scores hcov v hv
  rw [Bool.and_eq_true]
  exact ⟨by simpa using Nat.le_trans hlo hl,
         by simpa using Nat.lt_of_le_of_lt hu hhi⟩

end TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_Manhattan_TakeAll
