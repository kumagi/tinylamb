import TinylambOpt.ResearchGoals.StorageExecution.BitLayouts

/-!
SQL column readers can work with packed bits when lookup and filtering preserve the
represented values and row positions.

Proved

- `RowMajor.SharedEdgeByte` — `∀ n w i, 0<w → ((footprint layout n w i).any (footprint
  layout n w (i+1)).contains) = decide ((i+1)*w%8≠0)`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_RowMajor_SharedEdgeByte

open TinylambOpt.ResearchGoals.StorageExecution.BitLayouts
open TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.RowMajor

/-- Membership in the two erased footprints is just a pair of lane indices. -/
private theorem any_footprint_eq (n w i : Nat) :
  ((footprint layout n w i).any (footprint layout n w (i+1)).contains) = true ↔
    ∃ b, b < w ∧ ∃ c, c < w ∧ (i*w+b)/8 = ((i+1)*w+c)/8 := by
  simp only [footprint, lanes, layout, List.any_eq_true, List.mem_eraseDups,
    List.mem_map, List.mem_range, List.contains_iff_mem]
  constructor
  · rintro ⟨x, ⟨b, hb, rfl⟩, ⟨c, hc, hcx⟩⟩
    exact ⟨b, hb, c, hc, hcx.symm⟩
  · rintro ⟨b, hb, c, hc, heq⟩
    exact ⟨(i*w+b)/8, ⟨b, hb, rfl⟩, ⟨c, hc, heq.symm⟩⟩

/-- Two consecutive bit ranges share a byte exactly when the boundary is not byte aligned. -/
private theorem share_iff (w i : Nat) (hw : 0 < w) :
    (∃ b, b < w ∧ ∃ c, c < w ∧ (i*w+b)/8 = ((i+1)*w+c)/8) ↔ (i+1)*w % 8 ≠ 0 := by
  rw [Nat.succ_mul]
  constructor
  · rintro ⟨b, hb, c, hc, heq⟩
    omega
  · intro h
    refine ⟨w-1, by omega, 0, by omega, ?_⟩
    omega

theorem sound_SharedEdgeByte : RowMajor.SharedEdgeByte.Goal := by
  intro n w i hw
  apply Bool.eq_iff_iff.mpr
  simpa using (any_footprint_eq n w i).trans (share_iff w i hw)

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_RowMajor_SharedEdgeByte
