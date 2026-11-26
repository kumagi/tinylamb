import TinylambOpt.ResearchGoals.JitKernels.PredicatedStores

/-!
# ZeroFillInactiveOutput: sound

At any byte whose validity lane is live, the masked store and the
unconditional zero-filled store (inactive lanes replaced by `0`) agree, so
SQL execution may replace the predicated store by a full-width store as long
as the reader checks the validity bitmap first — inactive bytes may be
written arbitrarily.
-/

set_option maxHeartbeats 4000000
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.JitKernels.PredicatedStores_ZeroFillInactiveOutput
open TinylambOpt.ResearchGoals.JitKernels.PredicatedStores

private theorem store_apply (m : Mem) (base : Nat) (bs : List UInt8) (i : Nat) :
    store m base bs i = if base ≤ i then bs[i - base]?.getD (m i) else m i := rfl

private theorem masked_apply (m : Mem) (a : Nat) (xs : List (Option UInt8)) (i : Nat) :
    masked m a xs i = if a ≤ i then xs[i - a]?.join.getD (m i) else m i := rfl

theorem sound : ZeroFillInactiveOutput.Goal := by
  intro m a xs i h
  rw [masked_apply, store_apply]
  rw [if_pos (Nat.le_add_right a i), if_pos (Nat.le_add_right a i),
    Nat.add_sub_cancel_left]
  obtain ⟨v, hv⟩ : ∃ v, xs[i]?.join = some v := by
    cases h' : xs[i]?.join with
    | none => exact absurd h (by simp [h'])
    | some v => exact ⟨v, rfl⟩
  cases hxs : xs[i]? with
  | none => exact absurd hv (by simp [hxs])
  | some u =>
    cases u with
    | none => exact absurd hv (by simp [hxs])
    | some w =>
      rw [List.getElem?_map, hxs]
      simp

end TinylambOpt.ResearchProofs.JitKernels.PredicatedStores_ZeroFillInactiveOutput
