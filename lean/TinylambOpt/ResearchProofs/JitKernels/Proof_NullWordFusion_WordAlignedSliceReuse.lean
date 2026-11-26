import TinylambOpt.ResearchGoals.JitKernels.NullWordFusion

/-!
**Proved: word-aligned bitmap slice reuse.** Dropping a whole-word prefix from
the flattened lane list agrees with dropping those words from the bitmap and
rebuilding only the requested suffix, because `64 * base` bit offsets shift the
word index by exactly `base` and preserve the low-six-bit lane index. SQL
execution may pass the original word slice to a nested kernel without repacking
its null bitmap whenever the slice starts at a word boundary.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.JitKernels.NullWordFusion_WordAlignedSliceReuse
open TinylambOpt.ResearchGoals.JitKernels.NullWordFusion

/-- Indexing a dropped list reads the original list at the shifted index. -/
private theorem getElem?_drop {α : Type} (l : List α) (s i : Nat) :
    (l.drop s)[i]? = l[s + i]? := by
  induction l generalizing s with
  | nil => simp
  | cons x xs ih =>
    cases s with
    | zero => simp [Nat.zero_add]
    | succ s =>
      have harith : s + 1 + i = s + i + 1 := by omega
      show (xs.drop s)[i]? = (x :: xs)[s + 1 + i]?
      rw [harith, List.getElem?_cons_succ, ← ih]

theorem sound : WordAlignedSliceReuse.Goal := by
  intro ws base n
  unfold lanes
  rw [← List.map_drop, List.range_add,
    List.drop_append_of_le_length (by simp : 64 * base ≤ (List.range (64 * base)).length),
    List.drop_eq_nil_of_le (by simp : (List.range (64 * base)).length ≤ 64 * base),
    List.nil_append, List.map_map]
  refine List.map_congr_left ?_
  intro i hi
  simp only [Function.comp_apply, getElem?_drop,
    Nat.mul_add_div (by decide : 64 > 0) base i, Nat.mul_add_mod 64 base i]

end TinylambOpt.ResearchProofs.JitKernels.NullWordFusion_WordAlignedSliceReuse
