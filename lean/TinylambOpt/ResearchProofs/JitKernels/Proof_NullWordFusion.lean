import TinylambOpt.ResearchGoals.JitKernels.NullWordFusion

/-!
# Null-word fusion proof audit

This module audits branchless SQL kernels over the engine's 64-row null words.
It proves the sound mask identities, allowing execution to replace per-lane
validity tests, CASE null selection, sparse null guards, and aligned bitmap
slices with the corresponding word operations without changing the modeled
result. It also refutes four unsafe optimizations with concrete words: an
unqualified padding-complement count, an all-NULL inference from two nonempty
null sets, word-index narrowing across a boundary, and validity-vector reuse
from equal null counts alone.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.Proof_NullWordFusion

open TinylambOpt.ResearchGoals.JitKernels.NullWordFusion

/-- The logical window has a set bit exactly below its bounded row count. This
supports proving tail-word validity from the bits of the input null word. -/
private theorem window_getLsbD (n i : Nat) :
    (window n).getLsbD i = decide (i < min 64 n) := by
  by_cases h : i < min 64 n
  · have hi : i < 64 := Nat.lt_of_lt_of_le h (Nat.min_le_left 64 n)
    simp [window, BitVec.getLsbD_ofNat, Nat.testBit_two_pow_sub_one, h, hi]
  · have hmin := Nat.min_le_left 64 n
    have hi : ¬ i < 64 := by omega
    simp [window, BitVec.getLsbD_ofNat, Nat.testBit_two_pow_sub_one, h, hi]

/-- **Proved: no nulls from the tail word.** For a packet of at most 64 rows,
a masked validity word equal to the logical window is equivalent to every
logical input lane being non-null. SQL execution may therefore use one word
comparison as the non-null guard while ignoring unconstrained padding bits. -/
theorem noNullsFromTailWord : NoNullsFromTailWord.Goal := by
  intro n m hn
  constructor
  · intro h i hi
    have hbit := congrArg (fun w : W => w.getLsbD i) h
    rw [valid, BitVec.getLsbD_and, BitVec.getLsbD_not, window_getLsbD,
      Nat.min_eq_right hn, decide_eq_true hi, decide_eq_true (Nat.lt_of_lt_of_le hi hn),
      Bool.true_and, Bool.true_and] at hbit
    simpa using hbit
  · intro hm
    apply BitVec.eq_of_getLsbD_eq_iff.mpr
    intro i hi
    rw [valid, BitVec.getLsbD_and, BitVec.getLsbD_not, window_getLsbD,
      Nat.min_eq_right hn, decide_eq_true hi]
    by_cases hin : i < n
    · rw [decide_eq_true hin, hm i hin]
      simp
    · rw [decide_eq_false hin]
      simp

/-- **Refuted: padding-complement count without a positive-width guard.** At
`n = 0`, the zero word satisfies the padding-clearness premise, but its valid
ordinal count is `0` rather than `64`. SQL execution cannot complement the full
word popcount to count rows in an empty tail packet. -/
theorem paddingBitComplementCount_refuted : ¬ PaddingBitComplementCount.Goal := by
  intro h
  have hpad : (0 : W) &&& ~~~(window 0) = 0 := by decide
  have hh := h 0 (0 : W) (by omega) hpad
  exact (by decide :
    ¬ (ords (valid 0 (0 : W))).length = 64 - (ords (0 : W)).length) hh

/-- **Proved: COALESCE null intersection.** De Morgan's law makes the null mask
of the bitwise input intersection equal to the union of their logical validity
masks. SQL execution may derive COALESCE validity from the two input masks
without materializing every input lane. -/
theorem coalesceNullIntersection : CoalesceNullIntersection.Goal := by
  intro a b n
  unfold valid
  bv_decide

/-- **Proved: branchless CASE null mask.** At every physical lane, selecting the
then/else null bit with a truth-and-non-null-predicate mask agrees with scalar
CASE evaluation, including NULL predicate lanes taking the else branch. SQL
execution may replace per-lane predicate tests with this bit select. -/
theorem branchNullMaskMux : BranchNullMaskMux.Goal := by
  intro truth predNull thenNull elseNull i hi
  dsimp only
  simp only [BitVec.getLsbD_and, BitVec.getLsbD_or, BitVec.getLsbD_not,
    decide_eq_true hi, Bool.true_and]
  cases ht : truth.getLsbD i <;> cases hp : predNull.getLsbD i <;>
    cases hn : thenNull.getLsbD i <;> cases he : elseNull.getLsbD i <;>
    simp_all

/-- **Refuted: strict arithmetic all-NULL shortcut.** Words `1` and `2` each
contain a NULL in a three-row packet, yet their union leaves lane 2 valid.
SQL execution therefore cannot infer an all-NULL strict result merely because
both operands contain some NULL. -/
theorem strictArithmeticAllNull_refuted : ¬ StrictArithmeticAllNull.Goal := by
  intro h
  have ha : (1 : W) &&& window 3 ≠ 0 := by decide
  have hb : (2 : W) &&& window 3 ≠ 0 := by decide
  have hh := h (1 : W) (2 : W) 3 ha hb
  exact (by decide : ¬ valid 3 ((1 : W) ||| (2 : W)) = 0) hh

/-- **Refuted: gather word-index narrowing across a boundary.** Starting at
row 63 in words `[0, 1]`, the second gathered lane comes from word 1 and is
NULL, while the narrowed expression reads word 0 and reports non-NULL. SQL
execution may narrow indices only under an additional no-boundary-crossing
certificate. -/
theorem gatherWordIndexNarrowing_refuted : ¬ GatherWordIndexNarrowing.Goal := by
  intro h
  have hh := h ([0, 1] : List W) 63
  have hsecond := congrArg (fun xs : List (Option Bool) => xs[1]?) hh
  exact (by decide : ¬
    (((List.range 32).map (fun j =>
      (lanes ([0, 1] : List W) (63 + 32))[63 + j]?))[1]? =
      ((List.range 32).map (fun j =>
        some ((([0, 1] : List W)[63 / 64]?.getD 0).getLsbD ((63 + j) % 64))))[1]?)) hsecond

/-- **Proved: sparse on-demand null guard.** A selection word is disjoint from
the null word exactly when every selected ordinal has a clear null bit. SQL
execution may guard the unchecked value path by the word intersection, or
check only the sparse selected ordinals, with the same result. -/
theorem sparseNullGuardOnDemand : SparseNullGuardOnDemand.Goal := by
  intro selection nulls
  constructor
  · intro hdisjoint i hi
    have hsel := (List.mem_filter.mp hi).2
    have hbit := congrArg (fun w : W => w.getLsbD i) hdisjoint
    simp only [BitVec.getLsbD_and, hsel, Bool.true_and] at hbit
    simpa using hbit
  · intro hm
    apply BitVec.eq_of_getLsbD_eq_iff.mpr
    intro i hi
    simp only [BitVec.getLsbD_and, BitVec.getLsbD_zero]
    cases hsel : selection.getLsbD i with
    | false => simp
    | true =>
        have himem : i ∈ ords selection :=
          List.mem_filter.mpr ⟨List.mem_range.mpr hi, hsel⟩
        simp [hm i himem]

/-- **Refuted: unpacked validity cache keyed only by null count.** Words `1`
and `2` have one NULL apiece, but selection `[0]` sees different validity.
SQL execution cannot reuse an unpacked column vector from equal null counts
without also certifying equal masks. -/
theorem sharedUnpackCacheCount_refuted : ¬ SharedUnpackCacheCount.Goal := by
  intro h
  have hcnt : (ords (1 : W)).length = (ords (2 : W)).length := by decide
  have hh := h (1 : W) (2 : W) [0] hcnt
  exact (by decide : ¬ gather (1 : W) [0] = gather (2 : W) [0]) hh

/-- **Proved: word-aligned bitmap slice reuse.** Dropping a whole-word prefix
from the flattened lane list agrees with dropping those words and rebuilding
only the requested suffix. SQL execution may pass the original word slice to
a nested kernel without repacking its null bitmap. -/
theorem wordAlignedSliceReuse : WordAlignedSliceReuse.Goal := by
  intro ws base n
  unfold lanes
  rw [← List.map_drop, List.range_add,
    List.drop_append_of_le_length (by simp),
    List.drop_of_length_le (by simp [List.length_range])]
  simp only [List.map_nil, List.nil_append, Nat.sub_self, List.drop_zero,
    List.map_map, Function.comp_apply]
  apply List.map_congr_left
  intro i hi
  simp [Nat.mul_add_div (by decide : 0 < 64), Nat.mul_add_mod, Nat.add_comm]

/-- **Proved: nullable equality mask intersection.** The word expression
accepts equal NULL/NULL pairs and uses payload equality only for non-NULL
pairs. SQL execution may fuse `IS NOT DISTINCT FROM` validity into this mask
without changing any lane's result. -/
theorem nullableEqualityMaskIntersection : NullableEqualityMaskIntersection.Goal := by
  intro a b equalPayload i hi
  simp only [BitVec.getLsbD_and, BitVec.getLsbD_or, BitVec.getLsbD_not,
    decide_eq_true hi, Bool.true_and]
  cases ha : a.getLsbD i <;> cases hb : b.getLsbD i <;>
    cases he : equalPayload.getLsbD i <;>
    simp_all

end TinylambOpt.ResearchProofs.JitKernels.Proof_NullWordFusion
