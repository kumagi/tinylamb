import TinylambOpt.ResearchGoals.JitKernels.PlanePredicates

/-!
# Plane predicate kernel proofs and refutations

The unsigned plane comparator, common-plane elimination, population-count sum,
and signed sign-bit bias are proved sound. Threshold truncation, AND-fold range
minima, below-base residual thresholds, permuted dot-product inputs, and OR-mask
distinct summaries are each refuted by a concrete input.

The positive results justify executing the corresponding SQL predicate and
aggregate kernels without decoding lanes or changing their modeled results.
The negative results identify the missing guards: execution must not apply
those rewrites under only the assumptions stated in the refuted goals.
-/

namespace TinylambOpt.ResearchProofs.JitKernels.Proof_PlanePredicates

open TinylambOpt.ResearchGoals.JitKernels.PlanePredicates

set_option maxHeartbeats 0
set_option maxRecDepth 10000

/-- Pointwise agreement of the bit-fold steps makes two folds equal. -/
private theorem foldBits_congr_step (x c x' c' : Nat) (indices : List Nat)
    (hstep : ∀ j ∈ indices,
      (fun lt : Bool => if bit x j == bit c j then lt else bit c j) =
        (fun lt : Bool => if bit x' j == bit c' j then lt else bit c' j)) :
    (indices.foldl (fun lt j => if bit x j == bit c j then lt else bit c j) false) =
      (indices.foldl (fun lt j => if bit x' j == bit c' j then lt else bit c' j) false) := by
  induction indices with
  | nil => rfl
  | cons j js ih =>
      have hstep' : ∀ k ∈ js,
          (fun lt : Bool => if bit x k == bit c k then lt else bit c k) =
            (fun lt : Bool => if bit x' k == bit c' k then lt else bit c' k) :=
        fun k hk => hstep k (List.mem_cons_of_mem j hk)
      rw [List.foldl_cons, List.foldl_cons,
        hstep j List.mem_cons_self]
      exact ih hstep'

/-- Toggling one named plane flips exactly that bit. -/
private theorem bit_xor_two_pow (x j k : Nat) :
    bit (x ^^^ 2 ^ j) k = (if k = j then !bit x j else bit x j) := by
  simp only [bit, Nat.testBit_xor, Nat.testBit_two_pow]
  by_cases hkj : k = j
  · subst k
    simp
  · simp [Ne.symm hkj]

/-- A fold ignores high bits outside the inspected frame. -/
private theorem foldBits_mod (x c w : Nat) :
    foldBits x c w = foldBits (x % 2 ^ w) (c % 2 ^ w) w := by
  apply foldBits_congr_step
  intro j hj
  funext lt
  have hjlt : j < w := List.mem_range.mp hj
  simp only [bit, Nat.testBit_mod_two_pow, decide_eq_true hjlt, Bool.true_and]

/-- Adding one plane applies its decision after all lower-plane decisions. -/
private theorem foldBits_succ (x c w : Nat) :
    foldBits x c (w + 1) =
      (if bit x w == bit c w then foldBits x c w else bit c w) := by
  simp [foldBits, List.range_succ]

/-- Split a bounded natural into its low bits and its selected high bit. -/
private theorem eq_high_add_mod {x w : Nat} (hx : x < 2 ^ (w + 1)) :
    x = (if bit x w then 2 ^ w else 0) + x % 2 ^ w := by
  by_cases hbit : bit x w = true
  · have hle : 2 ^ w ≤ x := Nat.ge_two_pow_of_testBit hbit
    have hdiv : x / 2 ^ w = 1 := by
      apply Nat.div_eq_of_lt_le (k := 1) (n := 2 ^ w) (m := x)
      · simpa using hle
      · simpa [Nat.pow_succ, Nat.mul_comm, Nat.add_mul] using hx
    have hdecomp := Nat.div_add_mod x (2 ^ w)
    rw [hdiv, Nat.one_mul] at hdecomp
    simp only [hbit, if_true]
    exact hdecomp.symm
  · have hlt : x < 2 ^ w := by
      by_contra hnot
      exact hbit (Nat.testBit_of_two_pow_le_and_two_pow_add_one_gt
        (Nat.le_of_not_gt hnot) hx)
    simp only [hbit, if_false, Nat.zero_add]
    exact (Nat.mod_eq_of_lt hlt).symm

/-- The low-width binary expansion reconstructs any bounded natural exactly. -/
private theorem sum_bits_eq_self (x w : Nat) (hx : x < 2 ^ w) :
    ((List.range w).map (fun j => if bit x j then 2 ^ j else 0)).sum = x := by
  induction w with
  | zero =>
      have hx0 : x = 0 := by simpa using hx
      subst x
      rfl
  | succ w ih =>
      have hlow : x % 2 ^ w < 2 ^ w := Nat.mod_lt _ (Nat.two_pow_pos w)
      have hmap :
          ((List.range w).map (fun j => if bit (x % 2 ^ w) j then 2 ^ j else 0)) =
            ((List.range w).map (fun j => if bit x j then 2 ^ j else 0)) := by
        apply List.map_congr_left
        intro j hj
        have hjlt : j < w := List.mem_range.mp hj
        simp [bit, Nat.testBit_mod_two_pow, hjlt]
      have hdecomp := eq_high_add_mod hx
      rw [List.range_succ, List.map_append, List.map_cons, List.map_nil,
        List.sum_append, List.sum_cons, List.sum_nil, Nat.add_zero,
        ← hmap, ih hlow]
      cases hb : bit x w <;> simp only [hb, if_true, if_false, Nat.add_zero] at hdecomp ⊢ <;>
        omega

/-- A population count multiplied by its plane weight equals a lane-wise sum. -/
private theorem weight_eq_map_sum (xs : List Nat) (j : Nat) :
    weight xs j = (xs.map (fun x => if bit x j then 2 ^ j else 0)).sum := by
  induction xs with
  | nil => rfl
  | cons x xs ih =>
      cases hb : bit x j <;>
        simp [weight, planes, hb, List.count_cons, List.sum_cons, Nat.add_mul, ih]

/-- Adding a lane adds its selected bit weight to every plane population. -/
private theorem weight_cons (x : Nat) (xs : List Nat) (j : Nat) :
    weight (x :: xs) j = weight xs j + (if bit x j then 2 ^ j else 0) := by
  cases hb : bit x j <;>
    simp [weight, planes, hb, List.count_cons, Nat.add_mul]

/-- Summing a list of pointwise sums may be performed plane by plane. -/
private theorem sum_map_add (indices : List Nat) (f g : Nat → Nat) :
    (indices.map (fun j => f j + g j)).sum =
      (indices.map f).sum + (indices.map g).sum := by
  induction indices with
  | nil => rfl
  | cons j js ih =>
      simp only [List.map_cons, List.sum_cons, ih]
      omega

/-- For two bounded unsigned lanes, the least-to-most-significant fold is the
unsigned comparison. SQL execution may therefore compare compressed residuals
without decoding them, while preserving exactly which rows satisfy `<`. -/
theorem planeComparisonWithoutDecode : PlaneComparisonWithoutDecode.Goal := by
  intro x c w
  induction w with
  | zero =>
      intro hx hc
      have hx0 : x = 0 := by simpa using hx
      have hc0 : c = 0 := by simpa using hc
      subst x
      subst c
      rfl
  | succ w ih =>
      intro hx hc
      have hxlow : x < 2 ^ w :=
        Nat.lt_of_lt_of_le hx (Nat.pow_le_pow_right (by decide : 0 < 2) (Nat.le_succ w))
      have hclow : c < 2 ^ w :=
        Nat.lt_of_lt_of_le hc (Nat.pow_le_pow_right (by decide : 0 < 2) (Nat.le_succ w))
      rw [foldBits_succ]
      cases hxbit : bit x w <;> cases hcbit : bit c w
      · simp only [hxbit, hcbit, Bool.false_eq_false, if_true]
        rw [ih hxlow hclow]
        apply decide_eq_decide.mpr
        rw [eq_high_add_mod hx, eq_high_add_mod hc, hxbit, hcbit]
        simp only [if_false, Nat.zero_add]
        omega
      · simp only [hxbit, hcbit, Bool.false_eq_true, if_false]
        apply decide_eq_true_eq.mpr
        have hxlt : x < 2 ^ w := Nat.lt_of_not_ge (fun hge =>
          hxbit (Nat.testBit_of_two_pow_le_and_two_pow_add_one_gt hge hx))
        have hcge : 2 ^ w ≤ c := Nat.ge_two_pow_of_testBit hcbit
        omega
      · simp only [hxbit, hcbit, Bool.true_eq_false, if_false]
        apply decide_eq_false_iff_not.mpr
        have hxge : 2 ^ w ≤ x := Nat.ge_two_pow_of_testBit hxbit
        have hclt : c < 2 ^ w := Nat.lt_of_not_ge (fun hge =>
          hcbit (Nat.testBit_of_two_pow_le_and_two_pow_add_one_gt hge hc))
        omega
      · simp only [hxbit, hcbit, Bool.true_eq_true, if_true]
        rw [ih hxlow hclow]
        apply decide_eq_decide.mpr
        rw [eq_high_add_mod hx, eq_high_add_mod hc, hxbit, hcbit]
        simp only [if_true]
        omega

/-- The stated truncation rule is false: with `x = 0`, `c = 2`, and `w = 1`,
the low-bit ballot is false although `0 < 2`. SQL execution cannot truncate an
out-of-frame threshold to zero and retain the original comparison result. -/
theorem unsignedThresholdTruncation_refuted : ¬ UnsignedThresholdTruncation.Goal := by
  intro h
  have heq := h 0 2 1 (by decide)
  exact (by decide : foldBits 0 2 1 ≠ decide (0 < 2)) heq

/-- If a plane agrees at every compared pair, toggling that plane in both lanes
leaves the ballot unchanged. SQL execution may elide such a common plane while
preserving all modeled row comparisons. -/
theorem commonPlaneElision : CommonPlaneElision.Goal := by
  intro x c j w hbit
  apply foldBits_congr
  · intro k _
    simp only [bit, Nat.testBit_xor, Nat.testBit_two_pow]
    by_cases hkj : k = j
    · subst k
      simp [hbit]
    · simp [hkj]
  · intro k _
    simp only [bit, Nat.testBit_xor, Nat.testBit_two_pow]
    by_cases hkj : k = j
    · subst k
      simp [hbit]
    · simp [hkj]

/-- An AND fold is not a numeric minimum: `3 &&& 5 = 1`, but `min 3 5 = 3`.
With no extra assumptions, SQL range-minimum execution must use an ordered fold
rather than treating an intersected bit mask as the smallest lane. -/
theorem orFoldForRangeMinimum_refuted : ¬ OrFoldForRangeMinimum.Goal := by
  intro h
  have heq := h 3 [5]
  exact (by decide : List.foldl Nat.land 3 [5] ≠ ((3 :: [5]).min?).getD 0) heq

/-- When every lane fits in `w` bits, summing the weighted plane populations
reconstructs the lane sum, including duplicates and the empty frame. SQL
execution may compute `SUM` from the ballots without decoding individual lanes. -/
theorem planePopcountSum : PlanePopcountSum.Goal := by
  intro xs w
  induction xs with
  | nil =>
      intro _
      rfl
  | cons x xs ih =>
      intro hbound
      have hx : x < 2 ^ w := hbound x (List.mem_cons_self x xs)
      have hxs : ∀ y ∈ xs, y < 2 ^ w :=
        fun y hy => hbound y (List.mem_cons_of_mem x hy)
      have hmap :
          ((List.range w).map (weight (x :: xs))) =
            ((List.range w).map (fun j => weight xs j + (if bit x j then 2 ^ j else 0))) := by
        apply List.map_congr_left
        intro j _
        exact weight_cons x xs j
      rw [hmap, sum_map_add, ih hxs, sum_bits_eq_self x w hx]
      simp only [List.sum_cons]
      omega

/-- Truncated residual subtraction is false for a threshold below the page base:
for `base = 1`, `x = 0`, and `c = 0`, the residual test holds while the absolute
test fails. SQL execution needs an explicit below-base rejection guard before
using this predicate rewrite. -/
theorem biasedResidualThreshold_refuted : ¬ BiasedResidualThreshold.Goal := by
  intro h
  have heq := h 1 0 0
  omega

/-- Flipping the 32-bit sign bit converts signed two's-complement order into
unsigned order for every lane pair. SQL execution may bias both signed operands
and then reuse the unsigned plane comparator without changing row order. -/
theorem xorBiasSignedOrder : XorBiasSignedOrder.Goal := by
  intro a b
  have hbool :
      a.slt b = decide ((a ^^^ 0x80000000) < (b ^^^ 0x80000000)) := by
    apply (Bool.eq_iff_iff).mpr
    bv_decide
  have hdecide : decide (a.toInt < b.toInt) =
      decide ((a ^^^ 0x80000000).toNat < (b ^^^ 0x80000000).toNat) := by
    simpa only [BitVec.slt_eq_decide, decide_eq_true_eq, BitVec.lt_def] using hbool
  exact decide_eq_decide.mp hdecide

/-- Equal lengths and permutation do not preserve a positional dot product:
`[1,2] · [1,2] = 5`, but `[1,2] · [2,1] = 4`. SQL execution cannot reorder one
input independently; the rewrite needs a permutation applied to both ballots. -/
theorem sharedBitMarginalsDotProduct_refuted : ¬ SharedBitMarginalsDotProduct.Goal := by
  intro h
  have heq := h [1, 2] [1, 2] [2, 1] (by decide) (by decide)
  exact (by decide :
    (List.zipWith (· * ·) [1, 2] [1, 2]).sum ≠
      (List.zipWith (· * ·) [1, 2] [2, 1]).sum) heq

/-- An OR synopsis does not determine the distinct values: `[3,3]` and `[1,2]`
have the same OR but different deduplicated lists. SQL execution cannot use the
OR alone as an exact distinct-set synopsis, even when frame lengths agree. -/
theorem distinctMaskFromOr_refuted : ¬ DistinctMaskFromOr.Goal := by
  intro h
  have heq := h [3, 3] [1, 2] (by decide) (by decide)
  exact (by decide : ¬ List.Perm [3, 3].eraseDups [1, 2].eraseDups) heq

end TinylambOpt.ResearchProofs.JitKernels.Proof_PlanePredicates
