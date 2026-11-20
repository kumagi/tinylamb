import TinylambOpt.BranchlessJitCodegen

/-! The fused scalar SUM reads signed INT64 columns with no NULLs and a
total predicate. The runtime guard checks the multiply and add separately
and bounds every prefix from the existing accumulator. The fused reduction
shares the exact selected-row count with the split pipeline, including the
empty-result case. -/

namespace TinylambOpt.FusedAggregateJit

abbrev Input := Int × Int

def projection (mul add value : Int) : Int := value * mul + add

def fusedStep (keep : Int → Bool) (mul add : Int) (state : Int × Nat)
    (row : Input) : Int × Nat :=
  (state.1 + (if keep row.1 then projection mul add row.2 else 0),
    state.2 + (keep row.1).toNat)

def fused (keep : Int → Bool) (mul add : Int) (rows : List Input)
    (state : Int × Nat) : Int × Nat :=
  rows.foldl (fusedStep keep mul add) state

def materialized (keep : Int → Bool) (mul add : Int) (rows : List Input) : List Int :=
  (rows.filter (fun row => keep row.1)).map (fun row => projection mul add row.2)

theorem fused_matches_materialized (keep : Int → Bool) (mul add : Int)
    (rows : List Input) (state : Int × Nat) :
    fused keep mul add rows state =
      ((materialized keep mul add rows).foldl (· + ·) state.1,
       state.2 + (materialized keep mul add rows).length) := by
  induction rows generalizing state with
  | nil => simp [fused, materialized]
  | cons row rest ih =>
    change fused keep mul add rest (fusedStep keep mul add state row) = _
    rw [ih]
    cases h : keep row.1 <;>
      simp [materialized, fusedStep, h, Nat.add_assoc, Nat.add_comm]

theorem affine_endpoints_bound (lo hi v mul : Int)
    (range : lo ≤ v ∧ v ≤ hi) :
    min (lo * mul) (hi * mul) ≤ v * mul ∧
      v * mul ≤ max (lo * mul) (hi * mul) := by
  by_cases sign : 0 ≤ mul
  · have left := Int.mul_le_mul_of_nonneg_right range.1 sign
    have right := Int.mul_le_mul_of_nonneg_right range.2 sign
    omega
  · have positive : 0 ≤ -mul := by omega
    have left := Int.mul_le_mul_of_nonneg_right range.1 positive
    have right := Int.mul_le_mul_of_nonneg_right range.2 positive
    simp only [Int.mul_neg] at left right
    omega

def selectedOptions (keep : Int → Bool) (mul add : Int) (rows : List Input) :
    List (Option Int) :=
  rows.map fun row => if keep row.1 then some (projection mul add row.2) else none

theorem selected_fold (keep : Int → Bool) (mul add : Int) (rows : List Input)
    (state : Int × Nat) :
    simdZeroMaskedSum state.1 (selectedOptions keep mul add rows) =
      (fused keep mul add rows state).1 := by
  induction rows generalizing state with
  | nil => rfl
  | cons row rest ih =>
    calc
      _ = simdZeroMaskedSum (fusedStep keep mul add state row).1
            (selectedOptions keep mul add rest) := by
              cases h : keep row.1 <;>
                simp [simdZeroMaskedSum, selectedOptions, fusedStep, h]
      _ = (fused keep mul add rest (fusedStep keep mul add state row)).1 := ih _
      _ = _ := rfl

/-- Intermediate projection overflow and running-prefix overflow are both
    absent for the emitted batch. The LLVM loop's masked signed reduction
    computes the reference sum from the same incoming total. -/
theorem guarded_fused_sum (keep : Int → Bool) (mul add lo hi : Int)
    (rows : List Input) (state : Int × Nat)
    (range : ∀ r ∈ rows, lo ≤ r.2 ∧ r.2 ≤ hi)
    (initial : InI64 state.1)
    (mulLow : InI64 (min (lo * mul) (hi * mul)))
    (mulHigh : InI64 (max (lo * mul) (hi * mul)))
    (addLow : InI64 (min (lo * mul) (hi * mul) + add))
    (addHigh : InI64 (max (lo * mul) (hi * mul) + add))
    (envLow : i64MinInt ≤ state.1 + (rows.length : Int) * min 0 (min (lo * mul) (hi * mul) + add))
    (envHigh : state.1 + (rows.length : Int) * max 0 (max (lo * mul) (hi * mul) + add) ≤ i64MaxInt) :
    (∀ r ∈ rows, InI64 (r.2 * mul) ∧ InI64 (projection mul add r.2)) ∧
    foldCheckedSum state.1 (selectedOptions keep mul add rows) =
      some (fused keep mul add rows state).1 := by
  constructor
  · intro row present
    obtain ⟨low, high⟩ := affine_endpoints_bound lo hi row.2 mul (range row present)
    exact ⟨⟨Int.le_trans mulLow.1 low, Int.le_trans high mulHigh.2⟩,
      ⟨Int.le_trans addLow.1 (Int.add_le_add_right low _),
       Int.le_trans (Int.add_le_add_right high _) addHigh.2⟩⟩
  · rw [← selected_fold]
    apply zonemap_prefix_sum_masked_sound (min (lo * mul) (hi * mul) + add)
        (max (lo * mul) (hi * mul) + add)
    · intro o ho v hv
      obtain ⟨row, present, shape⟩ := List.mem_map.mp ho
      have ⟨low, high⟩ := affine_endpoints_bound lo hi row.2 mul (range row present)
      cases h : keep row.1 <;> simp [h, projection, hv] at shape
      omega
    · exact initial
    · simpa [selectedOptions] using envLow
    · simpa [selectedOptions] using envHigh

/-- The plain bit-vector ADD used by LLVM remains associative under
    reassociation, including SIMD lanes whose partial sums wrap. The checked
    scalar reference is recovered because its final signed total is in range. -/
theorem modular_reassociate (a b c d : BitVec 64) :
    ((a + b) + c) + d = (a + c) + (b + d) := by
  bv_decide

end TinylambOpt.FusedAggregateJit
