import Init.Data.Order.Lemmas
import TinylambOpt.ResearchGoals.QueryRewrites.PartitionedAggregateStates

namespace TinylambOpt.ResearchProofs.Batch5.QueryRewrites.PartitionedAggregateStates.Goal_SignedMeanDeviationGuard

open TinylambOpt.ResearchGoals.QueryRewrites.PartitionedAggregateStates


/-!
A positive relative-deviation guard with a signed mean only fires when the mean
is positive. Once that is certified, the division by the mean can be replaced by
an exact cross-product comparison, so the optimizer may test `10 * max delta
(-delta) > mean` without introducing a signed denominator.
-/

set_option maxHeartbeats 0

namespace Scratch.SignedMeanDeviationGuard

open TinylambOpt.ResearchGoals.QueryRewrites.PartitionedAggregateStates

/-- The maximum of a value and its negation is its absolute value, hence nonnegative. -/
theorem max_neg_nonneg (delta : Rat) : 0 ≤ max delta (-delta) := by
  rcases Rat.le_total with h | h
  · exact Rat.le_trans h Std.left_le_max
  · exact Rat.le_trans (by simpa using (Rat.neg_le_neg h : -0 ≤ -delta)) Std.right_le_max

/-- Negating a nonzero rational negates its inverse. -/
theorem inv_neg_self (m : Rat) (hm : m ≠ 0) : (-m)⁻¹ = -m⁻¹ := by
  apply Rat.inv_eq_of_mul_eq_one
  calc
    -m * -m⁻¹ = -(m * -m⁻¹) := Rat.neg_mul m (-m⁻¹)
    _ = -(-(m * m⁻¹)) := by rw [Rat.mul_neg]
    _ = m * m⁻¹ := by rw [Rat.neg_neg]
    _ = 1 := Rat.mul_inv_cancel m hm

/-- A nonnegative numerator over a negative denominator is nonpositive. -/
theorem div_nonpos_of_nonneg_of_neg {d m : Rat} (hd : 0 ≤ d) (hm : m < 0) : d / m ≤ 0 := by
  rw [Rat.div_def]
  have hpos : 0 < -m := by simpa using (Rat.neg_lt_neg hm : -0 < -m)
  have hinvpos : 0 < (-m)⁻¹ := Rat.inv_pos.mpr hpos
  have hinvnegpos : 0 < -m⁻¹ := by
    simpa [inv_neg_self m (Rat.ne_of_lt hm)] using hinvpos
  have hprod : 0 ≤ d * -m⁻¹ := Rat.mul_nonneg hd (Rat.le_of_lt hinvnegpos)
  have hneg : 0 ≤ -(d * m⁻¹) := by simpa [Rat.mul_neg] using hprod
  exact (Rat.neg_le_neg_iff).mp (by simpa using hneg)

/-- One tenth in exact rational arithmetic is positive. -/
theorem tenth_pos : 0 < (1 / 10 : Rat) := by
  have hinv : 0 < (10 : Rat)⁻¹ := Rat.inv_pos.mpr (by decide : (0 : Rat) < 10)
  simpa [Rat.div_def] using (Rat.mul_pos (by decide : (0 : Rat) < 1) hinv)

/-- Clearing a positive mean from the guard yields the product comparison. -/
theorem guard_to_product {d m : Rat} (hm : 0 < m) (h : (1 / 10 : Rat) < d / m) : m < 10 * d := by
  have h1 : (1 / 10 : Rat) * m < d := (Rat.lt_div_iff hm).mp h
  have hmul := Rat.mul_lt_mul_of_pos_right h1 (by decide : (0 : Rat) < 10)
  calc
    m = ((1 / 10 : Rat) * m) * 10 := by
      rw [Rat.mul_comm (1 / 10 : Rat) m, Rat.mul_assoc,
        Rat.div_mul_cancel (by decide : (10 : Rat) ≠ 0), Rat.mul_one]
    _ < d * 10 := hmul
    _ = 10 * d := Rat.mul_comm d 10

/-- The product comparison implies the guard when the mean is positive. -/
theorem product_to_guard {d m : Rat} (hm : 0 < m) (h : m < 10 * d) : (1 / 10 : Rat) < d / m := by
  rw [Rat.lt_div_iff hm]
  have hmul := Rat.mul_lt_mul_of_pos_left h tenth_pos
  calc
    (1 / 10 : Rat) * m < (1 / 10 : Rat) * (10 * d) := hmul
    _ = d := by
      rw [← Rat.mul_assoc, Rat.div_mul_cancel (by decide : (10 : Rat) ≠ 0), Rat.one_mul]

theorem sound : SignedMeanDeviationGuard.Goal := by
  intro delta mean hne
  constructor
  · intro h
    have hd0 := max_neg_nonneg delta
    have hposmean : 0 < mean := by
      by_cases hle : mean ≤ 0
      · have hlt : mean < 0 := Rat.lt_of_le_of_ne hle hne
        have hnonpos := div_nonpos_of_nonneg_of_neg hd0 hlt
        have hquotpos : 0 < max delta (-delta) / mean :=
          Rat.not_le.mp (fun hq0 => (Rat.not_le.mpr h)
            (Rat.le_trans hq0 (Rat.le_of_lt tenth_pos)))
        exact False.elim ((Rat.not_le.mpr hquotpos) hnonpos)
      · exact Rat.not_le.mp hle
    exact ⟨hposmean, guard_to_product hposmean h⟩
  · intro hright
    rcases hright with ⟨hposmean, hprod⟩
    exact product_to_guard hposmean hprod

end Scratch.SignedMeanDeviationGuard
end TinylambOpt.ResearchProofs.Batch5.QueryRewrites.PartitionedAggregateStates.Goal_SignedMeanDeviationGuard
