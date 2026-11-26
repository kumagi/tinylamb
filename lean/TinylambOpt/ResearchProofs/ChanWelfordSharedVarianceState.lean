import TinylambOpt.ResearchGoals.ChanWelfordSharedVarianceState

/-!
Proved

- `Goal` — Shared Chan/Welford statistical accumulator for `(COUNT, AVG, STDDEV_SAMP)`
  (TPC-DS Q17/Q39): (1) computing one shared tuple state yields exact `COUNT`, `AVG`,
  and `VAR_SAMP` (matching `n ≤ 1 → none`), (2) `n * sumSq - sum^2` equals the scaled
  sum of squared deviations `∑ (n * x - sum)^2 / n`, and (3) parallel chunks satisfy
  the exact integer Chan cross-term identity `n1 * n2 * M2_num(xs ++ ys) =
  n2*(n1+n2)*M2_num(xs) + n1*(n1+n2)*M2_num(ys) + (n2*s1 - n1*s2)^2`.

The reference model is defined in
`TinylambOpt.ResearchGoals.ChanWelfordSharedVarianceState`.
-/

namespace TinylambOpt.ResearchProofs.ChanWelfordSharedVarianceState

open TinylambOpt.ResearchGoals.ChanWelfordSharedVarianceState

private theorem validValues_append (xs ys : List (Option Int)) :
    validValues (xs ++ ys) = validValues xs ++ validValues ys := by
  simp [validValues]

private theorem cntVals_append (xs ys : List (Option Int)) :
    cntVals (xs ++ ys) = cntVals xs + cntVals ys := by
  simp [cntVals, validValues]

private theorem sumVals_append (xs ys : List (Option Int)) :
    sumVals (xs ++ ys) = sumVals xs + sumVals ys := by
  simp [sumVals, validValues]

private theorem sumSqVals_append (xs ys : List (Option Int)) :
    sumSqVals (xs ++ ys) = sumSqVals xs + sumSqVals ys := by
  simp [sumSqVals, validValues]

private theorem foldl_stepShared_eq (xs : List (Option Int)) (st : SharedState) :
    xs.foldl stepShared st =
      { n := st.n + cntVals xs
        sum := st.sum + sumVals xs
        sumSq := st.sumSq + sumSqVals xs } := by
  induction xs generalizing st with
  | nil =>
      simp [cntVals, sumVals, sumSqVals, validValues]
  | cons a xs ih =>
      cases a with
      | none =>
          simp [stepShared, cntVals, sumVals, sumSqVals, validValues, ih]
      | some x =>
          simp [stepShared, cntVals, sumVals, sumSqVals, validValues, ih]
          constructor <;> omega

private theorem buildShared_eq (xs : List (Option Int)) :
    buildShared xs = { n := cntVals xs, sum := sumVals xs, sumSq := sumSqVals xs } := by
  unfold buildShared
  rw [foldl_stepShared_eq]
  simp

private theorem scaled_sq_identity (vals : List Int) (N S : Int) :
    (vals.map (fun x => (N * x - S) * (N * x - S))).sum =
      N * N * (vals.map (fun x => x * x)).sum - N * S * vals.sum -
        S * N * vals.sum + Int.ofNat vals.length * S * S := by
  induction vals with
  | nil => simp
  | cons x xs ih =>
      simp [ih]
      simp only [Int.mul_add, Int.sub_eq_add_neg, Int.mul_neg, Int.neg_neg, Int.add_assoc,
        Int.add_left_comm, Int.add_comm, Int.mul_comm, Int.mul_left_comm, Int.mul_one,
        Int.neg_add]

private theorem chan_identity_vals (vals : List Int) :
    Int.ofNat vals.length *
        (Int.ofNat vals.length * (vals.map (fun x => x * x)).sum - vals.sum * vals.sum)
      = (vals.map (fun x =>
          (Int.ofNat vals.length * x - vals.sum) *
          (Int.ofNat vals.length * x - vals.sum))).sum := by
  rw [scaled_sq_identity vals (Int.ofNat vals.length) vals.sum]
  rw [Int.mul_sub]
  simp only [Int.sub_eq_add_neg, Int.mul_assoc, Int.add_comm, Int.mul_comm, Int.mul_left_comm]
  omega

private theorem chan_identity (xs : List (Option Int)) :
    Int.ofNat (cntVals xs) * chanM2Num xs = sumScaledSqDevs xs := by
  simpa [chanM2Num, sumScaledSqDevs, cntVals, sumVals, sumSqVals] using
    chan_identity_vals (validValues xs)

private theorem chan_merge_int (n1 n2 s1 s2 q1 q2 : Int) :
    (n1 * n2) * ((n1 + n2) * (q1 + q2) - (s1 + s2) * (s1 + s2)) =
      (n2 * (n1 + n2)) * (n1 * q1 - s1 * s1) +
        (n1 * (n1 + n2)) * (n2 * q2 - s2 * s2) +
          (n2 * s1 - n1 * s2) * (n2 * s1 - n1 * s2) := by
  simp only [Int.mul_add, Int.sub_eq_add_neg, Int.mul_neg, Int.add_assoc, Int.add_left_comm,
    Int.mul_comm, Int.mul_left_comm, Int.neg_add]
  omega

private theorem chan_merge_vals (v1 v2 : List Int) :
    let n1 : Int := Int.ofNat v1.length
    let n2 : Int := Int.ofNat v2.length
    let s1 : Int := v1.sum
    let s2 : Int := v2.sum
    let q1 : Int := (v1.map (fun x => x * x)).sum
    let q2 : Int := (v2.map (fun x => x * x)).sum
    (n1 * n2) * ((n1 + n2) * (q1 + q2) - (s1 + s2) * (s1 + s2)) =
      (n2 * (n1 + n2)) * (n1 * q1 - s1 * s1) +
        (n1 * (n1 + n2)) * (n2 * q2 - s2 * s2) +
          (n2 * s1 - n1 * s2) * (n2 * s1 - n1 * s2) := by
  dsimp
  exact chan_merge_int _ _ _ _ _ _

private theorem chan_merge (xs ys : List (Option Int)) :
    let n1 := cntVals xs
    let n2 := cntVals ys
    let s1 := sumVals xs
    let s2 := sumVals ys
    Int.ofNat (n1 * n2) * chanM2Num (xs ++ ys) =
      Int.ofNat (n2 * (n1 + n2)) * chanM2Num xs +
        Int.ofNat (n1 * (n1 + n2)) * chanM2Num ys +
          (Int.ofNat n2 * s1 - Int.ofNat n1 * s2) *
            (Int.ofNat n2 * s1 - Int.ofNat n1 * s2) := by
  dsimp
  simpa [chanM2Num, cntVals, sumVals, sumSqVals, validValues] using
    chan_merge_vals (validValues xs) (validValues ys)

theorem sound : TinylambOpt.ResearchGoals.ChanWelfordSharedVarianceState.Goal := by
  constructor
  · intro xs ys
    constructor
    · rw [buildShared_eq, buildShared_eq, buildShared_eq]
      simp [mergeShared, cntVals, sumVals, sumSqVals, validValues]
    · constructor
      · exact chan_identity xs
      · exact chan_merge xs ys
  · constructor
    · simp [buildShared, finishAvgRat, finishVarSampRat]
    · intro x
      simp [buildShared, stepShared, finishAvgRat, finishVarSampRat]

end TinylambOpt.ResearchProofs.ChanWelfordSharedVarianceState
