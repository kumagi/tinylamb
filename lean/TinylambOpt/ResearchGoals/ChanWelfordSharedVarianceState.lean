import Std

namespace TinylambOpt.ResearchGoals.ChanWelfordSharedVarianceState

def validValues (xs : List (Option Int)) : List Int :=
  xs.filterMap id

def sumVals (xs : List (Option Int)) : Int :=
  (validValues xs).sum

def sumSqVals (xs : List (Option Int)) : Int :=
  ((validValues xs).map (fun x => x * x)).sum

def cntVals (xs : List (Option Int)) : Nat :=
  (validValues xs).length

/-- Exact integer numerator `n * ∑ x^2 - (∑ x)^2 = n^2 * Var_pop(X) * n`. -/
def chanM2Num (xs : List (Option Int)) : Int :=
  let n : Int := Int.ofNat (cntVals xs)
  let s := sumVals xs
  n * sumSqVals xs - s * s

def sumScaledSqDevs (xs : List (Option Int)) : Int :=
  let n : Int := Int.ofNat (cntVals xs)
  let s := sumVals xs
  ((validValues xs).map (fun x => (n * x - s) * (n * x - s))).sum

structure SharedState where
  n : Nat
  sum : Int
  sumSq : Int
  deriving DecidableEq

def stepShared (st : SharedState) : Option Int → SharedState
  | none => st
  | some x => { n := st.n + 1, sum := st.sum + x, sumSq := st.sumSq + x * x }

def buildShared (xs : List (Option Int)) : SharedState :=
  xs.foldl stepShared { n := 0, sum := 0, sumSq := 0 }

def mergeShared (a b : SharedState) : SharedState :=
  { n := a.n + b.n, sum := a.sum + b.sum, sumSq := a.sumSq + b.sumSq }

def finishCount (st : SharedState) : Nat := st.n

/-- Exact `(numerator, denominator)` for `AVG` (`none` iff `n = 0`). -/
def finishAvgRat (st : SharedState) : Option (Int × Nat) :=
  if st.n == 0 then none else some (st.sum, st.n)

/-- Exact `(numerator, denominator)` for `VAR_SAMP` (`none` iff `n ≤ 1`). -/
def finishVarSampRat (st : SharedState) : Option (Int × Nat) :=
  if st.n ≤ 1 then none
  else
    let n : Int := Int.ofNat st.n
    let m2Num := n * st.sumSq - st.sum * st.sum
    some (m2Num, st.n * (st.n - 1))

/-- Shared Chan/Welford statistical accumulator for `(COUNT, AVG, STDDEV_SAMP)`
    (TPC-DS Q17/Q39):
    (1) computing one shared tuple state yields exact `COUNT`, `AVG`, and
    `VAR_SAMP` (matching `n ≤ 1 → none`),
    (2) `n * sumSq - sum^2` equals the scaled sum of squared deviations
    `∑ (n * x - sum)^2 / n`, and
    (3) parallel chunks satisfy the exact integer Chan cross-term identity
    `n1 * n2 * M2_num(xs ++ ys) = n2*(n1+n2)*M2_num(xs) + n1*(n1+n2)*M2_num(ys) + (n2*s1 - n1*s2)^2`. -/
def Goal : Prop :=
  (∀ (xs ys : List (Option Int)),
    mergeShared (buildShared xs) (buildShared ys) = buildShared (xs ++ ys) ∧
    Int.ofNat (cntVals xs) * chanM2Num xs = sumScaledSqDevs xs ∧
    let n1 := cntVals xs
    let n2 := cntVals ys
    let s1 := sumVals xs
    let s2 := sumVals ys
    Int.ofNat (n1 * n2) * chanM2Num (xs ++ ys) =
      Int.ofNat (n2 * (n1 + n2)) * chanM2Num xs +
      Int.ofNat (n1 * (n1 + n2)) * chanM2Num ys +
      (Int.ofNat n2 * s1 - Int.ofNat n1 * s2) * (Int.ofNat n2 * s1 - Int.ofNat n1 * s2)) ∧
  (finishAvgRat (buildShared []) = none ∧ finishVarSampRat (buildShared []) = none) ∧
  (∀ x : Int,
    finishAvgRat (buildShared [some x]) = some (x, 1) ∧
    finishVarSampRat (buildShared [some x]) = none)

example :
    let xs : List (Option Int) := [none, some (-4), some 2, some 8]
    let ys : List (Option Int) := [some 10, none, some (-2)]
    mergeShared (buildShared xs) (buildShared ys) = buildShared (xs ++ ys) ∧
    Int.ofNat (cntVals xs) * chanM2Num xs = sumScaledSqDevs xs ∧
    let n1 := cntVals xs
    let n2 := cntVals ys
    let s1 := sumVals xs
    let s2 := sumVals ys
    Int.ofNat (n1 * n2) * chanM2Num (xs ++ ys) =
      Int.ofNat (n2 * (n1 + n2)) * chanM2Num xs +
      Int.ofNat (n1 * (n1 + n2)) * chanM2Num ys +
      (Int.ofNat n2 * s1 - Int.ofNat n1 * s2) * (Int.ofNat n2 * s1 - Int.ofNat n1 * s2) := by
  decide

example :
    finishAvgRat (buildShared [none]) = none ∧
    finishVarSampRat (buildShared [none]) = none ∧
    finishAvgRat (buildShared [some 7]) = some (7, 1) ∧
    finishVarSampRat (buildShared [some 7]) = none ∧
    finishVarSampRat (buildShared [some 2, some 4]) = some (4, 2) := by
  decide

end TinylambOpt.ResearchGoals.ChanWelfordSharedVarianceState
