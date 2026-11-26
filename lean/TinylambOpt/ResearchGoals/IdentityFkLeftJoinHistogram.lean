import Std

/-!
Right-side row `(fk, val_for_count, pred_pass)`.

Optimized right-only single pass: never touches left table `1..N`, accumulates non-null
counts only for valid in-range keys `[1..N]`, then synthesizes `hist(0) = N -
nonzeroKeys`.

Modeled claims

- `Goal` — Identity `1..N` left-outer-join histogram shortcut (TPC-H Q13
  `customer(is_identity_1_to_n)` ⟕ `orders`): scanning only `R` with filter +
  non-null value check and computing `hist(0) = N - nonzeroKeys` matches
  materializing the full `LEFT OUTER JOIN` and double `GROUP BY`.
-/

namespace TinylambOpt.ResearchGoals.IdentityFkLeftJoinHistogram

/-- Right-side row `(fk, val_for_count, pred_pass)`. -/
structure RRow where
  fk : Option Int
  val : Option Int
  pred : Bool
  deriving DecidableEq

def refPerKeyCounts (n : Nat) (rs : List RRow) : List Nat :=
  let filteredR := rs.filter RRow.pred
  (List.range n).map fun i =>
    let lk : Int := Int.ofNat (i + 1)
    (filteredR.filter (fun r => r.fk == some lk && r.val.isSome)).length

def refHist (n : Nat) (rs : List RRow) (c : Nat) : Nat :=
  ((refPerKeyCounts n rs).filter (· == c)).length

def incSlot (arr : List Nat) (idx : Nat) : List Nat :=
  arr.zipIdx.map fun (v, i) => if i == idx then v + 1 else v

/-- Optimized right-only single pass: never touches left table `1..N`, accumulates
    non-null counts only for valid in-range keys `[1..N]`, then synthesizes
    `hist(0) = N - nonzeroKeys`. -/
def optHist (n : Nat) (rs : List RRow) (c : Nat) : Nat :=
  let counts :=
    rs.foldl
      (fun acc r =>
        if !r.pred || !r.val.isSome then acc
        else
          match r.fk with
          | some k =>
              if 1 ≤ k && k ≤ Int.ofNat n then
                incSlot acc (k.toNat - 1)
              else acc
          | none => acc)
      (List.replicate n 0)
  let nonZero := counts.filter (0 < ·)
  if c == 0 then n - nonZero.length
  else (nonZero.filter (· == c)).length

/-- Identity `1..N` left-outer-join histogram shortcut (TPC-H Q13
    `customer(is_identity_1_to_n)` ⟕ `orders`): scanning only `R` with filter +
    non-null value check and computing `hist(0) = N - nonzeroKeys` matches
    materializing the full `LEFT OUTER JOIN` and double `GROUP BY`. -/
def Goal : Prop :=
  ∀ (n : Nat) (rs : List RRow) (c : Nat),
    optHist n rs c = refHist n rs c

private def sampleR : List RRow :=
  [ { fk := some 1, val := some 10, pred := true }
  , { fk := some 1, val := some 20, pred := true }
  , { fk := some 1, val := none, pred := true }
  , { fk := some 2, val := none, pred := true }
  , { fk := some 3, val := some 30, pred := false }
  , { fk := some 99, val := some 40, pred := true }
  , { fk := none, val := some 50, pred := true }
  ]

example :
    optHist 4 sampleR 0 = 3 ∧ refHist 4 sampleR 0 = 3 ∧
    optHist 4 sampleR 2 = 1 ∧ refHist 4 sampleR 2 = 1 := by
  decide

example :
    optHist 0 sampleR 0 = 0 ∧ refHist 0 sampleR 0 = 0 := by
  decide

end TinylambOpt.ResearchGoals.IdentityFkLeftJoinHistogram
