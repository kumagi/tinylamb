import Std

/-!
These claims relate a widened total to the bounds of a checked integer representation.
SQL can choose a narrower aggregate path under the required range guarantee.

Modeled claims

- `Goal` — Parallel `exact_integer_total` + O(1) `constBlock` fold
  (`executor/detail/fused_grouped_join.cpp`): when the sum of per-block absolute
  magnitude bounds fits strictly within `int64Max` (`2^63 - 1`), associative
  block-parallel summation and constant multiplication cannot overflow in any
  evaluation order and match sequential checked SQL `SUM`.
-/

namespace TinylambOpt.ResearchGoals.ExactIntTotalOverflowGuard

def int64Min : Int := -9223372036854775808
def int64Max : Int := 9223372036854775807

inductive Block where
  | constBlock (cnt : Nat) (val : Option Int)
  | flatBlock (xs : List (Option Int)) (maxAbs : Nat)
  deriving DecidableEq

def blockToRows : Block → List (Option Int)
  | .constBlock cnt val => List.replicate cnt val
  | .flatBlock xs _ => xs

def nonNullCount (xs : List (Option Int)) : Nat :=
  (xs.filter Option.isSome).length

def ValidBlock : Block → Prop
  | .constBlock _ none => True
  | .constBlock _ (some v) => int64Min ≤ v ∧ v ≤ int64Max
  | .flatBlock xs maxAbs =>
      ∀ v : Int, some v ∈ xs → v.natAbs ≤ maxAbs

def blockAbsBound : Block → Nat
  | .constBlock _ none => 0
  | .constBlock cnt (some v) => cnt * v.natAbs
  | .flatBlock xs maxAbs => nonNullCount xs * maxAbs

inductive CheckedResult where
  | ok (val : Option Int)
  | overflow
  deriving DecidableEq

def checkedAddInt64 (a b : Int) : CheckedResult :=
  let s := a + b
  if decide (int64Min ≤ s ∧ s ≤ int64Max) then .ok s else .overflow

def refCheckedSum (rows : List (Option Int)) : CheckedResult :=
  rows.foldl
    (fun res ov =>
      match res with
      | .overflow => .overflow
      | .ok acc =>
          match ov with
          | none => .ok acc
          | some v =>
              match acc with
              | none =>
                  if decide (int64Min ≤ v ∧ v ≤ int64Max) then .ok (some v)
                  else .overflow
              | some cur =>
                  match checkedAddInt64 cur v with
                  | .overflow => .overflow
                  | .ok (some s) => .ok (some s)
                  | .ok none => .overflow)
    (.ok none)

def evalBlockUnchecked : Block → Option Int
  | .constBlock 0 _ => none
  | .constBlock _ none => none
  | .constBlock (n + 1) (some v) => some (Int.ofNat (n + 1) * v)
  | .flatBlock xs _ =>
      xs.foldl
        (fun acc ov =>
          match ov with
          | none => acc
          | some v => some (acc.getD 0 + v))
        none

def combineOptSum (a b : Option Int) : Option Int :=
  match a, b with
  | none, y => y
  | x, none => x
  | some x, some y => some (x + y)

def parExactSum (bs : List Block) : Option Int :=
  bs.foldl (fun acc b => combineOptSum acc (evalBlockUnchecked b)) none

/-- Parallel `exact_integer_total` + O(1) `constBlock` fold
    (`executor/detail/fused_grouped_join.cpp`): when the sum of per-block
    absolute magnitude bounds fits strictly within `int64Max` (`2^63 - 1`),
    associative block-parallel summation and constant multiplication cannot
    overflow in any evaluation order and match sequential checked SQL `SUM`. -/
def Goal : Prop :=
  ∀ (bs : List Block),
    (∀ b ∈ bs, ValidBlock b) →
    (bs.map blockAbsBound).sum ≤ int64Max.toNat →
    CheckedResult.ok (parExactSum bs) = refCheckedSum (bs.flatMap blockToRows)

private def bConst : Block := .constBlock 4 (some 100)
private def bNull : Block := .constBlock 3 none
private def bFlat : Block := .flatBlock [none, some (-50), some 20] 50

example :
    CheckedResult.ok (parExactSum [bConst, bNull, bFlat]) =
      refCheckedSum ([bConst, bNull, bFlat].flatMap blockToRows) ∧
    parExactSum [bConst, bNull, bFlat] = some 370 := by
  decide

example :
    let overflowBlocks : List Block :=
      [.flatBlock [some int64Max] int64Max.toNat, .flatBlock [some 1] 1, .flatBlock [some (-1)] 1]
    parExactSum overflowBlocks = some int64Max ∧
    refCheckedSum (overflowBlocks.flatMap blockToRows) = .overflow := by
  decide

end TinylambOpt.ResearchGoals.ExactIntTotalOverflowGuard
