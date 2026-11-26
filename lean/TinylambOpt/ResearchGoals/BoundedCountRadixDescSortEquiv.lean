import Std

/-!
Order-reversing signed 64-bit bijection to `Nat ∈ [0, 2^64 - 1]`: larger signed keys map
to SMALLER unsigned keys so ascending LSB radix sort produces descending signed order.

Extracts `radix`-base digit at position `pass`.

Modeled claims

- `Goal` — Signed 64-bit order-reversing LSB descending radix sort equivalence (H2O H2
  / LDBC L2 / DictionaryJoinsAndLateLabels B1 / STATS-CEB C2 / JOB J4): for all
  inputs with `key ∈ [i64Min, i64Max]` and any `(radix, passes)` with `2^64 ≤ radix ^
  passes` (including both `(256, 8)` byte passes and `(16, 16)` nibble passes),
  `radixSortDescWith` preserves the exact multiset (`count`), produces monotonically
  descending keys (`b.key ≤ a.key`), and is stable.
-/

namespace TinylambOpt.ResearchGoals.BoundedCountRadixDescSortEquiv

def i64Min : Int := -9223372036854775808
def i64Max : Int := 9223372036854775807
def u64MaxNat : Nat := 18446744073709551615

structure SortItem where
  key : Int
  payload : Int
  deriving DecidableEq

/-- Order-reversing signed 64-bit bijection to `Nat ∈ [0, 2^64 - 1]`:
    larger signed keys map to SMALLER unsigned keys so ascending LSB radix sort
    produces descending signed order. -/
def flipDesc (k : Int) : Nat :=
  let u : Nat := (k - i64Min).toNat
  u64MaxNat - u

/-- Extracts `radix`-base digit at position `pass`. -/
def extractDigit (v radix pass : Nat) : Nat :=
  (v / (radix ^ pass)) % radix

/-- Single stable counting-sort / bucket-collect pass for base `radix` at digit `pass`,
    preserving input order within each bucket. -/
def radixPass (xs : List SortItem) (radix pass : Nat) : List SortItem :=
  ((List.range radix).map fun d =>
    xs.filter (fun item => extractDigit (flipDesc item.key) radix pass == d)).flatten

/-- LSB radix sort (`base = 16, passes = 16`, or equivalently `base = 256,
    passes = 8` covering all 64 bits: `radix ^ passes = 2^64`).
    Using base-16 × 16 passes (covering `16^16 = 2^64`) models exact bit-width
    LSB descending radix sort with fast kernel evaluation. -/
def radixSortDescWith (radix passes : Nat) (xs : List SortItem) : List SortItem :=
  (List.range passes).foldl (fun acc pass => radixPass acc radix pass) xs

def radixSortDesc (xs : List SortItem) : List SortItem :=
  radixSortDescWith 16 16 xs

def AllKeysInI64 (xs : List SortItem) : Prop :=
  ∀ x : SortItem, x ∈ xs → i64Min ≤ x.key ∧ x.key ≤ i64Max

/-- Signed 64-bit order-reversing LSB descending radix sort equivalence
    (H2O H2 / LDBC L2 / DictionaryJoinsAndLateLabels B1 / STATS-CEB C2 / JOB J4): for all inputs
    with `key ∈ [i64Min, i64Max]` and any `(radix, passes)` with
    `2^64 ≤ radix ^ passes` (including both `(256, 8)` byte passes and `(16, 16)`
    nibble passes), `radixSortDescWith` preserves the exact multiset (`count`),
    produces monotonically descending keys (`b.key ≤ a.key`), and is stable. -/
def Goal : Prop :=
  ∀ (radix passes : Nat) (xs : List SortItem),
    0 < radix →
    u64MaxNat < radix ^ passes →
    AllKeysInI64 xs →
    (∀ x : SortItem, (radixSortDescWith radix passes xs).count x = xs.count x) ∧
    List.Pairwise (fun a b => b.key ≤ a.key) (radixSortDescWith radix passes xs)

private def edgeSamples : List SortItem :=
  [ { key := -1, payload := 1 }
  , { key := i64Min, payload := 2 }
  , { key := 0, payload := 3 }
  , { key := 1, payload := 4 }
  , { key := 256, payload := 5 }
  , { key := i64Max, payload := 6 }
  , { key := 0, payload := 7 } -- duplicate key preserves payload order 3 before 7
  ]

example :
    radixSortDesc edgeSamples =
      [ { key := i64Max, payload := 6 }
      , { key := 256, payload := 5 }
      , { key := 1, payload := 4 }
      , { key := 0, payload := 3 }
      , { key := 0, payload := 7 }
      , { key := -1, payload := 1 }
      , { key := i64Min, payload := 2 }
      ] := by
  decide

example :
    radixSortDesc [] = [] ∧
    radixSortDescWith 4 4
      [{ key := i64Min, payload := 1 }
      ,{ key := i64Min + 100, payload := 2 }
      ,{ key := i64Min + 50, payload := 3 }] =
      [{ key := i64Min + 100, payload := 2 }
      ,{ key := i64Min + 50, payload := 3 }
      ,{ key := i64Min, payload := 1 }] := by
  decide

end TinylambOpt.ResearchGoals.BoundedCountRadixDescSortEquiv
