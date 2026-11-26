import Std

/-!
Reference hash-based group aggregate for key pair `(k1, k2)`: returns `none` if the
group never appeared, or `some (cntStar, sumOpt)`.

Maps `Option Int` key onto flat dimension index `[0..span]`, sending `none` (`NULL`) to
the dedicated sentinel index `span`.

Modeled claims

- `Goal` — Bounded 2-key dense direct-array parallel aggregation with NULL sentinel
  slots (H2O H2, H4 / LDBC L2 / DictionaryJoinsAndLateLabels B3 / STATS-CEB C5):
  mapping bounded non-NULL keys onto `[0..span-1]` and `NULL` onto index `span`
  produces a collision-free 2D table per worker whose elementwise merge matches full
  hash-based `GROUP BY (k1, k2)` across empty, zero-canceling, and NULL-key groups.
-/

namespace TinylambOpt.ResearchGoals.BoundedMultiKeyDirectArrayAggregate

structure InputRow where
  k1 : Option Int
  k2 : Option Int
  v : Option Int
  deriving DecidableEq

structure Slot where
  seen : Bool := false
  cntStar : Nat := 0
  nnCnt : Nat := 0
  sumVal : Int := 0
  deriving DecidableEq

def KeysInBounds (rows : List InputRow) (min1 : Int) (span1 : Nat)
    (min2 : Int) (span2 : Nat) : Prop :=
  ∀ r : InputRow, r ∈ rows →
    (match r.k1 with
     | none => True
     | some a => min1 ≤ a ∧ a < min1 + Int.ofNat span1) ∧
    (match r.k2 with
     | none => True
     | some b => min2 ≤ b ∧ b < min2 + Int.ofNat span2)

def sumOptList (vals : List (Option Int)) : Option Int :=
  vals.foldl
    (fun acc v =>
      match acc, v with
      | none, y => y
      | x, none => x
      | some a, some b => some (a + b))
    none

/-- Reference hash-based group aggregate for key pair `(k1, k2)`:
    returns `none` if the group never appeared, or `some (cntStar, sumOpt)`. -/
def refGroupAgg (rows : List InputRow) (k1 k2 : Option Int) :
    Option (Nat × Option Int) :=
  let matching := rows.filter (fun r => r.k1 == k1 && r.k2 == k2)
  if matching.isEmpty then none
  else some (matching.length, sumOptList (matching.map InputRow.v))

/-- Maps `Option Int` key onto flat dimension index `[0..span]`, sending
    `none` (`NULL`) to the dedicated sentinel index `span`. -/
def slotCoord (minK : Int) (span : Nat) (ok : Option Int) : Nat :=
  match ok with
  | none => span
  | some k => (k - minK).toNat

def incFlatSlot (arr : List Slot) (idx : Nat) (ov : Option Int) : List Slot :=
  arr.zipIdx.map fun (s, i) =>
    if i == idx then
      match ov with
      | none => { s with seen := true, cntStar := s.cntStar + 1 }
      | some x =>
          { seen := true
            cntStar := s.cntStar + 1
            nnCnt := s.nnCnt + 1
            sumVal := s.sumVal + x }
    else s

def localFlatBuild (min1 : Int) (span1 : Nat) (min2 : Int) (span2 : Nat)
    (rows : List InputRow) : List Slot :=
  let totalSlots := (span1 + 1) * (span2 + 1)
  let init := List.replicate totalSlots {}
  rows.foldl
    (fun arr r =>
      let i1 := slotCoord min1 span1 r.k1
      let i2 := slotCoord min2 span2 r.k2
      let flatIdx := i1 * (span2 + 1) + i2
      incFlatSlot arr flatIdx r.v)
    init

def mergeSlot (a b : Slot) : Slot :=
  { seen := a.seen || b.seen
    cntStar := a.cntStar + b.cntStar
    nnCnt := a.nnCnt + b.nnCnt
    sumVal := a.sumVal + b.sumVal }

/-- Parallel two-worker flat-array 2-key direct aggregation with NULL sentinel
    slots and elementwise vector addition merge. -/
def optParallelFlatAgg (min1 : Int) (span1 : Nat) (min2 : Int) (span2 : Nat)
    (rowsA rowsB : List InputRow) (k1 k2 : Option Int) :
    Option (Nat × Option Int) :=
  let arrA := localFlatBuild min1 span1 min2 span2 rowsA
  let arrB := localFlatBuild min1 span1 min2 span2 rowsB
  let merged := (arrA.zip arrB).map (fun (sa, sb) => mergeSlot sa sb)
  let idx := slotCoord min1 span1 k1 * (span2 + 1) + slotCoord min2 span2 k2
  let s := merged.getD idx {}
  if !s.seen then none
  else some (s.cntStar, if s.nnCnt == 0 then none else some s.sumVal)

/-- Bounded 2-key dense direct-array parallel aggregation with NULL sentinel
    slots (H2O H2, H4 / LDBC L2 / DictionaryJoinsAndLateLabels B3 / STATS-CEB C5): mapping bounded
    non-NULL keys onto `[0..span-1]` and `NULL` onto index `span` produces a
    collision-free 2D table per worker whose elementwise merge matches full
    hash-based `GROUP BY (k1, k2)` across empty, zero-canceling, and NULL-key
    groups. -/
def Goal : Prop :=
  ∀ (min1 : Int) (span1 : Nat) (min2 : Int) (span2 : Nat)
    (rowsA rowsB : List InputRow),
    KeysInBounds (rowsA ++ rowsB) min1 span1 min2 span2 →
    ∀ (k1 k2 : Option Int),
      (match k1 with | none => True | some a => min1 ≤ a ∧ a < min1 + Int.ofNat span1) →
      (match k2 with | none => True | some b => min2 ≤ b ∧ b < min2 + Int.ofNat span2) →
      optParallelFlatAgg min1 span1 min2 span2 rowsA rowsB k1 k2 =
        refGroupAgg (rowsA ++ rowsB) k1 k2

example :
    let rowsA : List InputRow :=
      [ { k1 := none, k2 := some 0, v := some 5 }
      , { k1 := some 1, k2 := some (-1), v := none }
      , { k1 := some 1, k2 := some 0, v := some 10 }
      ]
    let rowsB : List InputRow :=
      [ { k1 := none, k2 := some 0, v := some (-5) }
      , { k1 := some 1, k2 := none, v := some 7 }
      ]
    let keys : List (Option Int × Option Int) :=
      [(none, some 0), (some 1, some (-1)), (some 1, some 0), (some 1, none), (some 2, some 0)]
    keys.all (fun (k1, k2) =>
      optParallelFlatAgg 1 2 (-1) 2 rowsA rowsB k1 k2 ==
        refGroupAgg (rowsA ++ rowsB) k1 k2) = true := by
  decide

example :
    optParallelFlatAgg 1 2 (-1) 2 [] [] none (some 0) = none ∧
    optParallelFlatAgg 0 2 0 2 [{ k1 := some 0, k2 := some 0, v := some 5 }]
      [{ k1 := some 0, k2 := some 0, v := some (-5) }] (some 0) (some 0) = some (2, some 0) := by
  decide

end TinylambOpt.ResearchGoals.BoundedMultiKeyDirectArrayAggregate
