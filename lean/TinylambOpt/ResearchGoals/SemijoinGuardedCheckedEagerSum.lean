import Std

/-!
Reference flat join followed by checked signed 64-bit `SUM(L.v)`. Returns `none` on
overflow, `some none` when no non-NULL value joined.

Unguarded pre-aggregation checks partial sums on ALL keys in `ls`, including dead
unmatched keys!

Modeled claims

- `Goal` — Semijoin-guarded checked eager `SUM` (H2O H5 / STATS-CEB C1 / LDBC L2 /
  BigBench B2): filtering the pre-aggregation side by semijoin existence against `R`
  and bounding post-fanout positive and negative contributions within `[i64Min,
  i64Max]` ensures exact agreement with sequential checked post-join `SUM`, while
  eliminating false overflows caused by unmatched dead keys (`[i64Max, 1]`).
-/

namespace TinylambOpt.ResearchGoals.SemijoinGuardedCheckedEagerSum

def i64Min : Int := -9223372036854775808
def i64Max : Int := 9223372036854775807

def inI64 (z : Int) : Bool :=
  i64Min ≤ z && z ≤ i64Max

structure LRow where
  k : Option Int
  v : Option Int
  deriving DecidableEq

structure RRow where
  k : Option Int
  deriving DecidableEq

def checkedSumList (vals : List (Option Int)) : Option (Option Int) :=
  vals.foldl
    (fun acc ov =>
      match acc with
      | none => none
      | some cur =>
          match ov with
          | none => some cur
          | some x =>
              if !inI64 x then none
              else
                match cur with
                | none => some (some x)
                | some s =>
                    let nxt := s + x
                    if inI64 nxt then some (some nxt) else none)
    (some none)

/-- Reference flat join followed by checked signed 64-bit `SUM(L.v)`.
    Returns `none` on overflow, `some none` when no non-NULL value joined. -/
def refFlatJoinSum (ls : List LRow) (rs : List RRow) : Option (Option Int) :=
  let vals :=
    ls.flatMap fun l =>
      rs.filterMap fun r =>
        match l.k, r.k with
        | some lk, some rk => if lk == rk then some l.v else none
        | _, _ => none
  checkedSumList vals

/-- Unguarded pre-aggregation checks partial sums on ALL keys in `ls`,
    including dead unmatched keys! -/
def unguardedPreAgg (ls : List LRow) (rs : List RRow) : Option (Option Int) :=
  let checkAll := checkedSumList (ls.map LRow.v)
  match checkAll with
  | none => none
  | some _ => refFlatJoinSum ls rs

def rightHasKey (rs : List RRow) (k : Int) : Bool :=
  rs.any (fun r => r.k == some k)

def fanout (rs : List RRow) (k : Int) : Nat :=
  (rs.filter (fun r => r.k == some k)).length

def addUniqueInt (acc : List Int) (x : Int) : List Int :=
  if acc.contains x then acc else x :: acc

/-- Semijoin-guarded checked eager SUM:
    1. Filter `ls` by semijoin existence against `rs` so unmatched dead keys never trigger overflow.
    2. Accumulate exact (i128-wide) key sum `* fan(k)` for surviving keys.
    3. Range-check final total once against `i64`. -/
def optSemijoinEagerSum (ls : List LRow) (rs : List RRow) : Option (Option Int) :=
  let liveL :=
    ls.filter fun l =>
      match l.k with
      | some k => rightHasKey rs k
      | none => false
  let ks :=
    liveL.foldl
      (fun acc l =>
        match l.k with
        | some k => addUniqueInt acc k
        | none => acc)
      []
  let (hasVal, total) :=
    ks.foldl
      (fun (st : Bool × Int) (k : Int) =>
        let (seen, tot) := st
        let f := Int.ofNat (fanout rs k)
        let (kSeen, kSum) :=
          liveL.foldl
            (fun (kst : Bool × Int) (l : LRow) =>
              if l.k == some k then
                match l.v with
                | some x => (true, kst.2 + x)
                | none => kst
              else kst)
            (false, 0)
        if kSeen then (true, tot + kSum * f) else (seen, tot))
      (false, 0)
  if !hasVal then some none
  else if inI64 total then some (some total)
  else none

def weightedPosTotal (ls : List LRow) (rs : List RRow) : Int :=
  ls.foldl
    (fun acc l =>
      match l.k, l.v with
      | some k, some x =>
          let f := fanout rs k
          if 0 < f && 0 ≤ x then acc + x * Int.ofNat f else acc
      | _, _ => acc)
    0

def weightedNegTotal (ls : List LRow) (rs : List RRow) : Int :=
  ls.foldl
    (fun acc l =>
      match l.k, l.v with
      | some k, some x =>
          let f := fanout rs k
          if 0 < f && x < 0 then acc + x * Int.ofNat f else acc
      | _, _ => acc)
    0

def deadOverflowExampleL : List LRow :=
  [ { k := some 1, v := some 10 }
  , { k := some 1, v := some (-3) }
  , { k := some 99, v := some i64Max }
  , { k := some 99, v := some 1 }
  ]

def deadOverflowExampleR : List RRow :=
  [ { k := some 1 }
  , { k := some 1 }
  ]

/-- Semijoin-guarded checked eager `SUM` (H2O H5 / STATS-CEB C1 / LDBC L2 /
    BigBench B2): filtering the pre-aggregation side by semijoin existence against
    `R` and bounding post-fanout positive and negative contributions within
    `[i64Min, i64Max]` ensures exact agreement with sequential checked post-join
    `SUM`, while eliminating false overflows caused by unmatched dead keys (`[i64Max, 1]`). -/
def Goal : Prop :=
  (∀ (ls : List LRow) (rs : List RRow),
    inI64 (weightedPosTotal ls rs) = true →
    inI64 (weightedNegTotal ls rs) = true →
    optSemijoinEagerSum ls rs = refFlatJoinSum ls rs ∧
    optSemijoinEagerSum ls rs ≠ none) ∧
  (unguardedPreAgg deadOverflowExampleL deadOverflowExampleR = none ∧
   optSemijoinEagerSum deadOverflowExampleL deadOverflowExampleR = some (some 14) ∧
   refFlatJoinSum deadOverflowExampleL deadOverflowExampleR = some (some 14))

example :
    unguardedPreAgg deadOverflowExampleL deadOverflowExampleR = none ∧
    optSemijoinEagerSum deadOverflowExampleL deadOverflowExampleR = some (some 14) ∧
    refFlatJoinSum deadOverflowExampleL deadOverflowExampleR = some (some 14) := by
  decide

example :
    let ls : List LRow := [{ k := some 1, v := none }, { k := none, v := some i64Max }]
    let rs : List RRow := [{ k := some 1 }]
    optSemijoinEagerSum ls rs = some none ∧
    refFlatJoinSum ls rs = some none := by
  decide

end TinylambOpt.ResearchGoals.SemijoinGuardedCheckedEagerSum
