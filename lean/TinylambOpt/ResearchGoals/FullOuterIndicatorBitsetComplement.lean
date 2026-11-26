import Std

/-!
Single unified pass using a 2-bit tagger on non-null pairs `(k1, k2)` plus explicit
tracking of partial-NULL distinct pairs `(some k1, none)`.

Modeled claims

- `Goal` — Single-table 2-bit indicator complement for `FULL OUTER JOIN` of two
  deduplicated streams (TPC-DS Q97 `ssci FULL OUTER JOIN csci`): counting `tag=1`,
  `tag=2`, `tag=3` in one unified non-null hash set plus `(some k1, none)`
  partial-NULL orphans reproduces exact `(store_only, catalog_only,
  store_and_catalog)` without materializing or joining two hash tables.
-/

namespace TinylambOpt.ResearchGoals.FullOuterIndicatorBitsetComplement

abbrev Key := Option Int × Option Int

def insertUniq (xs : List Key) (k : Key) : List Key :=
  if xs.contains k then xs else xs ++ [k]

def dedupKeys (xs : List Key) : List Key :=
  xs.foldl insertUniq []

def eqJoinKey (a b : Key) : Bool :=
  match a.1, a.2, b.1, b.2 with
  | some a1, some a2, some b1, some b2 => a1 == b1 && a2 == b2
  | _, _, _, _ => false

structure ThreeCounts where
  storeOnly : Nat
  catalogOnly : Nat
  storeAndCatalog : Nat
  deriving DecidableEq

def refFullOuterCounts (a b : List Key) : ThreeCounts :=
  let dA := dedupKeys a
  let dB := dedupKeys b
  let leftPairs : List (Option Int × Option Int) :=
    dA.flatMap fun ak =>
      let hits := dB.filter (eqJoinKey ak)
      if hits.isEmpty then [(ak.1, none)]
      else hits.map (fun bk => (ak.1, bk.1))
  let rightUnmatched : List (Option Int × Option Int) :=
    dB.filterMap fun bk =>
      if dA.any (fun ak => eqJoinKey ak bk) then none else some (none, bk.1)
  let allPairs := leftPairs ++ rightUnmatched
  { storeOnly := (allPairs.filter (fun (l, r) => l.isSome && r.isNone)).length
    catalogOnly := (allPairs.filter (fun (l, r) => l.isNone && r.isSome)).length
    storeAndCatalog := (allPairs.filter (fun (l, r) => l.isSome && r.isSome)).length }

/-- Single unified pass using a 2-bit tagger on non-null pairs `(k1, k2)` plus
    explicit tracking of partial-NULL distinct pairs `(some k1, none)`. -/
def optBitsetCounts (a b : List Key) : ThreeCounts :=
  let dA := dedupKeys a
  let dB := dedupKeys b
  let nnA := dA.filterMap fun | (some k1, some k2) => some (k1, k2) | _ => none
  let nnB := dB.filterMap fun | (some k1, some k2) => some (k1, k2) | _ => none
  let nullItemA :=
    (dA.filter (fun (k1, k2) => k1.isSome && k2.isNone)).length
  let nullItemB :=
    (dB.filter (fun (k1, k2) => k1.isSome && k2.isNone)).length
  let both := (nnA.filter (fun p => nnB.contains p)).length
  let leftNNOnly := (nnA.filter (fun p => !nnB.contains p)).length
  let rightNNOnly := (nnB.filter (fun p => !nnA.contains p)).length
  { storeOnly := leftNNOnly + nullItemA
    catalogOnly := rightNNOnly + nullItemB
    storeAndCatalog := both }

/-- Single-table 2-bit indicator complement for `FULL OUTER JOIN` of two
    deduplicated streams (TPC-DS Q97 `ssci FULL OUTER JOIN csci`): counting
    `tag=1`, `tag=2`, `tag=3` in one unified non-null hash set plus `(some k1,
    none)` partial-NULL orphans reproduces exact `(store_only, catalog_only,
    store_and_catalog)` without materializing or joining two hash tables. -/
def Goal : Prop :=
  ∀ (a b : List Key),
    optBitsetCounts a b = refFullOuterCounts a b

example :
    let a : List Key := [(some 1, some 10), (some 1, some 10), (some 2, none), (none, some 9)]
    let b : List Key := [(some 1, some 10), (some 3, some 20), (some 2, none), (none, none)]
    optBitsetCounts a b = refFullOuterCounts a b ∧
    optBitsetCounts a b = { storeOnly := 1, catalogOnly := 2, storeAndCatalog := 1 } := by
  decide

example :
    optBitsetCounts [] [(none, some 1), (none, none)] =
      refFullOuterCounts [] [(none, some 1), (none, none)] := by
  decide

end TinylambOpt.ResearchGoals.FullOuterIndicatorBitsetComplement
