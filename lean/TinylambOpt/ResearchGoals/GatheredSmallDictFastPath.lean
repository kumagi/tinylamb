import Std

/-!
Sharing sentinel `0` between NULL and unreferenced/first IDs merges NULL rows into group
`dict[0]`.

Modeled claims

- `Goal` — Compact gathered dictionary remap (`executor/detail/fused_grouped_join.cpp`
  `left_pack` `kGathered` `total_rows ≤ 4096`): remapping active canonical dictionary
  IDs to dense IDs `0..m-1` while keeping `none` (NULL) separate from unreferenced
  entries preserves grouped `SUM` for every key `k`.
-/

namespace TinylambOpt.ResearchGoals.GatheredSmallDictFastPath

def addAssoc {α : Type} [DecidableEq α] (m : List (α × Int)) (k : α) (v : Int) :
    List (α × Int) :=
  match m with
  | [] => [(k, v)]
  | (k', s) :: rest =>
      if k' == k then (k', s + v) :: rest
      else (k', s) :: addAssoc rest k v

def lookupAssoc {α : Type} [DecidableEq α] (m : List (α × Int)) (k : α) : Option Int :=
  match m with
  | [] => none
  | (k', s) :: rest => if k' == k then some s else lookupAssoc rest k

def refDecodeRow (dict : List Int) (codes : List (Option Nat)) (rowIdx : Nat) :
    Option Int :=
  match codes[rowIdx]? with
  | some (some cid) => dict[cid]?
  | _ => none

def refGroupedSum (dict : List Int) (codes : List (Option Nat))
    (sel : List Nat) (vals : List Int) : List (Option Int × Int) :=
  sel.foldl
    (fun acc rowIdx =>
      let key := refDecodeRow dict codes rowIdx
      let v := vals[rowIdx]?.getD 0
      addAssoc acc key v)
    []

structure CompactAgg where
  compactDict : List Int
  compactSums : List (Nat × Int)
  nullSum : Option Int
  deriving DecidableEq

def compactGroupedSum (dict : List Int) (codes : List (Option Nat))
    (sel : List Nat) (vals : List Int) : CompactAgg :=
  let usedCodes :=
    (sel.filterMap fun rowIdx =>
      match codes[rowIdx]? with
      | some (some cid) => some cid
      | _ => none).eraseDups
  let compactDict := usedCodes.filterMap (fun cid => dict[cid]?)
  let init : List (Nat × Int) × Option Int := ([], none)
  let (cSums, nSum) :=
    sel.foldl
      (fun (cm, ns) rowIdx =>
        let v := vals[rowIdx]?.getD 0
        match codes[rowIdx]? with
        | some (some origCid) =>
            match usedCodes.findIdx? (· == origCid) with
            | some newId => (addAssoc cm newId v, ns)
            | none => (cm, ns)
        | _ => (cm, some (ns.getD 0 + v)))
      init
  ⟨compactDict, cSums, nSum⟩

def queryCompactSum (agg : CompactAgg) (k : Option Int) : Option Int :=
  match k with
  | none => agg.nullSum
  | some target =>
      match agg.compactDict.findIdx? (· == target) with
      | none => none
      | some newId => lookupAssoc agg.compactSums newId

/-- Compact gathered dictionary remap (`executor/detail/fused_grouped_join.cpp`
    `left_pack` `kGathered` `total_rows ≤ 4096`): remapping active canonical
    dictionary IDs to dense IDs `0..m-1` while keeping `none` (NULL) separate
    from unreferenced entries preserves grouped `SUM` for every key `k`. -/
def Goal : Prop :=
  ∀ (dict : List Int) (codes : List (Option Nat))
    (sel : List Nat) (vals : List Int) (k : Option Int),
    dict.Nodup →
    (∀ rowIdx ∈ sel,
      rowIdx < codes.length ∧ rowIdx < vals.length ∧
      match codes[rowIdx]? with
      | some (some cid) => cid < dict.length
      | some none => true
      | none => false) →
    queryCompactSum (compactGroupedSum dict codes sel vals) k =
      lookupAssoc (refGroupedSum dict codes sel vals) k

/-- Sharing sentinel `0` between NULL and unreferenced/first IDs merges NULL
    rows into group `dict[0]`. -/
def badSentinelMap (dict : List Int) (codes : List (Option Nat))
    (sel : List Nat) (vals : List Int) : List (Int × Int) :=
  sel.foldl
    (fun acc rowIdx =>
      let v := vals[rowIdx]?.getD 0
      let cid := (codes[rowIdx]?.join).getD 0
      match dict[cid]? with
      | some dv => addAssoc acc dv v
      | none => acc)
    []

private def exDict : List Int := [100, 200, 300]
private def exCodes : List (Option Nat) := [some 2, none, some 2, some 0]
private def exSel : List Nat := [0, 1, 2]
private def exVals : List Int := [10, 5, 20, 99]

example :
    let agg := compactGroupedSum exDict exCodes exSel exVals
    queryCompactSum agg (some 300) = some 30 ∧
    queryCompactSum agg none = some 5 ∧
    queryCompactSum agg (some 100) = none := by
  decide

example :
    lookupAssoc (badSentinelMap exDict exCodes exSel exVals) 100 = some 5 := by
  decide

end TinylambOpt.ResearchGoals.GatheredSmallDictFastPath
