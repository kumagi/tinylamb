import Std

/-!
Unchecked truncation to 32 bits aliases `(lo1 + 2^32, lo2)` with `(lo1, lo2)`.

Modeled claims

- `Goal` — Worker-local `packed_2x32` fast aggregation + NULL/out-of-range fallback
  merge (`executor/detail/fused_grouped_join.cpp` `packed_2x32`): fast-packed keys
  and fallback keys remain disjoint and exact across arbitrary worker splits and
  negative key bases.
-/

namespace TinylambOpt.ResearchGoals.PackedRadix2x32BatchAgg

def twoPow32 : Int := 4294967296

abbrev RawRow := Option Int × Option Int × Int
abbrev GroupKey := Option Int × Option Int

def tryPack (lo1 lo2 : Int) : GroupKey → Option (BitVec 64)
  | (some k1, some k2) =>
      if decide (0 ≤ k1 - lo1 ∧ k1 - lo1 < twoPow32 ∧ 0 ≤ k2 - lo2 ∧ k2 - lo2 < twoPow32) then
        let d1 : BitVec 64 := BitVec.ofNat 64 (k1 - lo1).toNat
        let d2 : BitVec 64 := BitVec.ofNat 64 (k2 - lo2).toNat
        some ((d1 <<< 32) ||| d2)
      else
        none
  | _ => none

def unpack (lo1 lo2 : Int) (pk : BitVec 64) : Int × Int :=
  let hi := (pk >>> 32).toNat
  let lo := (pk &&& 0xFFFFFFFF#64).toNat
  (lo1 + Int.ofNat hi, lo2 + Int.ofNat lo)

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

def mergeAssoc {α : Type} [DecidableEq α] (a b : List (α × Int)) : List (α × Int) :=
  b.foldl (fun acc kv => addAssoc acc kv.1 kv.2) a

def refGroupedSum (rows : List RawRow) : List (GroupKey × Int) :=
  rows.foldl (fun acc (k1, k2, v) => addAssoc acc (k1, k2) v) []

structure WorkerAgg where
  fast : List (BitVec 64 × Int) := []
  fallback : List (GroupKey × Int) := []
  deriving DecidableEq

def workerAccum (lo1 lo2 : Int) (rows : List RawRow) : WorkerAgg :=
  rows.foldl
    (fun st (k1, k2, v) =>
      match tryPack lo1 lo2 (k1, k2) with
      | some pk => { st with fast := addAssoc st.fast pk v }
      | none => { st with fallback := addAssoc st.fallback (k1, k2) v })
    {}

def mergedGroupAgg (lo1 lo2 : Int) (workers : List (List RawRow)) :
    List (GroupKey × Int) × List (GroupKey × Int) :=
  let aggs := workers.map (workerAccum lo1 lo2)
  let fastMerged := aggs.foldl (fun acc w => mergeAssoc acc w.fast) []
  let fbMerged := aggs.foldl (fun acc w => mergeAssoc acc w.fallback) []
  let fastDecoded := fastMerged.map fun (pk, s) =>
    let (k1, k2) := unpack lo1 lo2 pk
    ((some k1, some k2), s)
  (fastDecoded, fbMerged)

def queryMergedSum (lo1 lo2 : Int) (workers : List (List RawRow)) (g : GroupKey) :
    Option Int :=
  let (fastDecoded, fbMerged) := mergedGroupAgg lo1 lo2 workers
  match lookupAssoc fastDecoded g with
  | some s => some s
  | none => lookupAssoc fbMerged g

/-- Worker-local `packed_2x32` fast aggregation + NULL/out-of-range fallback
    merge (`executor/detail/fused_grouped_join.cpp` `packed_2x32`): fast-packed
    keys and fallback keys remain disjoint and exact across arbitrary worker
    splits and negative key bases. -/
def Goal : Prop :=
  (∀ (lo1 lo2 k1 k2 : Int) (pk : BitVec 64),
    tryPack lo1 lo2 (some k1, some k2) = some pk → unpack lo1 lo2 pk = (k1, k2)) ∧
  (∀ (lo1 lo2 : Int) (workers : List (List RawRow)),
    let (fastDecoded, fbMerged) := mergedGroupAgg lo1 lo2 workers
    ∀ p ∈ fastDecoded, ∀ q ∈ fbMerged, p.1 ≠ q.1) ∧
  (∀ (lo1 lo2 : Int) (workers : List (List RawRow)) (g : GroupKey),
    queryMergedSum lo1 lo2 workers g = lookupAssoc (refGroupedSum workers.flatten) g)

/-- Unchecked truncation to 32 bits aliases `(lo1 + 2^32, lo2)` with
    `(lo1, lo2)`. -/
def uncheckedPack (lo1 lo2 k1 k2 : Int) : BitVec 64 :=
  let d1 : BitVec 64 := BitVec.ofNat 64 (k1 - lo1).toNat
  let d2 : BitVec 64 := BitVec.ofNat 64 (k2 - lo2).toNat
  (d1 <<< 32) ||| d2

private def w1 : List RawRow :=
  [(some (-5), some 10, 3), (none, some 10, 4), (some twoPow32, some 10, 7)]
private def w2 : List RawRow :=
  [(some (-5), some 10, 5), (some twoPow32, some 10, 2), (some 0, some 10, 1)]

example :
    queryMergedSum (-10) 0 [w1, w2] (some (-5), some 10) = some 8 ∧
    queryMergedSum (-10) 0 [w1, w2] (none, some 10) = some 4 ∧
    queryMergedSum (-10) 0 [w1, w2] (some twoPow32, some 10) = some 9 := by
  decide

example :
    uncheckedPack 0 0 twoPow32 5 = uncheckedPack 0 0 0 5 := by
  decide

end TinylambOpt.ResearchGoals.PackedRadix2x32BatchAgg
