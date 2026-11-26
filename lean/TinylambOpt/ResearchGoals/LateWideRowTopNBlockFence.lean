import Std

/-!
Can block `b` be skipped because the heap already holds `k > 0` non-null entries and
`b.allNullSort || heapMax < b.minNonNullSort`?

Optimized two-stage Top-N: skip entire blocks fenced below `minNonNullSort` when `k`
non-null winners already exist, evaluate `filterPass` only on surviving blocks, and
decode wide `payload` only for final `≤ k` winners.

Modeled claims

- `Goal` — Block bitwise AND/OR point-lookup pruning (ClickBench Q20) and sorted block
  fencing + late wide-row payload decoding (ClickBench Q24): (1) for any non-empty
  block of `UInt64` keys, if `(target &&& ~~~orMask) ≠ 0` or `(~~~target &&& andMask)
  ≠ 0`, `target` cannot appear in the block; (2) under `ValidSortBlock`, skipping
  fenced blocks under `NULLS LAST` and decoding wide payloads only after final Top-N
  yields exact outputs with `payloadDecodes ≤ k`.
-/

namespace TinylambOpt.ResearchGoals.LateWideRowTopNBlockFence

def mayMatchMask (andMask orMask target : UInt64) : Bool :=
  (target &&& ~~~orMask) == 0 && (~~~target &&& andMask) == 0

def foldAndMask : List UInt64 → UInt64
  | [] => 0
  | x :: xs => xs.foldl (· &&& ·) x

def foldOrMask : List UInt64 → UInt64
  | [] => 0
  | x :: xs => xs.foldl (· ||| ·) x

structure WideRow where
  sortKey : Option Int
  filterPass : Bool
  payload : Nat
  deriving DecidableEq

structure Block where
  minNonNullSort : Int
  allNullSort : Bool
  rows : List WideRow
  deriving DecidableEq

def ValidSortBlock (b : Block) : Prop :=
  ∀ r ∈ b.rows,
    match r.sortKey with
    | none => True
    | some k => b.allNullSort = false ∧ b.minNonNullSort ≤ k

def leNullsLast (a b : WideRow × Nat) : Bool :=
  match a.1.sortKey, b.1.sortKey with
  | some ka, some kb =>
      if ka != kb then decide (ka ≤ kb) else decide (a.2 ≤ b.2)
  | some _, none => true
  | none, some _ => false
  | none, none => decide (a.2 ≤ b.2)

def insertSortedWR (x : WideRow × Nat) : List (WideRow × Nat) → List (WideRow × Nat)
  | [] => [x]
  | y :: ys => if leNullsLast x y then x :: y :: ys else y :: insertSortedWR x ys

def sortWR : List (WideRow × Nat) → List (WideRow × Nat)
  | [] => []
  | x :: xs => insertSortedWR x (sortWR xs)

def refWideTopN (k : Nat) (bs : List Block) : List Nat :=
  let all := (bs.flatMap Block.rows).zipIdx
  let filtered := all.filter (fun p => p.1.filterPass)
  ((sortWR filtered).take k).map (fun p => p.1.payload)

structure FencedResult where
  payloads : List Nat
  predEvals : Nat
  payloadDecodes : Nat
  deriving DecidableEq

/-- Can block `b` be skipped because the heap already holds `k > 0` non-null
    entries and `b.allNullSort || heapMax < b.minNonNullSort`? -/
def blockPrunedByHeap (k : Nat) (heap : List (WideRow × Nat)) (b : Block) : Bool :=
  if k == 0 || heap.length < k then false
  else
    match heap.getLast?.bind (fun p => p.1.sortKey) with
    | none => false
    | some tau => b.allNullSort || decide (tau < b.minNonNullSort)

/-- Optimized two-stage Top-N: skip entire blocks fenced below `minNonNullSort`
    when `k` non-null winners already exist, evaluate `filterPass` only on
    surviving blocks, and decode wide `payload` only for final `≤ k` winners. -/
def optLateWideTopN (k : Nat) (bs : List Block) : FencedResult :=
  let (heap, evals, _) :=
    bs.foldl
      (fun (accHeap, ev, globalIdx) b =>
        if k == 0 || blockPrunedByHeap k accHeap b then
          (accHeap, ev, globalIdx + b.rows.length)
        else
          let candidates :=
            b.rows.zipIdx.filterMap fun (r, localIdx) =>
              if r.filterPass then some (r, globalIdx + localIdx) else none
          let nextHeap := (sortWR (accHeap ++ candidates)).take k
          (nextHeap, ev + b.rows.length, globalIdx + b.rows.length))
      ([], 0, 0)
  { payloads := heap.map (fun p => p.1.payload)
    predEvals := evals
    payloadDecodes := heap.length }

/-- Block bitwise AND/OR point-lookup pruning (ClickBench Q20) and sorted block
    fencing + late wide-row payload decoding (ClickBench Q24):
    (1) for any non-empty block of `UInt64` keys, if `(target &&& ~~~orMask) ≠ 0`
    or `(~~~target &&& andMask) ≠ 0`, `target` cannot appear in the block;
    (2) under `ValidSortBlock`, skipping fenced blocks under `NULLS LAST` and
    decoding wide payloads only after final Top-N yields exact outputs with
    `payloadDecodes ≤ k`. -/
def Goal : Prop :=
  (∀ (xs : List UInt64) (t : UInt64),
    xs ≠ [] → t ∈ xs → mayMatchMask (foldAndMask xs) (foldOrMask xs) t = true) ∧
  (∀ (k : Nat) (bs : List Block),
    (∀ b ∈ bs, ValidSortBlock b) →
      let res := optLateWideTopN k bs
      res.payloads = refWideTopN k bs ∧ res.payloadDecodes ≤ k)

private def bFast0 : Block :=
  { minNonNullSort := 10, allNullSort := false
    rows := [⟨some 10, true, 101⟩, ⟨some 20, true, 102⟩, ⟨none, true, 103⟩] }
private def bFast1 : Block :=
  { minNonNullSort := 30, allNullSort := false
    rows := [⟨some 30, true, 201⟩, ⟨none, true, 202⟩] }
private def bAllNull : Block :=
  { minNonNullSort := 0, allNullSort := true
    rows := [⟨none, true, 301⟩] }

example :
    let res := optLateWideTopN 2 [bFast0, bFast1, bAllNull]
    res.payloads = refWideTopN 2 [bFast0, bFast1, bAllNull] ∧
    res.payloads = [101, 102] ∧
    res.predEvals = 3 ∧
    res.payloadDecodes = 2 := by
  decide

example :
    let xs : List UInt64 := [0x0103, 0x0107]
    mayMatchMask (foldAndMask xs) (foldOrMask xs) 0x0103 = true ∧
    mayMatchMask (foldAndMask xs) (foldOrMask xs) 0x0203 = false ∧
    mayMatchMask (foldAndMask xs) (foldOrMask xs) 0x0007 = false := by
  decide

end TinylambOpt.ResearchGoals.LateWideRowTopNBlockFence
