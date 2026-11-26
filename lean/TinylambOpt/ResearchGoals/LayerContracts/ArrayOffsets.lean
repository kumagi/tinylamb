import Std

/-!
SQL UNNEST and array operators must preserve the modeled element offsets, NULL padding,
and row multiplicities.
-/

namespace TinylambOpt.ResearchGoals.LayerContracts.ArrayOffsets

/-! Correlated array expansion. Each result identifies input offsets; no offset
is distinct from a present element whose SQL value is NULL. Logical outer
expansion emits a single pair of missing offsets for an empty result. -/
abbrev Arr := List (Option Int)
structure Input where
  left : Arr
  right : Arr
abbrev Pos := Option Nat × Option Nat
def emptyPos : Pos := (none,none)
def ids (xs : Arr) := List.range xs.length
def indexIf (n i : Nat) : Option Nat := if i < n then some i else none
def live (xs : Arr) := (ids xs).filter (fun i => (xs[i]?.join).isSome)
def lpos (i : Nat) : Pos := (some i,none)
def both (i j : Nat) : Pos := (some i,some j)
def padded (s : Input) (i : Nat) : Pos := (indexIf s.left.length i,indexIf s.right.length i)
structure Model where
  rows : Input → List Pos
  count : Input → Nat
  address : Input → Nat → Pos
def selectPos (m : Model) (s : Input) (i : Nat) := if i < m.count s then some (m.address s i) else none
def outer (xs : List Pos) := if xs.isEmpty then [emptyPos] else xs
def skipParents (m : Model) : List Input → Nat → Option Pos
  | [],_ => none
  | s::ss,i => if i < m.count s then selectPos m s i else skipParents m ss (i-m.count s)
def payload (s : Input) (p : Pos) := (p.1.bind (fun i => s.left[i]?.join),p.2.bind (fun j => s.right[j]?.join))

/-- Avoid materializing UNNEST when only its row count is consumed. -/
def cardinality (m : Model) : Prop := ∀ s, m.count s = (m.rows s).length

/-- Random access with a zero-based offset reproduces expansion order and exhaustion. -/
def ordinal (m : Model) : Prop := ∀ s i, selectPos m s i = (m.rows s)[i]?

/-- Skip fully expanded parents using their sizes before applying global OFFSET. -/
def lateralOffset (m : Model) : Prop := ∀ ss i, skipParents m ss i = (ss.flatMap m.rows)[i]?

/-- Proposed row-count rule for a preserved parent under correlated OUTER expansion. -/
def leftOuterCount (m : Model) : Prop := ∀ s, (outer (m.rows s)).length = max 1 (m.count s)

/-- All present coordinates refer to an actual array cell, possibly a NULL one. -/
def addressValidity (m : Model) : Prop := ∀ s p, p ∈ m.rows s → (∀ i ∈ p.1, i < s.left.length) ∧ (∀ j ∈ p.2, j < s.right.length)

/-- Can DISTINCT on output payloads be dropped if offsets are distinct? -/
def payloadDedup (m : Model) : Prop := ∀ s, (m.rows s).Nodup → ((m.rows s).map (payload s)).Nodup

/-- Generate only the requested offset interval for a correlated LIMIT. -/
def lateSlice (m : Model) : Prop := ∀ s off n, ((List.range (min n (m.count s-off))).map (fun j => m.address s (off+j))) =
  ((m.rows s).drop off).take n

/-- Cache both group sizes and the offset map across inputs of the same shape. -/
def valueBlind (m : Model) : Prop := ∀ s t,
  s.left.length = t.left.length → s.right.length = t.right.length →
  m.count s = m.count t ∧ ∀ i, selectPos m s i = selectPos m t i

namespace LeftElements

/-- UNNEST the first array, retaining NULL cells. -/
def model : Model :=
  ⟨fun s => (ids s.left).map lpos,
   fun s => s.left.length,
   fun _ i => lpos i⟩

def Cardinality.Goal : Prop := cardinality model
def Ordinal.Goal : Prop := ordinal model
def LateralOffset.Goal : Prop := lateralOffset model
def LeftOuterCount.Goal : Prop := leftOuterCount model
def AddressValidity.Goal : Prop := addressValidity model
def PayloadDedup.Goal : Prop := payloadDedup model
def LateSlice.Goal : Prop := lateSlice model
def ValueBlind.Goal : Prop := valueBlind model

def KeepNull.Goal : Prop := ∀ s i, s.left[i]? = some none → lpos i ∈ model.rows s

def NoRight.Goal : Prop := ∀ s p, p ∈ model.rows s → p.2=none

end LeftElements

namespace ReverseOffsets

/-- Read a repeated field from the last offset down. -/
def model : Model :=
  ⟨fun s => (ids s.left).reverse.map lpos,
   fun s => s.left.length,
   fun s i => lpos (s.left.length-1-i)⟩

def Cardinality.Goal : Prop := cardinality model
def Ordinal.Goal : Prop := ordinal model
def LateralOffset.Goal : Prop := lateralOffset model
def LeftOuterCount.Goal : Prop := leftOuterCount model
def AddressValidity.Goal : Prop := addressValidity model
def PayloadDedup.Goal : Prop := payloadDedup model
def LateSlice.Goal : Prop := lateSlice model
def ValueBlind.Goal : Prop := valueBlind model

def ReverseFirst.Goal : Prop := ∀ s, 0 < s.left.length → (model.rows s).head? = some (lpos (s.left.length-1))

def LastZero.Goal : Prop := ∀ s, 0 < s.left.length → (model.rows s).getLast? = some (lpos 0)

end ReverseOffsets

namespace ZipShortest

/-- Inner zip truncated to the shorter field. -/
def model : Model :=
  ⟨fun s => ((ids s.left).zip (ids s.right)).map (fun (i,j) => both i j),
   fun s => min s.left.length s.right.length,
   fun _ i => both i i⟩

def Cardinality.Goal : Prop := cardinality model
def Ordinal.Goal : Prop := ordinal model
def LateralOffset.Goal : Prop := lateralOffset model
def LeftOuterCount.Goal : Prop := leftOuterCount model
def AddressValidity.Goal : Prop := addressValidity model
def PayloadDedup.Goal : Prop := payloadDedup model
def LateSlice.Goal : Prop := lateSlice model
def ValueBlind.Goal : Prop := valueBlind model

def Diagonal.Goal : Prop := ∀ s i j, both i j ∈ model.rows s → i=j

def EmptyRight.Goal : Prop := ∀ (a : Arr), model.rows ⟨a,[]⟩ = []

end ZipShortest

namespace ZipPad

/-- Full aligned zip with distinct padding and SQL NULL. -/
def model : Model :=
  ⟨fun s => (List.range (max s.left.length s.right.length)).map (padded s),
   fun s => max s.left.length s.right.length,
   padded⟩

def Cardinality.Goal : Prop := cardinality model
def Ordinal.Goal : Prop := ordinal model
def LateralOffset.Goal : Prop := lateralOffset model
def LeftOuterCount.Goal : Prop := leftOuterCount model
def AddressValidity.Goal : Prop := addressValidity model
def PayloadDedup.Goal : Prop := payloadDedup model
def LateSlice.Goal : Prop := lateSlice model
def ValueBlind.Goal : Prop := valueBlind model

def RightTail.Goal : Prop := ∀ s i, s.left.length ≤ i → i < s.right.length → (none,some i) ∈ model.rows s

def NoDoublePad.Goal : Prop := ∀ s, emptyPos ∉ model.rows s

end ZipPad

namespace Cartesian

/-- Two independent UNNEST sources in left-major order. -/
def model : Model :=
  ⟨fun s => (ids s.left).flatMap (fun i => (ids s.right).map (both i)),
   fun s => s.left.length*s.right.length,
   fun s i => both (i/s.right.length) (i%s.right.length)⟩

def Cardinality.Goal : Prop := cardinality model
def Ordinal.Goal : Prop := ordinal model
def LateralOffset.Goal : Prop := lateralOffset model
def LeftOuterCount.Goal : Prop := leftOuterCount model
def AddressValidity.Goal : Prop := addressValidity model
def PayloadDedup.Goal : Prop := payloadDedup model
def LateSlice.Goal : Prop := lateSlice model
def ValueBlind.Goal : Prop := valueBlind model

def Factor.Goal : Prop := ∀ s i, i < s.left.length → ((model.rows s).filter (fun p => p.1 == some i)).length = s.right.length

def InverseOffset.Goal : Prop := ∀ s i j, i < s.left.length → j < s.right.length → selectPos model s (i*s.right.length+j) = some (both i j)

end Cartesian

namespace NonNullOnly

/-- Push IS NOT NULL below expansion, preserving original offsets. -/
def model : Model :=
  ⟨fun s => (live s.left).map lpos,
   fun s => s.left.countP Option.isSome,
   fun s i => lpos ((live s.left)[i]?.getD 0)⟩

def Cardinality.Goal : Prop := cardinality model
def Ordinal.Goal : Prop := ordinal model
def LateralOffset.Goal : Prop := lateralOffset model
def LeftOuterCount.Goal : Prop := leftOuterCount model
def AddressValidity.Goal : Prop := addressValidity model
def PayloadDedup.Goal : Prop := payloadDedup model
def LateSlice.Goal : Prop := lateSlice model
def ValueBlind.Goal : Prop := valueBlind model

def PhysicalIndex.Goal : Prop := ∀ s i, lpos i ∈ model.rows s ↔ (s.left[i]?.join).isSome

def DropNulls.Goal : Prop := ∀ s, model.count s + s.left.count none = s.left.length

end NonNullOnly

namespace EvenOffsets

/-- A modulo filter on WITH OFFSET skips odd cells. -/
def model : Model :=
  ⟨fun s => ((ids s.left).filter (fun i => i%2 == 0)).map lpos,
   fun s => (s.left.length+1)/2,
   fun _ i => lpos (2*i)⟩

def Cardinality.Goal : Prop := cardinality model
def Ordinal.Goal : Prop := ordinal model
def LateralOffset.Goal : Prop := lateralOffset model
def LeftOuterCount.Goal : Prop := leftOuterCount model
def AddressValidity.Goal : Prop := addressValidity model
def PayloadDedup.Goal : Prop := payloadDedup model
def LateSlice.Goal : Prop := lateSlice model
def ValueBlind.Goal : Prop := valueBlind model

def NoOdd.Goal : Prop := ∀ s i, lpos (2*i+1) ∉ model.rows s

def LastEven.Goal : Prop := ∀ s, 0 < s.left.length → (model.rows s).getLast? = some (lpos (2*((s.left.length-1)/2)))

end EvenOffsets

namespace AppendFields

/-- Concatenate two repeated fields without losing their source coordinates. -/
def model : Model :=
  ⟨fun s => (ids s.left).map lpos ++ (ids s.right).map (fun j => (none,some j)),
   fun s => s.left.length+s.right.length,
   fun s i => if i < s.left.length then lpos i else (none,some (i-s.left.length))⟩

def Cardinality.Goal : Prop := cardinality model
def Ordinal.Goal : Prop := ordinal model
def LateralOffset.Goal : Prop := lateralOffset model
def LeftOuterCount.Goal : Prop := leftOuterCount model
def AddressValidity.Goal : Prop := addressValidity model
def PayloadDedup.Goal : Prop := payloadDedup model
def LateSlice.Goal : Prop := lateSlice model
def ValueBlind.Goal : Prop := valueBlind model

def Boundary.Goal : Prop := ∀ s, 0 < s.right.length → selectPos model s s.left.length = some (none,some 0)

def LeftPrefix.Goal : Prop := ∀ s, (model.rows s).take s.left.length = (ids s.left).map lpos

end AppendFields

namespace LeadPair

/-- Pair a left offset with the next right offset. -/
def model : Model :=
  ⟨fun s => ((ids s.left).zip (ids s.right).tail).map (fun (i,j) => both i j),
   fun s => min s.left.length (s.right.length-1),
   fun _ i => both i (i+1)⟩

def Cardinality.Goal : Prop := cardinality model
def Ordinal.Goal : Prop := ordinal model
def LateralOffset.Goal : Prop := lateralOffset model
def LeftOuterCount.Goal : Prop := leftOuterCount model
def AddressValidity.Goal : Prop := addressValidity model
def PayloadDedup.Goal : Prop := payloadDedup model
def LateSlice.Goal : Prop := lateSlice model
def ValueBlind.Goal : Prop := valueBlind model

def NoFirstRight.Goal : Prop := ∀ s i, both i 0 ∉ model.rows s

def UnitRight.Goal : Prop := ∀ s, s.right.length = 1 → model.rows s = []

end LeadPair

namespace MatchValue

/-- Skip the matches of entire left cells. Within the selected group, enumerate
only the matching right offsets; the Cartesian expansion is never constructed. -/
private def findMatch (right : Arr) : List (Option Int × Nat) → Nat → Pos
  | [], _ => emptyPos
  | (none,_)::xs, n => findMatch right xs n
  | (some v,i)::xs, n =>
    let size := right.count (some v)
    if n < size then
      let rightOffsets := (ids right).filter (fun j => right[j]? == some (some v))
      (some i, rightOffsets[n]?)
    else findMatch right xs (n-size)

/-- Equijoin array elements, retaining multiplicity and suppressing NULL matches. -/
def model : Model :=
  ⟨fun s => (ids s.left).flatMap (fun i => ((live s.right).filter (fun j => s.left[i]? == s.right[j]?)).map (both i)),
   fun s => ((s.left.filterMap id).map (fun v => s.right.count (some v))).sum,
   fun s n => findMatch s.right s.left.zipIdx n⟩

def Cardinality.Goal : Prop := cardinality model
def Ordinal.Goal : Prop := ordinal model
def LateralOffset.Goal : Prop := lateralOffset model
def LeftOuterCount.Goal : Prop := leftOuterCount model
def AddressValidity.Goal : Prop := addressValidity model
def PayloadDedup.Goal : Prop := payloadDedup model
def LateSlice.Goal : Prop := lateSlice model
def ValueBlind.Goal : Prop := valueBlind model

def NoNullMatch.Goal : Prop := ∀ s i j, both i j ∈ model.rows s → (s.left[i]?.join).isSome

def AllMatches.Goal : Prop := ∀ n k (v : Int), model.count ⟨List.replicate n (some v),List.replicate k (some v)⟩ = n*k

end MatchValue

#guard selectPos MatchValue.model ⟨[none,some 7,some 7],[some 7,none,some 7]⟩ 2 == some (both 2 0)
#guard ZipPad.model.rows ⟨[none],[none,some 5]⟩ == [both 0 0,(none,some 1)]
#guard (outer (LeftElements.model.rows ⟨[],[]⟩)) != LeftElements.model.rows ⟨[none],[]⟩

end TinylambOpt.ResearchGoals.LayerContracts.ArrayOffsets
