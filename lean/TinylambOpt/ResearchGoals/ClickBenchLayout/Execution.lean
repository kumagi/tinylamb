import Std

namespace TinylambOpt.ResearchGoals.ClickBenchLayout.Execution

namespace ClockRange
/-! A dependent timestamp uses the same row's event clock and a residual.
A sparse escape stores an absolute timestamp; its base may be far outside
the selection range. NULL dependent timestamps never pass WHERE. -/
inductive Slot where
  | missing | delta (d : Int) | absolute (v : Int)
def inside (l h v : Int) : Bool := l ≤ v && v < h
def plain (base : Int) : Slot → Option Int
  | .missing => none
  | .delta d => some (base+d)
  | .absolute v => some v

-- The second result counts dependent value decodes (escape or inline).
def probe (l h dmin dmax base : Int) : Slot → Bool × Nat
  | .missing => (false,0)
  | .absolute v => (inside l h v,1)
  | .delta d =>
    if base+dmax < l || h ≤ base+dmin then (false,0)
    else if l ≤ base+dmin && base+dmax < h then (true,0)
    else (inside l h (base+d),1)

/-- For a nonempty selection interval, deciding from the base alone is
possible exactly when every admissible residual gives the same answer.
Escapes still get the same WHERE result as a full timestamp decode. -/
def Goal : Prop := ∀ l h a b base, l < h → a ≤ b →
  (∀ s, (∀ d, s = .delta d → a ≤ d ∧ d ≤ b) →
    (probe l h a b base s).1 = (plain base s).any (inside l h)) ∧
  ((probe l h a b base (.delta a)).2 = 0 ↔
    ∀ d, a ≤ d → d ≤ b → inside l h (base+d) = inside l h (base+a))

#guard probe 10 20 0 2 1 (.absolute 11) == (true,1)
#guard probe 10 20 (-1) 1 15 (.delta 0) == (true,0)
end ClockRange

namespace ConcatSearch
/-! A length stream and a concatenated byte stream. Empty payloads may
belong to empty strings or NULLs. Use cumulative ends to attribute arbitrary
overlapping byte matches to one row. Each matching row is emitted once. -/
abbrev Word := List UInt8
def starts (needle haystack : Word) := (List.range (haystack.length+1)).filter
  (fun i => needle.isPrefixOf (haystack.drop i))
def rowIDs (xs : List (Option Word)) (needle : Word) :=
  let words := xs.map (·.getD [])
  let ends := ((words.map List.length).scanl (·+·) 0).drop 1
  if needle.isEmpty then xs.zipIdx.filterMap (fun (x,i) => x.map (fun _ => i))
  else ((starts needle words.flatten).filterMap (fun pos => do
    let i := ends.findIdx (pos < ·)
    let e ← ends[i]?
    if pos+needle.length ≤ e then some i else none)).eraseDups

/-- May a scanner search one stream and then consult the ends only for hits?
Reject cross-row byte matches, handle empty patterns and preserve row order. -/
def Goal : Prop := ∀ xs needle, rowIDs xs needle =
  xs.zipIdx.filterMap (fun (x,i) =>
    if x.any (fun w => !(starts needle w).isEmpty) then some i else none)

#guard rowIDs [some [1],none,some [],some [2],some [1,2,1,2]] [1,2] == [4]
#guard rowIDs [none,some [],some [1]] [] == [1,2]
end ConcatSearch

namespace AnchorDemand
/-! One sibling reference tag per physical row. A base read observes a whole
compressed frame. A fixed width slice may request IDs repeatedly or in any
order. Distinct frames are independently accessible. -/
abbrev Word := List UInt8
inductive Tag where
  | literal (v : Option Word) | copyBase
  deriving BEq, DecidableEq
def atRow (tags : List Tag) (base : Nat → Option Word) (i : Nat) :=
  match tags[i]? with
  | some (.literal v) => v
  | some .copyBase => base i
  | none => none

def needed (tags : List Tag) (frameOf : Nat → Nat) (ids : List Nat) :=
  ((ids.filter (fun i => tags[i]? == some .copyBase)).map frameOf).eraseDups

/-- The computed anchor-frame set is the *least* sufficient set for every
possible anchor content. Includes NULL copies, literal empties, duplicate and
out-of-range gathers. A resident frame can be subtracted from I/O afterwards. -/
def Goal : Prop := ∀ tags frameOf (ids available : List Nat),
  (∀ f ∈ needed tags frameOf ids, f ∈ available) ↔
    ∀ (x y : Nat → Option Word), (∀ i, frameOf i ∈ available → x i = y i) →
      ids.map (atRow tags x) = ids.map (atRow tags y)

#guard needed [.literal (some []), .copyBase, .copyBase] (· / 2) [2,0,2] == [1]
end AnchorDemand

namespace SparseMark
/-! Lower mark and next mark are inclusive lexicographic bounds. Equal keys
can span granules. Only the two marks are known to the decision procedure. -/
abbrev Key := Nat × Nat
def le (a b : Key) := a.1 < b.1 ∨ (a.1 = b.1 ∧ a.2 ≤ b.2)
def least (fence : Key) (xl yl yh : Nat) : Key :=
  if fence.1 < xl then (xl,yl) else
  if fence.2 ≤ yh then (fence.1, max yl fence.2) else (fence.1+1,yl)

/-- A two-column rectangular predicate over a lex-clustered part may skip a
granule exactly when no possible key fits its marks and both SQL ranges. -/
def Goal : Prop := ∀ first last xl xh yl yh, xl ≤ xh → yl ≤ yh →
  let candidate := least first xl yl yh
  ((candidate.1 ≤ xh ∧ le candidate last) ↔
    ∃ p : Key, le first p ∧ le p last ∧ xl ≤ p.1 ∧ p.1 ≤ xh ∧ yl ≤ p.2 ∧ p.2 ≤ yh)

#guard least (3,99) 2 7 9 == (4,7)
#guard least (3,9) 2 7 9 == (3,9)
end SparseMark

namespace DefaultGroups
/-! A wide tuple has a known, possibly nullable default in each component.
Sparse lane streams record every deviation at most once for each row.
Untouched rows contribute to one implicit group, including for zero lanes. -/
abbrev Val := Option Int
abbrev Lane := Val × List (Nat × Val)
def tupleAt (ls : List Lane) (i : Nat) := ls.map (fun (d,es) => (es.lookup i).getD d)
def touched (ls : List Lane) := (ls.flatMap (fun l => l.2.map Prod.fst)).eraseDups

/-- Compute the exact GROUP BY multiplicity of any projected tuple from
exception row IDs, including the multiplicity of the all-default group. -/
def Goal : Prop := ∀ (lanes : List Lane) n (key : List Val),
  (∀ l ∈ lanes, ∀ e ∈ l.2, e.1 < n) →
  let ids := touched lanes
  ((List.range n).map (tupleAt lanes)).count key =
    (ids.map (tupleAt lanes)).count key + (if key == lanes.map Prod.fst then n-ids.length else 0)

#guard touched [(none,[(2,some 0)]),(some 0,[(2,none),(4,some 1)])] == [2,4]
end DefaultGroups

namespace LengthRollup
/-! ClickBench Q28 filters out empty URLs, groups by CounterID and needs
COUNT and the numerator of AVG(BYTE_LENGTH). A cluster run can intersect
several independently compressed text frames or start in the middle of one. -/
abbrev Word := List UInt8
abbrev Slice := Nat × Nat
def lengths (xs : List (Option Word)) := xs.map (fun s => (s.getD []).length)
def cut (prefixSums : List Nat) (n : Nat) (s : Slice) :=
  (prefixSums[min n (s.1+s.2)]?.getD 0) - (prefixSums[min n s.1]?.getD 0)

def summarized (xs : List (Option Word)) (slices : List Slice) :=
  let lens := lengths xs
  let ends := lens.scanl (·+·) 0
  let present := (lens.map (fun n => (n != 0).toNat)).scanl (·+·) 0
  ((slices.map (cut ends xs.length)).sum, (slices.map (cut present xs.length)).sum)

/-- For one group, start/end offsets plus nonempty ranks suffice to merge
byte sum and qualifying count without materializing URL strings. Clipped and
empty slices are legal. Repeated slices have bag multiplicity. -/
def Goal : Prop := ∀ xs slices,
  let rows := slices.flatMap (fun (s : Slice) => (xs.drop s.1).take s.2)
  let kept := rows.filterMap (fun v => v.bind (fun w => if w.isEmpty then none else some w))
  summarized xs slices = ((kept.map List.length).sum, kept.length)

#guard summarized [none,some [1,2],some [],some [3]] [(1,2),(3,40)] == (3,2)
end LengthRollup

end TinylambOpt.ResearchGoals.ClickBenchLayout.Execution
