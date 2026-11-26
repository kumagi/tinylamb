import TinylambOpt.ResearchGoals.Compression.Wire

/-! Compare row-group layout using exact min/max pruning and compressed bytes
actually touched by a range. Integer keys are non-null and pages are immutable.
A page may be eligible without containing any qualifying row. -/
namespace TinylambOpt.ResearchGoals.ResourcePolicies.ClusteredPages
open TinylambOpt.ResearchGoals.Compression.Wire

structure Page where
  keys : List Nat
  bytes : Nat
  deriving Repr
def candidate (lo hi : Nat) (p : Page) :=
  p.keys.min?.any (fun l => l ≤ hi) && p.keys.max?.any (fun h => lo ≤ h)
def readBytes (ps : List Page) (lo hi : Nat) :=
  ((ps.filter (candidate lo hi)).map Page.bytes).sum
def rangeRows (lo hi : Nat) (rows : List Nat) := rows.filter (fun k => lo ≤ k && k ≤ hi)

/-- Empty pages and inclusive bounds allow a metadata-only prefilter in front of the row predicate. -/
def ZonePrunedRangeScan.Goal : Prop := ∀ (ps : List Page) lo hi,
  rangeRows lo hi ((ps.filter (candidate lo hi)).flatMap Page.keys) = rangeRows lo hi (ps.flatMap Page.keys)

/-- Splitting a page without increasing payload bytes cannot increase the bytes charged by min/max pruning. -/
def UnpaddedSplitScanCost.Goal : Prop := ∀ a b lo hi,
  readBytes [a,b] lo hi ≤ readBytes [⟨a.keys++b.keys,a.bytes+b.bytes⟩] lo hi

/-- The incremental cost of coalescing two adjacent pages is bounded by the larger of their byte lengths. -/
def CoalescingPhantomRangeCost.Goal : Prop := ∀ a b lo hi,
  readBytes [⟨a.keys++b.keys,a.bytes+b.bytes⟩] lo hi ≤ readBytes [a,b] lo hi + max a.bytes b.bytes

/-- For gap-free integer pages, a nonempty interval overlapping a page must find a real value. -/
def DensePageNoFalseRead.Goal : Prop := ∀ p lo hi, lo ≤ hi →
  (∀ l ∈ p.keys, ∀ h ∈ p.keys, ∀ x, l ≤ x → x ≤ h → x ∈ p.keys) →
  (candidate lo hi p = true ↔ rangeRows lo hi p.keys ≠ [])

/-- Predicates on two independently zoned columns can reject an entire page, but correlation may leave no matching row. -/
def IndependentZonesCertifyMatchingPair.Goal : Prop := ∀ (rows : List (Nat × Nat)) x y,
  candidate x x ⟨rows.map Prod.fst,0⟩ = true → candidate y y ⟨rows.map Prod.snd,0⟩ = true →
  (x,y) ∈ rows

def physical (header alignTo : Nat) (p : Page) := align alignTo (header+p.bytes)
def physicalCost (ps : List Page) (header unit lo hi : Nat) :=
  ((ps.filter (candidate lo hi)).map (physical header unit)).sum

/-- Charge both headers and alignment before admitting a page split for the worst range workload. -/
def SplitPhysicalReadEnvelope.Goal : Prop := ∀ a b h u lo hi, 0 < u →
  physicalCost [a,b] h u lo hi ≤ physicalCost [⟨a.keys++b.keys,a.bytes+b.bytes⟩] h u lo hi + align u h + u

/-- Fewer surviving pages imply fewer physical read bytes even when compression ratios differ between groups. -/
def PageSelectivityAsByteProxy.Goal : Prop := ∀ (p q : List Page) lo hi,
  (p.filter (candidate lo hi)).length < (q.filter (candidate lo hi)).length →
  readBytes p lo hi ≤ readBytes q lo hi

def score (ps : List Page) (queries : List (Nat × Nat)) :=
  (queries.map (fun (lo,hi) => readBytes ps lo hi)).sum

/-- Repricing one compacted span can update a trace-weighted scan estimate without rescoring unaffected pages. -/
def LocalReclusterWorkDelta.Goal : Prop := ∀ a b kept workload,
  score (a++kept) workload + score b workload = score (b++kept) workload + score a workload

/-- The workload benefit amortizes rewrite traffic over repeated scans, including differences in compression. -/
def ReclusterPaybackEpoch.Goal : Prop := ∀ a b qs rewrite n, score b qs ≤ score a qs →
  (rewrite+n*score b qs ≤ n*score a qs ↔ rewrite ≤ n*(score a qs-score b qs))

#guard readBytes [⟨[0,8],16⟩,⟨[10],4⟩] 5 5 == 16

#guard readBytes [⟨[0],8⟩,⟨[10],8⟩] 5 5 == 0 && readBytes [⟨[0,10],16⟩] 5 5 == 16

end TinylambOpt.ResearchGoals.ResourcePolicies.ClusteredPages
