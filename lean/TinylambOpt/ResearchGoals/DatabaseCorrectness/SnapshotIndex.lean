import TinylambOpt.ResearchGoals.DatabaseCorrectness.VarcharKeys

/-! Compound binary VARCHAR keys use the actual field encoding. The upper
bound on a nonempty proper prefix gets a trailing FF, as in EncodeEndParts.
The second model compares a physical index plus a changed-position journal
against an independent snapshot heap scan. -/
namespace TinylambOpt.ResearchGoals.DatabaseCorrectness.SnapshotIndex
open TinylambOpt.ResearchGoals.DatabaseCorrectness.VarcharKeys (Bytes encode)

abbrev Key := List Bytes
def image (k : Key) := k.flatMap encode
def upper (arity : Nat) (k : Key) : Bytes :=
  image k ++ (if !k.isEmpty && k.length < arity then [255] else [])
def mayRead (arity : Nat) (lo hi k : Key) := decide
  ((lo = [] ∨ image lo ≤ image k) ∧ (hi = [] ∨ image k ≤ upper arity hi))
def logical (lo hi k : Key) := (lo = [] ∨ lo ≤ k.take lo.length) ∧ (hi = [] ∨ k.take hi.length ≤ hi)

/-- The encoded range test agrees with decoded inclusive tuple-prefix bounds for full-arity binary keys. -/
def InclusiveCompoundPrefixScan.Goal : Prop := ∀ arity lo hi k, k.length = arity →
  lo.length ≤ arity → hi.length ≤ arity → (mayRead arity lo hi k = true ↔ logical lo hi k)

/-- Appending binary fields to a matched proper prefix cannot push the full key past the FF sentinel. -/
def AllDescendantsBelowPrefixFence.Goal : Prop := ∀ pre rest, pre ≠ [] → rest ≠ [] →
  image pre < image (pre++rest) ∧ image (pre++rest) < upper (pre.length+rest.length) pre

/-- A column containing the empty string is a bounded point, distinct from an absent bound. -/
def EmptyVarcharIsBoundedPoint.Goal : Prop := ∀ (v : Bytes), mayRead 1 [[]] [[]] [v] = decide (v = [])

abbrev Heap := Nat → Option Key
def matchesRange (p : Key → Bool) (h : Heap) (i : Nat) := (h i).any p
def scan (ids : List Nat) (h : Heap) (p : Key → Bool) := ids.filter (matchesRange p h)
def checkedIndex (physical changed : List Nat) (h : Heap) (p : Key → Bool) :=
  scan (physical++changed).eraseDups h p

/-- If every changed position was journaled and the latest index covers current rows, a recheck recovers
the exact snapshot bag of positions, even across key moves, deletes and stale physical entries. -/
def IndexedCandidateHeapScanAgreement.Goal : Prop := ∀ ids old now p physical changed, ids.Nodup →
  (∀ i ∈ physical++changed, i ∈ ids) →
  (∀ i ∈ ids, matchesRange p now i → i ∈ physical) →
  (∀ i ∈ ids, old i ≠ now i → i ∈ changed) →
  (checkedIndex physical changed old p).Perm (scan ids old p)

/-- A changed key can enter or leave the range. Excluding both old and new keys licenses skipping the changed-position lookup. -/
def DisjointOldAndNewKeyFence.Goal : Prop := ∀ ids old now p,
  (∀ i ∈ ids, old i ≠ now i → matchesRange p old i = false ∧ matchesRange p now i = false) →
  scan ids old p = scan ids now p

/-- Compare multiplicity by row position: two different matching rows with identical keys must both survive rechecking. -/
def DuplicateKeyDistinctPositions.Goal : Prop := ∀ a b k p old physical changed,
  a ≠ b → a ∈ physical++changed → b ∈ physical++changed → old a = some k → old b = some k → p k →
  (checkedIndex physical changed old p).count a + (checkedIndex physical changed old p).count b = 2

#guard mayRead 2 [[97]] [[97]] [[97],[255,0]]
#guard !mayRead 2 [[97]] [[97]] [[97,0],[]]

end TinylambOpt.ResearchGoals.DatabaseCorrectness.SnapshotIndex
