import TinylambOpt.ResearchGoals.Compression.Wire

/-! Seekable compressed frames. Model the length stream, sparse absolute restart
directory, and a patched fixed-width representation with rank-indexed exceptions. -/
namespace TinylambOpt.ResearchGoals.ResourcePolicies.RestartDirectory
open TinylambOpt.ResearchGoals.Compression.Wire

def offset (lengths : List Nat) (row : Nat) := (lengths.take row).sum
def anchor (stride row : Nat) := row / stride * stride
def seek (lengths : List Nat) (stride row : Nat) :=
  let a := anchor stride row
  offset lengths a + ((lengths.drop a).take (row-a)).sum

/-- A seek can restart at its recorded boundary even for empty values and a short last frame. -/
def RestartSeekMatchesPrefix.Goal : Prop := ∀ ns s i, 0 < s → i ≤ ns.length → seek ns s i = offset ns i

/-- An aligned directory refinement never increases the number of lengths decoded at a point lookup. -/
def NestedStridePointWork.Goal : Prop := ∀ coarse fine i, 0 < fine → 0 < coarse → fine ∣ coarse →
  i-anchor fine i ≤ i-anchor coarse i

/-- Arbitrary smaller strides are a proposed substitute for nested directories for each point workload. -/
def SmallerStridePointDominance.Goal : Prop := ∀ a b i, 0 < a → a ≤ b → i-anchor a i ≤ i-anchor b i

def directory (ns : List Nat) (s : Nat) :=
  ((List.range ns.length).filter (fun i => i%s == 0)).map (offset ns)

/-- An appended frame preserves all old directory entries; sharing the prefix avoids repacking them. -/
def AppendOnlyRestartDirectory.Goal : Prop := ∀ a b s, 0 < s →
  (directory (a++b) s).take (directory a s).length = directory a s

/-- A one-byte change before a restart can widen multiple directory entries at the same varint threshold. -/
def SingleEditDirectoryGrowth.Goal : Prop := ∀ (a b : List Nat) n s, 0 < s →
  streamBytes (directory (a++[n+1]++b) s) ≤ streamBytes (directory (a++[n]++b) s) + 1

/-- Storing relative restarts after a page-local base never costs more offset bytes than absolute varints. -/
def RelativeRestartVarintBudget.Goal : Prop := ∀ ds base,
  streamBytes (ds.map (·-base)) ≤ streamBytes ds

def flagged (w x : Nat) := decide (2^w ≤ x)
def patchedRead (w : Nat) (xs : List Nat) (i : Nat) : Option Nat := do
  let flag ← (xs.map (flagged w))[i]?
  if flag then
    let rank := ((xs.take i).filter (flagged w)).length
    (xs.filter (flagged w))[rank]?
  else return takeAt (pack w xs) w i

/-- Read a single value through the exception bitmap and its exclusive rank without expanding the frame. -/
def RankedExceptionPointRead.Goal : Prop := ∀ w xs i, patchedRead w xs i = xs[i]?

def patchedBytes (w : Nat) (xs : List Nat) :=
  byteCount (w*xs.length) + byteCount xs.length + streamBytes (xs.filter (flagged w))

/-- Local optimum under one-bit width changes is sufficient to choose a patched frame width. -/
def PatchedWidthLocalMinimum.Goal : Prop := ∀ xs w, 0 < w →
  patchedBytes w xs < patchedBytes (w-1) xs → patchedBytes w xs < patchedBytes (w+1) xs →
  ∀ v, patchedBytes w xs ≤ patchedBytes v xs

/-- Splitting a patched frame at the same width increases rounding overhead by at most two bytes. -/
def PatchedFrameSplitOverhead.Goal : Prop := ∀ w a b,
  patchedBytes w a + patchedBytes w b ≤ patchedBytes w (a++b) + 2

/-- A threshold scan above the lane maximum can skip all inline lanes including packed exception residues. -/
def ExceptionsOnlyThresholdScan.Goal : Prop := ∀ w (xs : List Nat) threshold, 2^w ≤ threshold →
  (xs.filter (flagged w)).filter (fun x => threshold ≤ x) = xs.filter (fun x => threshold ≤ x)

/-- Counting exceptions alone should predict the best width if exception payloads use varints. -/
def ExceptionCountWidthChoice.Goal : Prop := ∀ w (a b : List Nat), a.length = b.length →
  (a.filter (flagged w)).length = (b.filter (flagged w)).length →
  (patchedBytes w a ≤ packedSize a ↔ patchedBytes w b ≤ packedSize b)

#guard patchedRead 2 [0,11,3,4] 3 == some 4
#guard seek [0,5,0,3] 2 3 == 5

#guard 12 - anchor 5 12 == 2 && 12 - anchor 6 12 == 0

-- Width eight beats both neighboring widths on this frame.
#guard [7,8,9,0].map (fun w => patchedBytes w [0,0,129]) == [6,4,5,3]

end TinylambOpt.ResearchGoals.ResourcePolicies.RestartDirectory
