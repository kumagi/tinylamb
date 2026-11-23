import Std

namespace TinylambOpt.ResearchGoals.BinaryText

/-! Binary collation over complete byte strings. NUL and 0xff are ordinary bytes.
Offsets and the one-edit filter below count bytes, not Unicode scalar values. -/
abbrev Bytes := List UInt8

/-- Exclusive upper bound on all extensions. Carry through trailing 0xff and truncate. -/
def upperRev : Bytes → Option Bytes
  | [] => none
  | b::bs => if b == 255 then upperRev bs else some ((b+1)::bs)

def upper (p : Bytes) := (upperRev p.reverse).map List.reverse

def PrefixFence.Goal : Prop := ∀ (p x : Bytes),
  p.isPrefixOf x = (decide (p ≤ x) && (upper p).all (fun hi => decide (x < hi)))

/-- Front-coded dictionaries can compare adjacent entries using the previous probe's LCP.
When the two LCPs differ, no suffix comparison is needed. -/
def lcp : Bytes → Bytes → Nat
  | x::xs, y::ys => if x == y then 1 + lcp xs ys else 0
  | _, _ => 0

def nextCmp (prev curr probe : Bytes) (shared matched : Nat) : Ordering :=
  if shared < matched then compare (curr[shared]?) (prev[shared]?)
  else if matched < shared then compare (prev[matched]?) (probe[matched]?)
  else compare (curr.drop shared) (probe.drop matched)

def SharedPrefixCompare.Goal : Prop := ∀ prev curr probe,
  nextCmp prev curr probe (lcp prev curr) (lcp prev probe) = compare curr probe

/-- Probe the rarest required byte's positional postings; backtrack by its offset.
The lower bound on pos prevents truncated Nat subtraction from inventing a hit. -/
def matchesAt (s p : Bytes) (i : Nat) := p.isPrefixOf (s.drop i)

def AnchorPositions.Goal : Prop := ∀ (s p : Bytes) j b, p[j]? = some b →
  (((s.zipIdx.filter fun (v,i) => v == b && j ≤ i).map fun e => e.2-j).filter (matchesAt s p)) =
    ((List.range (s.length+1)).filter (matchesAt s p))

/-- Deduplicating equality candidates for edit distance one using one-deletion signatures.
Substitution shares a deletion on both sides; insert/delete shares the shorter input. -/
def deletes (s : Bytes) := (List.range s.length).map fun i => s.take i ++ s.drop (i+1)

def near : Bytes → Bytes → Bool
  | [], ys => ys.length ≤ 1
  | xs, [] => xs.length ≤ 1
  | x::xs, y::ys => if x == y then near xs ys else
      xs == ys || xs == y::ys || x::xs == ys

def DeleteSignature.Goal : Prop := ∀ a b, near a b = true →
  ((a::deletes a).any fun s => (b::deletes b).contains s) = true

/-- Sorted dictionary prefix counts from adjacent LCPs; each nonempty distinct prefix
is introduced once by the first word containing it. -/
def prefixes (s : Bytes) := (List.range s.length).map fun i => s.take (i+1)

def PrefixCardinality.Goal : Prop := ∀ (dict : List Bytes), dict.Pairwise (· ≤ ·) →
  ((([]::dict).zip dict).map fun (a,b) => b.length-lcp a b).sum =
    ((dict.flatMap prefixes).eraseDups).length

/-- A page-local common prefix can be serialized once; sorting only the suffixes
preserves the ordered bag, including duplicate empty suffixes. -/
def StripSort.Goal : Prop := ∀ (p : Bytes) (xs : List Bytes),
  (∀ x ∈ xs, p.isPrefixOf x = true) →
  ((xs.map (List.drop p.length)).mergeSort.map (p ++ ·)) = xs.mergeSort

/-- Count substring occurrences wholly inside chunks plus boundary-crossing starts.
The boundary window needs at most |p|-1 bytes from each side, even for overlaps. -/
def occurrences (s p : Bytes) := ((List.range (s.length+1)).filter (matchesAt s p)).length

def AcrossChunks.Goal : Prop := ∀ (a b p : Bytes), p ≠ [] →
  let cut := a.drop (a.length-(p.length-1))
  let fringe := cut ++ b.take (p.length-1)
  occurrences (a++b) p = occurrences a p + occurrences b p +
    ((List.range cut.length).filter (fun i => cut.length < i+p.length && matchesAt fringe p i)).length

/-- A unique byte delimiter in a multi-column key is unnecessary: escape a zero payload
byte as 00 ff and terminate fields with 00 00. Encoded bytes preserve tuple order. -/
def field (s : Bytes) : Bytes := (s.flatMap fun b => if b == 0 then [0,255] else [b]) ++ [0,0]

def TupleEncoding.Goal : Prop := ∀ (a b c d : Bytes),
  compare (field a ++ field b) (field c ++ field d) =
    (match compare a c with | .eq => compare b d | x => x)

#guard upper [1,255,255] == some [2] && upper [255] == none
#guard nextCmp [1,2] [1] [1,2,3] 1 2 == .lt
#guard ((List.range 4).filter (matchesAt [1,1,1] [1,1])) == [0,1]
#guard near [1,2] [2,1] == false && near [1,2] [1,3] == true
#guard (([[1],[1],[1,2]] : List Bytes).flatMap prefixes).eraseDups.length == 2
#guard (([[1],[1,0]] : List Bytes).map (List.drop 1)) == [[],[0]]
#guard occurrences [1,1,1] [1,1] == 2
#guard compare (field [0] ++ field []) (field [] ++ field [255]) == .gt

end TinylambOpt.ResearchGoals.BinaryText
