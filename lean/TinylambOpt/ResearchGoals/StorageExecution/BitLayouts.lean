import TinylambOpt.ResearchGoals.Compression.Wire

/-!
SQL column readers can work with packed bits when lookup and filtering preserve the
represented values and row positions.
-/

namespace TinylambOpt.ResearchGoals.StorageExecution.BitLayouts
open TinylambOpt.ResearchGoals.Compression.Wire

/-! Physical positions of bits in an unsigned n-row, w-bit column. Byte sets
are explicit so a gather or masked patch has a measurable I/O contract. -/
structure Layout where
  bit : Nat → Nat → Nat → Nat → Nat  -- rows, width, ordinal, significance
  capacity : Nat → Nat → Nat         -- bits including internal padding
def lanes (w : Nat) := List.range w
def footprint (l : Layout) (n w i : Nat) := ((lanes w).map (fun b => l.bit n w i b / 8)).eraseDups
def image (l : Layout) (w : Nat) (xs : List Nat) :=
  xs.zipIdx.foldl (fun acc (x,i) => (lanes w).foldl
    (fun a b => if x.testBit b then a ||| 2^(l.bit xs.length w i b) else a) acc) 0
def file (l : Layout) (w : Nat) (xs : List Nat) :=
  little (image l w xs) (byteCount (l.capacity xs.length w))
def fetch (l : Layout) (n w i : Nat) (read : Nat → Nat) :=
  ((lanes w).map (fun b => let p := l.bit n w i b
                            if (read (p/8)).testBit (p%8) then 2^b else 0)).sum
def atByte (bs : Bytes) (j : Nat) := bs[j]?.getD 0
def planeRows (l : Layout) (n w b : Nat) (bs : Bytes) :=
  (List.range n).filter (fun i => let p := l.bit n w i b
                                  (atByte bs (p/8)).testBit (p%8))
def rowMask (l : Layout) (n w i : Nat) :=
  (lanes w).foldl (fun a b => a ||| 2^(l.bit n w i b)) 0
def rowBits (l : Layout) (n w i x : Nat) :=
  (lanes w).foldl (fun a b => if x.testBit b then a ||| 2^(l.bit n w i b) else a) 0
def addressSet (l : Layout) (n w : Nat) (rows : List Nat) :=
  (rows.flatMap (footprint l n w)).eraseDups

/-- Read one lane from serialized bytes, including zero width. -/
def lossless (l : Layout) : Prop := ∀ w xs i, i < xs.length →
  fetch l xs.length w i (atByte (file l w xs)) = xs[i]?.getD 0 % 2^w

/-- A writer may fill distinct bit cells concurrently. -/
def nonoverlap (l : Layout) : Prop := ∀ n w i j a b, i<n → j<n → a<w → b<w →
  (l.bit n w i a = l.bit n w j b ↔ i=j ∧ a=b)

/-- Replace one lane by clearing its mask and inserting a new value. -/
def maskedPatch (l : Layout) : Prop := ∀ w xs i x, i < xs.length →
  image l w (xs.set i x) = (image l w xs ^^^ (image l w xs &&& rowMask l xs.length w i)) ||| rowBits l xs.length w i x

/-- BIT_TEST can produce matching row IDs from a single physical bit per row. -/
def planePredicate (l : Layout) : Prop := ∀ w xs b, b<w → planeRows l xs.length w b (file l w xs) =
  (List.range xs.length).filter (fun i => (xs[i]?.getD 0).testBit b)

/-- Only source bytes requested by selected ordinals need to survive cache eviction. -/
def sparseGather (l : Layout) : Prop := ∀ n w rows (a b : Nat → Nat),
  (∀ j ∈ addressSet l n w rows, a j = b j) → rows.map (fun i => fetch l n w i a) = rows.map (fun i => fetch l n w i b)

/-- Convert from this physical order to tight row-major bytes using lane reads. -/
def transcode (l : Layout) : Prop := ∀ w xs, pack w ((List.range xs.length).map
  (fun i => fetch l xs.length w i (atByte (file l w xs)))) = pack w xs

/-- Read only lanes requested by a probe and translate them through a possibly short dictionary. -/
def dictionaryProbe (l : Layout) : Prop := ∀ w xs (dict keys : List Nat), (∀ i ∈ keys, i < xs.length) →
  keys.map (fun i => dict[fetch l xs.length w i (atByte (file l w xs))]?) =
    keys.map (fun i => dict[xs[i]?.getD 0 % 2^w]?)

/-- Count addressed bytes once and bound a gather by both the physical frame and the naive per-bit read count. -/
def pageBudget (l : Layout) : Prop := ∀ n w rows, (∀ i ∈ rows, i<n) →
  (addressSet l n w rows).length ≤ min (byteCount (l.capacity n w)) (rows.eraseDups.length*w)

namespace RowMajor

/-- Consecutive packed values. -/
def layout : Layout := ⟨fun _ w i b => i*w+b, fun n w => n*w⟩

def Lossless.Goal : Prop := lossless layout
def Nonoverlap.Goal : Prop := nonoverlap layout
def SharedEdgeByte.Goal : Prop :=
  ∀ n w i, 0<w → ((footprint layout n w i).any (footprint layout n w (i+1)).contains) = decide ((i+1)*w%8≠0)
def MaskedPatch.Goal : Prop := maskedPatch layout
def PlanePredicate.Goal : Prop := planePredicate layout
def SparseGather.Goal : Prop := sparseGather layout
def Transcode.Goal : Prop := transcode layout
def DictionaryProbe.Goal : Prop := dictionaryProbe layout
def PageBudget.Goal : Prop := pageBudget layout

def PointSpan.Goal : Prop := ∀ n w i, (footprint layout n w i).length ≤ byteCount (w+7)

end RowMajor

namespace Vertical

/-- One bitmap per bit significance. -/
def layout : Layout := ⟨fun n _ i b => b*n+i, fun n w => n*w⟩

def Lossless.Goal : Prop := lossless layout
def Nonoverlap.Goal : Prop := nonoverlap layout
def PlaneScanSpan.Goal : Prop :=
  ∀ n w b, n%8=0 → b<w → ((List.range n).map (fun i => layout.bit n w i b/8)).eraseDups.length = n/8
def MaskedPatch.Goal : Prop := maskedPatch layout
def PlanePredicate.Goal : Prop := planePredicate layout
def SparseGather.Goal : Prop := sparseGather layout
def Transcode.Goal : Prop := transcode layout
def DictionaryProbe.Goal : Prop := dictionaryProbe layout
def PageBudget.Goal : Prop := pageBudget layout

def AlignedPlane.Goal : Prop := ∀ n w b, n%8=0 → b<w → (layout.bit n w 0 b)%8=0

end Vertical

namespace SerpentineRow

/-- Alternate the bit order of successive values. -/
def layout : Layout := ⟨fun _ w i b => i*w + (if i%2=0 then b else w-1-b), fun n w => n*w⟩

def Lossless.Goal : Prop := lossless layout
def Nonoverlap.Goal : Prop := nonoverlap layout
def DoubleStride.Goal : Prop :=
  ∀ n w i b, layout.bit n w (i+2) b = layout.bit n w i b + 2*w
def MaskedPatch.Goal : Prop := maskedPatch layout
def PlanePredicate.Goal : Prop := planePredicate layout
def SparseGather.Goal : Prop := sparseGather layout
def Transcode.Goal : Prop := transcode layout
def DictionaryProbe.Goal : Prop := dictionaryProbe layout
def PageBudget.Goal : Prop := pageBudget layout

def FacingEndpoints.Goal : Prop := ∀ n w i, i%2=0 → 0<w → layout.bit n w i (w-1)+1 = layout.bit n w (i+1) (w-1)

end SerpentineRow

namespace SerpentinePlane

/-- Reverse ordinal order on odd bit planes. -/
def layout : Layout := ⟨fun n _ i b => b*n + (if b%2=0 then i else n-1-i), fun n w => n*w⟩

def Lossless.Goal : Prop := lossless layout
def Nonoverlap.Goal : Prop := nonoverlap layout
def SharedPlaneEdge.Goal : Prop :=
  ∀ n w b, 0<n → b+1<w → b%2=0 → (layout.bit n w (n-1) b/8 = layout.bit n w (n-1) (b+1)/8 ↔ ((b+1)*n)%8≠0)
def MaskedPatch.Goal : Prop := maskedPatch layout
def PlanePredicate.Goal : Prop := planePredicate layout
def SparseGather.Goal : Prop := sparseGather layout
def Transcode.Goal : Prop := transcode layout
def DictionaryProbe.Goal : Prop := dictionaryProbe layout
def PageBudget.Goal : Prop := pageBudget layout

def FoldedBoundary.Goal : Prop := ∀ n w b, 0<n → b%2=0 → layout.bit n w (n-1) b + 1 = layout.bit n w (n-1) (b+1)

end SerpentinePlane

namespace TileTranspose

/-- Transpose groups of eight ordinals and pad the last tile. -/
def layout : Layout := ⟨fun _ w i b => (i/8)*8*w+b*8+i%8, fun n w => align 8 n*w⟩

def Lossless.Goal : Prop := lossless layout
def Nonoverlap.Goal : Prop := nonoverlap layout
def OneBytePerPlane.Goal : Prop :=
  ∀ n w tile, (addressSet layout n w ((List.range 8).map (tile*8+·))).length=w
def MaskedPatch.Goal : Prop := maskedPatch layout
def PlanePredicate.Goal : Prop := planePredicate layout
def SparseGather.Goal : Prop := sparseGather layout
def Transcode.Goal : Prop := transcode layout
def DictionaryProbe.Goal : Prop := dictionaryProbe layout
def PageBudget.Goal : Prop := pageBudget layout

def TileCharge.Goal : Prop := ∀ n w tile b i, i<8 → layout.bit n w (tile*8+i) b / 8 = tile*w+b

end TileTranspose

namespace ByteAlignedRow

/-- Individually byte-aligned words. -/
def layout : Layout := ⟨fun _ w i b => i*align 8 w+b, fun n w => n*align 8 w⟩

def Lossless.Goal : Prop := lossless layout
def Nonoverlap.Goal : Prop := nonoverlap layout
def DisjointRowWrites.Goal : Prop :=
  ∀ n w i j, i≠j → ∀ b ∈ footprint layout n w i, b ∉ footprint layout n w j
def MaskedPatch.Goal : Prop := maskedPatch layout
def PlanePredicate.Goal : Prop := planePredicate layout
def SparseGather.Goal : Prop := sparseGather layout
def Transcode.Goal : Prop := transcode layout
def DictionaryProbe.Goal : Prop := dictionaryProbe layout
def PageBudget.Goal : Prop := pageBudget layout

def PointBytes.Goal : Prop := ∀ n w i, (footprint layout n w i).length = byteCount w

end ByteAlignedRow

namespace ByteAlignedPlane

/-- Byte-align each bitmap independently. -/
def layout : Layout := ⟨fun n _ i b => b*align 8 n+i, fun n w => w*align 8 n⟩

def Lossless.Goal : Prop := lossless layout
def Nonoverlap.Goal : Prop := nonoverlap layout
def PlaneDirectory.Goal : Prop :=
  ∀ n w, byteCount (layout.capacity n w) = w*byteCount n
def MaskedPatch.Goal : Prop := maskedPatch layout
def PlanePredicate.Goal : Prop := planePredicate layout
def SparseGather.Goal : Prop := sparseGather layout
def Transcode.Goal : Prop := transcode layout
def DictionaryProbe.Goal : Prop := dictionaryProbe layout
def PageBudget.Goal : Prop := pageBudget layout

def AppendWithinPad.Goal : Prop := ∀ n w i b, 0<n → n%8≠0 → layout.bit (n+1) w i b = layout.bit n w i b

end ByteAlignedPlane

namespace NetworkBitOrder

/-- Reverse the physical bit order within each byte of a tight stream. -/
def layout : Layout := ⟨fun _ w i b => let p := i*w+b; p/8*8 + (7-p%8), fun n w => align 8 (n*w)⟩

def Lossless.Goal : Prop := lossless layout
def Nonoverlap.Goal : Prop := nonoverlap layout
def BitReversal.Goal : Prop :=
  ∀ n w i b, let p := i*w+b; (layout.bit n w i b)%8 + p%8 = 7 ∧ layout.bit n w i b/8=p/8
def MaskedPatch.Goal : Prop := maskedPatch layout
def PlanePredicate.Goal : Prop := planePredicate layout
def SparseGather.Goal : Prop := sparseGather layout
def Transcode.Goal : Prop := transcode layout
def DictionaryProbe.Goal : Prop := dictionaryProbe layout
def PageBudget.Goal : Prop := pageBudget layout

def ByteFootprint.Goal : Prop := ∀ n w i, footprint layout n w i = footprint RowMajor.layout n w i

end NetworkBitOrder

namespace RotatedPlane

/-- Rotate each plane by its significance to stagger banks. -/
def layout : Layout := ⟨fun n _ i b => b*n+(i+b)%n, fun n w => n*w⟩

def Lossless.Goal : Prop := lossless layout
def Nonoverlap.Goal : Prop := nonoverlap layout
def RotatedLocations.Goal : Prop :=
  ∀ n w b, 0<n → (((List.range n).map (fun i => layout.bit n w i b))).Perm ((List.range n).map (b*n+·))
def MaskedPatch.Goal : Prop := maskedPatch layout
def PlanePredicate.Goal : Prop := planePredicate layout
def SparseGather.Goal : Prop := sparseGather layout
def Transcode.Goal : Prop := transcode layout
def DictionaryProbe.Goal : Prop := dictionaryProbe layout
def PageBudget.Goal : Prop := pageBudget layout

def BankCycle.Goal : Prop := ∀ n w i b, 0<n → layout.bit n w (i+n) b = layout.bit n w i b

end RotatedPlane

namespace SplitNibble

/-- Row-major low nibble followed by transposed high bits. -/
def layout : Layout := ⟨fun n w i b => if b<4 then i*min 4 w+b else n*min 4 w+(b-4)*n+i, fun n w => n*w⟩

def Lossless.Goal : Prop := lossless layout
def Nonoverlap.Goal : Prop := nonoverlap layout
def MixedPointCost.Goal : Prop :=
  ∀ n w i, 4≤w → (footprint layout n w i).length ≤ 1+(w-4)
def MaskedPatch.Goal : Prop := maskedPatch layout
def PlanePredicate.Goal : Prop := planePredicate layout
def SparseGather.Goal : Prop := sparseGather layout
def Transcode.Goal : Prop := transcode layout
def DictionaryProbe.Goal : Prop := dictionaryProbe layout
def PageBudget.Goal : Prop := pageBudget layout

def LowNibbleLocal.Goal : Prop := ∀ n w i b, 4≤w → b<4 → layout.bit n w i b / 8 = i/2

end SplitNibble

#guard file Vertical.layout 2 [1,2,3] == [53]
#guard fetch TileTranspose.layout 3 2 1 (atByte (file TileTranspose.layout 2 [1,2,3])) == 2

end TinylambOpt.ResearchGoals.StorageExecution.BitLayouts
