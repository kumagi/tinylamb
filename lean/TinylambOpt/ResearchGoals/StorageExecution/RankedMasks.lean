import TinylambOpt.ResearchGoals.Compression.Wire
import TinylambOpt.ResearchGoals.Atlas.SynopsisBounds

namespace TinylambOpt.ResearchGoals.StorageExecution.RankedMasks
open TinylambOpt.ResearchGoals.Compression.Wire

/-! Constrained bitmaps can be represented by their lexicographic rank among
accepted pages of the same length. The state is sufficient for restarting a
suffix. The model counts paths without materializing the list of bitmaps. -/
structure Machine where
  step : Nat → Bool → Option Nat
  accept : Nat → Bool
def run (m : Machine) (q : Nat) (xs : List Bool) := xs.foldlM m.step q
def legal (m : Machine) (q : Nat) (xs : List Bool) := ((run m q xs).map m.accept).getD false
def count (m : Machine) : Nat → Nat → Nat
  | 0,q => (m.accept q).toNat
  | n+1,q => ((m.step q false).map (count m n)).getD 0 + ((m.step q true).map (count m n)).getD 0
def lowCount (m : Machine) (n q : Nat) := ((m.step q false).map (count m n)).getD 0
def rank (m : Machine) : Nat → List Bool → Option Nat
  | q,[] => if m.accept q then some 0 else none
  | q,b::bs => do
    let next ← m.step q b
    let tail ← rank m next bs
    return tail + (if b then lowCount m bs.length q else 0)
def unrank (m : Machine) : Nat → Nat → Nat → Option (List Bool)
  | 0,q,r => if r==0 && m.accept q then some [] else none
  | n+1,q,r => do
    let low := lowCount m n q
    let b := decide (low ≤ r)
    let next ← m.step q b
    let bs ← unrank m n next (if b then r-low else r)
    return b::bs
def words : Nat → List (List Bool)
  | 0 => [[]]
  | n+1 => (words n).map (false::·) ++ (words n).map (true::·)
def descend (m : Machine) : Nat → Nat → List Bool → Option (Nat × Nat)
  | _,q,[] => some (q,0)
  | n+1,q,b::bs => do
    let q' ← m.step q b
    let (last,off) ← descend m n q' bs
    return (last,off+if b then lowCount m n q else 0)
  | 0,_,_::_ => none
def codeBits (m : Machine) (n q : Nat) := width (count m n q - 1)

/-- Combinatorial rank equals position among accepted bitmaps. -/
def ordinal (m : Machine) : Prop := ∀ q xs, legal m q xs →
  rank m q xs = some (((words xs.length).filter (legal m q)).idxOf xs)

/-- Recreate every accepted bitmap from its compact code. -/
def decoder (m : Machine) : Prop := ∀ q xs r, rank m q xs = some r → unrank m xs.length q r = some xs

/-- Reject unused bit patterns in the final code word. -/
def rejectSlack (m : Machine) : Prop := ∀ n q r, (unrank m n q r).isNone = decide (count m n q ≤ r)

/-- Accepted completions of a fixed bitmap prefix occupy a contiguous interval of ranks. -/
def prefixRange (m : Machine) : Prop := ∀ n q pre q' off r, descend m n q pre = some (q',off) → r<count m n q →
  ((unrank m n q r).any (fun bs => pre.isPrefixOf bs)) = decide (off≤r ∧ r<off+count m (n-pre.length) q')

/-- Encode a suffix from a restart state and shift it by the prefix interval. -/
def splitCode (m : Machine) : Prop := ∀ q a b q' off, descend m (a.length+b.length) q a = some (q',off) →
  rank m q (a++b) = (rank m q' b).map (off+·)

/-- A rank packed into whole bytes decodes despite byte padding. -/
def wireLookup (m : Machine) : Prop := ∀ q xs r, rank m q xs=some r →
  unrank m xs.length q (get (pack (codeBits m xs.length q) [r]) (codeBits m xs.length q) 0) = some xs

/-- A shared language and row count can be amortized across equally sized bitmaps. -/
def spaceTest (m : Machine) : Prop := ∀ n q pages h, 0<pages → 0<count m n q →
  (h + byteCount (pages*codeBits m n q) < pages*byteCount n ↔
   h < pages*byteCount n ∧ pages*codeBits m n q ≤ 8*(pages*byteCount n-h-1))

/-- Compute a parallel shard count by conditioning on its prefix state. -/
def cofactor (m : Machine) : Prop := ∀ q a b, count m (a+b) q =
  ((words a).map (fun pre => ((run m q pre).map (count m b)).getD 0)).sum

namespace FixedPopcount

/-- State is the exact number of hits still required. -/
def model : Machine := ⟨fun q b => if b && q==0 then none else some (q-b.toNat), fun q => q==0⟩

def Ordinal.Goal : Prop := ordinal model
def Decoder.Goal : Prop := decoder model
def RejectSlack.Goal : Prop := rejectSlack model
def PrefixRange.Goal : Prop := prefixRange model
def SplitCode.Goal : Prop := splitCode model
def ComplementSymmetry.Goal : Prop :=
  ∀ n k, k≤n → codeBits model n k = codeBits model n (n-k)
def WireLookup.Goal : Prop := wireLookup model
def SpaceTest.Goal : Prop := spaceTest model
def Cofactor.Goal : Prop := cofactor model

/-- One count byte followed by a little-endian rank, as in the fixture probe. -/
def PopulationFrame.Goal : Prop := ∀ xs, xs.length < 256 →
  let n := xs.length
  let k := xs.count true
  let r := (rank model k xs).getD 0
  let bits := width (TinylambOpt.ResearchGoals.Atlas.SynopsisBounds.Samples.choose n k - 1)
  let packet := [k] ++ little r (byteCount bits)
  unrank model n (packet.head?.getD 0) (fromLittle (packet.drop 1)) = some xs ∧
    packet.length = 1+byteCount (codeBits model n k)

end FixedPopcount

namespace AtMostHits

/-- Store pages with at most the remaining exception budget. -/
def model : Machine := ⟨fun q b => if b && q==0 then none else some (q-b.toNat), fun _ => true⟩

def Ordinal.Goal : Prop := ordinal model
def Decoder.Goal : Prop := decoder model
def RejectSlack.Goal : Prop := rejectSlack model
def PrefixRange.Goal : Prop := prefixRange model
def SplitCode.Goal : Prop := splitCode model
def OneOutlier.Goal : Prop :=
  ∀ n, count model n 1 = n+1
def WireLookup.Goal : Prop := wireLookup model
def SpaceTest.Goal : Prop := spaceTest model
def Cofactor.Goal : Prop := cofactor model

def UnrestrictedBudget.Goal : Prop := ∀ n q, n≤q → count model n q = 2^n

end AtMostHits

namespace IndependentHits

/-- No two selected row IDs are adjacent. -/
def model : Machine := ⟨fun q b => if b && q==1 then none else some b.toNat, fun _ => true⟩

def Ordinal.Goal : Prop := ordinal model
def Decoder.Goal : Prop := decoder model
def RejectSlack.Goal : Prop := rejectSlack model
def PrefixRange.Goal : Prop := prefixRange model
def SplitCode.Goal : Prop := splitCode model
def CheapConcatenation.Goal : Prop :=
  ∀ q a b, legal model q a → legal model 0 b → legal model q (a++[false]++b)
def WireLookup.Goal : Prop := wireLookup model
def SpaceTest.Goal : Prop := spaceTest model
def Cofactor.Goal : Prop := cofactor model

def FibonacciCount.Goal : Prop := ∀ n, count model (n+2) 0 = count model (n+1) 0 + count model n 0

end IndependentHits

namespace PairedHoles

/-- Zero bits occur in even-length runs; set bits delimit holes. -/
def model : Machine := ⟨fun q b => if b then (if q==0 then some 0 else none) else some (1-q), fun q => q==0⟩

def Ordinal.Goal : Prop := ordinal model
def Decoder.Goal : Prop := decoder model
def RejectSlack.Goal : Prop := rejectSlack model
def PrefixRange.Goal : Prop := prefixRange model
def SplitCode.Goal : Prop := splitCode model
def EraseZeroPair.Goal : Prop :=
  ∀ a b, legal model 0 (a++[false,false]++b) = legal model 0 (a++b)
def WireLookup.Goal : Prop := wireLookup model
def SpaceTest.Goal : Prop := spaceTest model
def Cofactor.Goal : Prop := cofactor model

def EvenTail.Goal : Prop := ∀ n, legal model 0 (List.replicate n false) = decide (n%2=0)

end PairedHoles

namespace PrefixOnes

/-- Clustered matches form a single prefix of the block. -/
def model : Machine := ⟨fun q b => if q==1 && b then none else some (if b then 0 else 1), fun _ => true⟩

def Ordinal.Goal : Prop := ordinal model
def Decoder.Goal : Prop := decoder model
def RejectSlack.Goal : Prop := rejectSlack model
def PrefixRange.Goal : Prop := prefixRange model
def SplitCode.Goal : Prop := splitCode model
def CutOrdinal.Goal : Prop :=
  ∀ n k, k≤n → rank model 0 (List.replicate k true ++ List.replicate (n-k) false)=some k
def WireLookup.Goal : Prop := wireLookup model
def SpaceTest.Goal : Prop := spaceTest model
def Cofactor.Goal : Prop := cofactor model

def CutCount.Goal : Prop := ∀ n, count model n 0 = n+1

end PrefixOnes

namespace EvenParity

/-- Last bit is implied by the page parity. -/
def model : Machine := ⟨fun q b => some ((q+b.toNat)%2), fun q => q==0⟩

def Ordinal.Goal : Prop := ordinal model
def Decoder.Goal : Prop := decoder model
def RejectSlack.Goal : Prop := rejectSlack model
def PrefixRange.Goal : Prop := prefixRange model
def SplitCode.Goal : Prop := splitCode model
def ComplementOrder.Goal : Prop :=
  ∀ xs, xs.length%2=0 → legal model 0 xs → (rank model 0 xs).getD 0 + (rank model 0 (xs.map (!·))).getD 0 = count model xs.length 0-1
def WireLookup.Goal : Prop := wireLookup model
def SpaceTest.Goal : Prop := spaceTest model
def Cofactor.Goal : Prop := cofactor model

def ImpliedLastBit.Goal : Prop := ∀ n q, q<2 → count model (n+1) q = 2^n

end EvenParity

namespace BalancedNesting

/-- Open/close events cannot go below the page base depth. -/
def model : Machine := ⟨fun q b => if b then some (q+1) else if q==0 then none else some (q-1), fun q => q==0⟩

def Ordinal.Goal : Prop := ordinal model
def Decoder.Goal : Prop := decoder model
def RejectSlack.Goal : Prop := rejectSlack model
def PrefixRange.Goal : Prop := prefixRange model
def SplitCode.Goal : Prop := splitCode model
def DepthCap.Goal : Prop :=
  ∀ n q, n<q → count model n q=0
def WireLookup.Goal : Prop := wireLookup model
def SpaceTest.Goal : Prop := spaceTest model
def Cofactor.Goal : Prop := cofactor model

def OddLengthEmpty.Goal : Prop := ∀ n, count model (2*n+1) 0 = 0

end BalancedNesting

namespace RunLimit

/-- Three consecutive set bits are forbidden. -/
def model : Machine := ⟨fun q b => if b then (if q<2 then some (q+1) else none) else some 0, fun _ => true⟩

def Ordinal.Goal : Prop := ordinal model
def Decoder.Goal : Prop := decoder model
def RejectSlack.Goal : Prop := rejectSlack model
def PrefixRange.Goal : Prop := prefixRange model
def SplitCode.Goal : Prop := splitCode model
def EraseSeparator.Goal : Prop :=
  ∀ a b, legal model 0 a → legal model 0 b → legal model 0 (a++[false]++b)
def WireLookup.Goal : Prop := wireLookup model
def SpaceTest.Goal : Prop := spaceTest model
def Cofactor.Goal : Prop := cofactor model

def TribonacciCount.Goal : Prop := ∀ n, count model (n+3) 0 = count model (n+2) 0 + count model (n+1) 0 + count model n 0

end RunLimit

namespace SingleIsland

/-- At most one contiguous run of selected rows. -/
def model : Machine := ⟨fun q b => if q==2 && b then none else some (if b then 1 else if q==0 then 0 else 2), fun _ => true⟩

def Ordinal.Goal : Prop := ordinal model
def Decoder.Goal : Prop := decoder model
def RejectSlack.Goal : Prop := rejectSlack model
def PrefixRange.Goal : Prop := prefixRange model
def SplitCode.Goal : Prop := splitCode model
def EmptyFirst.Goal : Prop :=
  ∀ n, unrank model n 0 0 = some (List.replicate n false)
def WireLookup.Goal : Prop := wireLookup model
def SpaceTest.Goal : Prop := spaceTest model
def Cofactor.Goal : Prop := cofactor model

def IntervalsAndEmpty.Goal : Prop := ∀ n, count model n 0 = 1+n*(n+1)/2

end SingleIsland

namespace Cooldown

/-- A hit forces two unselected ordinals before the next hit. -/
def model : Machine := ⟨fun q b => if b then (if q==0 then some 2 else none) else some (q-1), fun _ => true⟩

def Ordinal.Goal : Prop := ordinal model
def Decoder.Goal : Prop := decoder model
def RejectSlack.Goal : Prop := rejectSlack model
def PrefixRange.Goal : Prop := prefixRange model
def SplitCode.Goal : Prop := splitCode model
def CheckpointConvergence.Goal : Prop :=
  ∀ q, q≤2 → run model q [false,false]=some 0
def WireLookup.Goal : Prop := wireLookup model
def SpaceTest.Goal : Prop := spaceTest model
def Cofactor.Goal : Prop := cofactor model

def SpacingRecurrence.Goal : Prop := ∀ n, count model (n+3) 0 = count model (n+2) 0 + count model n 0

end Cooldown

#guard rank FixedPopcount.model 2 [true,false,true,false] == some 4
#guard unrank IndependentHits.model 4 0 8 == none

end TinylambOpt.ResearchGoals.StorageExecution.RankedMasks
