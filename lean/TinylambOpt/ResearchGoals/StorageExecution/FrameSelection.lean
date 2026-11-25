import TinylambOpt.ResearchGoals.Compression.Wire
import TinylambOpt.ResearchGoals.StorageExecution.Boundaries

namespace TinylambOpt.ResearchGoals.StorageExecution.FrameSelection
open TinylambOpt.ResearchGoals.Compression.Wire

/-! Self-delimiting records for unsigned 32-bit lanes. Header sizes include a
format tag and ULEB row count. Plans pay for the serialization of their chosen
payload plus restart/offset entries, even for an incompressible block. -/
abbrev Writer := List Nat → Bytes
def fixed (xs : List Nat) := xs.flatMap (fun x => little x 4)
def raw (xs : List Nat) := [0] ++ uleb xs.length ++ fixed xs
def sizes (xs : List Nat) := xs.map (fun v => max 1 (byteCount (width v)))
def runs : List Nat → List (Nat × Nat)
  | [] => []
  | x::xs => match runs xs with
    | [] => [(x,1)]
    | (v,n)::rest => if v==x then (x,n+1)::rest else (x,1)::(v,n)::rest
def delta : Nat → List Nat → List Nat
  | _,[] => []
  | p,x::xs => zig ((x:Int)-p) :: delta x xs
def frame (f : Writer) (xs : List Nat) := [1] ++ uleb xs.length ++ f xs
def chosen (f : Writer) (xs : List Nat) := min (frame f xs).length (raw xs).length
def parts : List Nat → List (List (List Nat))
  | [] => [[]]
  | x::xs => (parts xs).flatMap (fun ps => match ps with
    | [] => [[[x]]]
    | a::rest => [[x]::a::rest,(x::a)::rest])
def charge (f : Writer) (fee : Nat) (ps : List (List Nat)) :=
  (ps.map (fun p => fee + chosen f p)).sum
def seekWork (ps : List (List Nat)) := (ps.map (fun p => p.length^2)).sum
def plan (f : Writer) (fee price : Nat) (xs : List Nat) :=
  ((parts xs).mergeSort (fun a b => charge f fee a + price*seekWork a ≤ charge f fee b + price*seekWork b)).head?.getD []
def budget (f : Writer) (xs : List Nat) (cap : Nat) :=
  (((parts xs).filter (fun ps => ps.all (fun p => p.length≤cap))).map (charge f 0)).min?
def saved (f : Writer) (xs : List Nat) := (raw xs).length-(frame f xs).length
def pageLimit : Nat → Nat → Nat := fun n cap => (n+cap-1)/cap
def admissible (xs : List Nat) := ∀ x ∈ xs, x < 2^32

def quotedOptimum (f : Writer) (fee price : Nat) (xs : List Nat) :=
  boundaryCost xs.length fun a b =>
    fee + chosen f ((xs.drop a).take (b-a)) + price*(b-a)^2

/-- Use the actual codec and raw fallback in the size/seek optimum. -/
def byteFrontier (f : Writer) : Prop := ∀ xs fee price ps, admissible xs → ps ∈ parts xs →
  let pick := plan f fee price xs
  quotedOptimum f fee price xs = charge f fee pick + price*seekWork pick ∧
    quotedOptimum f fee price xs ≤ charge f fee ps + price*seekWork ps

/-- Sum net page gains after counting the fixed-width alternative and each restart entry. -/
def strictSavings (f : Writer) : Prop := ∀ ps fee, charge f fee ps + (ps.map (saved f)).sum =
  (ps.map (fun p => (raw p).length)).sum + fee*ps.length

/-- Raising point-read cost can only reduce the squared decoded lengths of optimal pages. -/
def hotPointReads (f : Writer) : Prop := ∀ xs fee p q, p<q →
  seekWork (plan f fee q xs) ≤ seekWork (plan f fee p xs)

/-- Relaxing maximum rows per frame bounds the best fully framed size. -/
def seekCap (f : Writer) : Prop := ∀ xs a b s, a≤b → budget f xs a=some s →
  ∃ t≤s, budget f xs b=some t

/-- Independent workers may force a boundary; global optimum is bounded by the sum of their plans. -/
def parallelEncode (f : Writer) : Prop := ∀ a b fee, charge f fee (plan f fee 0 (a++b)) ≤
  charge f fee (plan f fee 0 a) + charge f fee (plan f fee 0 b)

/-- A page budget must cover at least the required number of tags and restart entries. -/
def mandatoryCuts (f : Writer) : Prop := ∀ xs cap fee ps, 0<cap → ps∈parts xs → (∀ p∈ps, p.length≤cap) →
  (fee+2)*pageLimit xs.length cap ≤ charge f fee ps

/-- A forced frame cap still limits worst-case expansion to raw cells and per-frame overhead. -/
def boundedRaw (f : Writer) : Prop := ∀ xs cap s, 0<cap → budget f xs cap=some s →
  s ≤ xs.length*4 + pageLimit xs.length cap*(1+ubytes cap)

/-- Aggressive hypothesis: the cheapest two adjacent groups never need reconsideration when a third arrives. -/
def greedyCut (f : Writer) : Prop := ∀ a b c, a≠[] → b≠[] → c≠[] →
  chosen f (a++b) ≤ chosen f a+chosen f b →
  min (chosen f (a++b++c)) (chosen f (a++b)+chosen f c) ≤ chosen f a+chosen f (b++c)

namespace SeparatedLengths

/-- Stream VByte control bytes, four 2-bit length codes per byte. -/
def write (xs : List Nat) : Bytes :=
  pack 2 ((sizes xs).map (·-1)) ++ (xs.zip (sizes xs)).flatMap (fun (v,w) => little v w)

def ByteFrontier.Goal : Prop := byteFrontier write
def StrictSavings.Goal : Prop := strictSavings write
def HotPointReads.Goal : Prop := hotPointReads write
def SeekCap.Goal : Prop := seekCap write
def ParallelEncode.Goal : Prop := parallelEncode write
def FourControls.Goal : Prop :=
  ∀ a b, a.length%4=0 → admissible (a++b) → (write (a++b)).length = (write a).length+(write b).length
def MandatoryCuts.Goal : Prop := mandatoryCuts write
def BoundedRaw.Goal : Prop := boundedRaw write
def GreedyCut.Goal : Prop := greedyCut write

def ControlBytes.Goal : Prop := ∀ xs, admissible xs → (write xs).length = byteCount (2*xs.length) + (sizes xs).sum

end SeparatedLengths

namespace ConstantExceptions

/-- Modal candidate from the first lane; each exception has an ordinal and four raw bytes. -/
def write (xs : List Nat) : Bytes :=
  let base := xs.head?.getD 0
  let ex := xs.zipIdx.filter (fun (v,_) => v != base)
  little base 4 ++ uleb ex.length ++ ex.flatMap (fun (v,i) => uleb i ++ little v 4)

def ByteFrontier.Goal : Prop := byteFrontier write
def StrictSavings.Goal : Prop := strictSavings write
def HotPointReads.Goal : Prop := hotPointReads write
def SeekCap.Goal : Prop := seekCap write
def ParallelEncode.Goal : Prop := parallelEncode write
def ModalBeatsFirst.Goal : Prop :=
  ∀ a b n, a≠b → 3≤n → (write (b::a::List.replicate (n-1) b)).length < (write (a::List.replicate n b)).length
def MandatoryCuts.Goal : Prop := mandatoryCuts write
def BoundedRaw.Goal : Prop := boundedRaw write
def GreedyCut.Goal : Prop := greedyCut write

def ConstantCharge.Goal : Prop := ∀ x n, (write (List.replicate n x)).length = 5

end ConstantExceptions

namespace DictionaryRuns

/-- Emit the local dictionary followed by runs of dictionary IDs. -/
def write (xs : List Nat) : Bytes :=
  let d := xs.eraseDups
  uleb d.length ++ fixed d ++ (runs (xs.map d.idxOf)).flatMap (fun (v,n) => uleb v ++ uleb n)

def ByteFrontier.Goal : Prop := byteFrontier write
def StrictSavings.Goal : Prop := strictSavings write
def HotPointReads.Goal : Prop := hotPointReads write
def SeekCap.Goal : Prop := seekCap write
def ParallelEncode.Goal : Prop := parallelEncode write
def DictionaryBoundary.Goal : Prop :=
  ∀ a b, a≠b → ∀ n m, 0<n → 0<m → (write (List.replicate n a ++ List.replicate m b)).length=11+ubytes n+ubytes m
def MandatoryCuts.Goal : Prop := mandatoryCuts write
def BoundedRaw.Goal : Prop := boundedRaw write
def GreedyCut.Goal : Prop := greedyCut write

def RepeatCliff.Goal : Prop := ∀ x n, 0<n → (write (List.replicate n x)).length = 6+ubytes n

end DictionaryRuns

namespace PatchedOctet

/-- Inline offsets under 255 and mark longer values with an escaped residual. -/
def write (xs : List Nat) : Bytes :=
  xs.flatMap (fun v => if v<255 then [v] else 255 :: uleb (v-255))

def ByteFrontier.Goal : Prop := byteFrontier write
def StrictSavings.Goal : Prop := strictSavings write
def HotPointReads.Goal : Prop := hotPointReads write
def SeekCap.Goal : Prop := seekCap write
def ParallelEncode.Goal : Prop := parallelEncode write
def EscapeCliff.Goal : Prop :=
  ∀ v, (write [254,v]).length+1 = (write [255,v]).length
def MandatoryCuts.Goal : Prop := mandatoryCuts write
def BoundedRaw.Goal : Prop := boundedRaw write
def GreedyCut.Goal : Prop := greedyCut write

def EscapeCost.Goal : Prop := ∀ xs, (write xs).length = xs.length + ((xs.filter (· ≥ 255)).map (fun v => ubytes (v-255))).sum

end PatchedOctet

namespace XorPredecessor

/-- ULEB cost depends on changed high bits against the previous value. -/
def write (xs : List Nat) : Bytes :=
  (xs.zip (0::xs)).flatMap (fun (v,p) => uleb (v ^^^ p))

def ByteFrontier.Goal : Prop := byteFrontier write
def StrictSavings.Goal : Prop := strictSavings write
def HotPointReads.Goal : Prop := hotPointReads write
def SeekCap.Goal : Prop := seekCap write
def ParallelEncode.Goal : Prop := parallelEncode write
def JoinCorrection.Goal : Prop :=
  ∀ a b, b≠[] → let p:=a.getLast?.getD 0; let x:=b.head?.getD 0; (write (a++b)).length+ubytes x=(write a).length+(write b).length+ubytes (p ^^^ x)
def MandatoryCuts.Goal : Prop := mandatoryCuts write
def BoundedRaw.Goal : Prop := boundedRaw write
def GreedyCut.Goal : Prop := greedyCut write

def EqualTail.Goal : Prop := ∀ x n, (write (x::List.replicate n x)).length = ubytes x+n

end XorPredecessor

namespace OrdinalResidual

/-- Subtract row ordinal, zigzag and bit-pack in the measured residual width. -/
def residuals (xs : List Nat) := xs.zipIdx.map (fun (v,i) => zig ((v:Int)-i))
def bitWidth (xs : List Nat) := width (peak (residuals xs))
def write (xs : List Nat) : Bytes :=
  uleb (bitWidth xs) ++ pack (bitWidth xs) (residuals xs)

def ByteFrontier.Goal : Prop := byteFrontier write
def StrictSavings.Goal : Prop := strictSavings write
def HotPointReads.Goal : Prop := hotPointReads write
def SeekCap.Goal : Prop := seekCap write
def ParallelEncode.Goal : Prop := parallelEncode write
def ShiftedShard.Goal : Prop :=
  ∀ (a b : List Nat), bitWidth (a ++ b.map (·+a.length)) = max (bitWidth a) (bitWidth b)
def MandatoryCuts.Goal : Prop := mandatoryCuts write
def BoundedRaw.Goal : Prop := boundedRaw write
def GreedyCut.Goal : Prop := greedyCut write

def ConsecutiveKeys.Goal : Prop := ∀ n, (write (List.range n)).length = 1

end OrdinalResidual

namespace SplitHigh

/-- Store low bytes flat and only runs of shared upper bytes. -/
def write (xs : List Nat) : Bytes :=
  xs.map (· % 256) ++ (runs (xs.map (· / 256))).flatMap (fun (v,n) => uleb v ++ uleb n)

def ByteFrontier.Goal : Prop := byteFrontier write
def StrictSavings.Goal : Prop := strictSavings write
def HotPointReads.Goal : Prop := hotPointReads write
def SeekCap.Goal : Prop := seekCap write
def ParallelEncode.Goal : Prop := parallelEncode write
def HighRunMerge.Goal : Prop :=
  ∀ x y n m, x/256=y/256 → 0<n → 0<m → (write (List.replicate n x ++ List.replicate m y)).length = n+m+ubytes (x/256)+ubytes (n+m)
def MandatoryCuts.Goal : Prop := mandatoryCuts write
def BoundedRaw.Goal : Prop := boundedRaw write
def GreedyCut.Goal : Prop := greedyCut write

def CommonUpper.Goal : Prop := ∀ xs h, xs≠[] → (∀ x∈xs, x/256=h) →
  (write xs).length = xs.length+ubytes h+ubytes xs.length

end SplitHigh

namespace MinimumDelta

/-- Store the smallest predecessor residual once and shift all residuals before packing. -/
def write (xs : List Nat) : Bytes :=
  let ds := delta 0 xs
  let a := ds.min?.getD 0
  let off := ds.map (·-a)
  let w := width (peak off)
  uleb a ++ uleb w ++ pack w off

def ByteFrontier.Goal : Prop := byteFrontier write
def StrictSavings.Goal : Prop := strictSavings write
def HotPointReads.Goal : Prop := hotPointReads write
def SeekCap.Goal : Prop := seekCap write
def ParallelEncode.Goal : Prop := parallelEncode write
def ConstantAmbiguity.Goal : Prop :=
  ∀ x n, 0<n → (write (List.replicate n 0)).length ≤ (write (List.replicate n x)).length
def MandatoryCuts.Goal : Prop := mandatoryCuts write
def BoundedRaw.Goal : Prop := boundedRaw write
def GreedyCut.Goal : Prop := greedyCut write

def ArithmeticKeys.Goal : Prop := ∀ d n, 0<n → (write ((List.range n).map (fun i => (i+1)*d))).length = ubytes (2*d)+1

end MinimumDelta

namespace SparsePlanes

/-- Per bitplane encode population and ULEB ordinals of all ones. -/
def write (xs : List Nat) : Bytes :=
  let w := width (peak xs)
  uleb w ++ (List.range w).flatMap (fun b =>
    let ps := xs.zipIdx.filter (fun (v,_) => v.testBit b)
    uleb ps.length ++ ps.flatMap (fun (_,i) => uleb i))

def ByteFrontier.Goal : Prop := byteFrontier write
def StrictSavings.Goal : Prop := strictSavings write
def HotPointReads.Goal : Prop := hotPointReads write
def SeekCap.Goal : Prop := seekCap write
def ParallelEncode.Goal : Prop := parallelEncode write
def ShiftedOutlier.Goal : Prop :=
  ∀ a b n, (write (List.replicate n 0++[2^a+2^(a+b+1)])).length = ubytes (a+b+2)+(a+b+2)+2*ubytes n
def MandatoryCuts.Goal : Prop := mandatoryCuts write
def BoundedRaw.Goal : Prop := boundedRaw write
def GreedyCut.Goal : Prop := greedyCut write

def OneHotOutlier.Goal : Prop := ∀ n b, (write (List.replicate n 0 ++ [2^b])).length = ubytes (b+1)+(b+1)+ubytes n

end SparsePlanes

namespace SharedDivisor

/-- Factor offsets by their common divisor before packing. -/
def write (xs : List Nat) : Bytes :=
  let g := xs.foldl Nat.gcd 0
  let vs := xs.map (·/g)
  let w := width (peak vs)
  uleb g ++ uleb w ++ pack w vs

def ByteFrontier.Goal : Prop := byteFrontier write
def StrictSavings.Goal : Prop := strictSavings write
def HotPointReads.Goal : Prop := hotPointReads write
def SeekCap.Goal : Prop := seekCap write
def ParallelEncode.Goal : Prop := parallelEncode write
def ScaleInvariance.Goal : Prop :=
  ∀ xs g, xs≠[] → xs.foldl Nat.gcd 0=1 → 0<g → (write (xs.map (·*g))).length+ubytes 1=(write xs).length+ubytes g
def MandatoryCuts.Goal : Prop := mandatoryCuts write
def BoundedRaw.Goal : Prop := boundedRaw write
def GreedyCut.Goal : Prop := greedyCut write

def ScaledBinary.Goal : Prop := ∀ g xs, 0<g → g∈xs → (∀ x∈xs, x=0 ∨ x=g) →
  (write xs).length = ubytes g+1+byteCount xs.length

end SharedDivisor

-- Provisional constant/exception frame: the locally cheaper first merge
-- prevents the globally cheaper split right after the first zero.
#guard let z := chosen ConstantExceptions.write
       z [0,1] == 10 && z [0]+z [1] == 12 &&
       z [0,1,1] == 14 && z [0,1]+z [1] == 16 && z [0]+z [1,1] == 13
#guard (SeparatedLengths.write [1,256,65536,16777216]).length == 11
#guard (OrdinalResidual.write (List.range 100)).length == 1

end TinylambOpt.ResearchGoals.StorageExecution.FrameSelection
