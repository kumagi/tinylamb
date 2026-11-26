import Std

namespace TinylambOpt.ResearchGoals.JitFrontiers.PackedKeys

/-! A word-key specialization returns none to route a row to the general
hash table. A present all-ones bit pattern is an ordinary key. Equality of
nullable keys below is GROUP BY / null-safe equality; ordinary equijoins
require an explicit non-null test. -/
abbrev Key := Option Int × Option Int
abbrev Fields := Nat × Nat
def unsigned (x : Int) : Option Nat := if x<0 then none else some x.toNat
def biased (w : Nat) (x : Int) := unsigned (x+2^(w-1))
def unbias (w n : Nat) : Int := (n:Int)-2^(w-1)
def signedRaw (w : Nat) (x : Int) : Option Nat :=
  if -(2^(w-1):Int) ≤ x && x<2^(w-1) then some (BitVec.ofInt w x).toNat else none
def pick (k : Key) (f : Int → Int → Option Fields) := do f (← k.1) (← k.2)
def pair (x y : Int) : Key := (some x,some y)
def zig (x : Int) := if x<0 then 2*x.natAbs-1 else 2*x.toNat
def unzig (n : Nat) : Int := if n%2==0 then (n/2:Nat) else -((n/2:Nat):Int)-1
structure Codec where
  lowBits : Nat
  fields : Key → Option Fields
  back : Fields → Key
def encode (c : Codec) (k : Key) : Option (BitVec 64) := do
  let (a,b) ← c.fields k
  if a<2^(64-c.lowBits) && b<2^c.lowBits then some (BitVec.ofNat 64 (a*2^c.lowBits+b)) else none
def decode (c : Codec) (v : BitVec 64) := c.back (v.toNat/2^c.lowBits,v.toNat%2^c.lowBits)
def fast (c : Codec) (xs : List Key) := xs.zipIdx.filterMap (fun (k,i) => (encode c k).map (i,·))
def cold (c : Codec) (xs : List Key) := xs.zipIdx.filter (fun (k,_) => (encode c k).isNone)
def hits (c : Codec) (a b : Key) := match encode c a,encode c b with
  | some x,some y => x==y
  | _,_ => a==b
def lex (a b : Key) := compare a.1 b.1 == .lt || (a.1==b.1 && compare a.2 b.2 == .lt)

/-- Recover the complete composite key on every admitted row. -/
def recover (c : Codec) : Prop := ∀ k v, encode c k=some v → decode c v=k

/-- An admitted 64-bit collision implies null-safe equality of both components. -/
def collisionFree (c : Codec) : Prop := ∀ a b v, encode c a=some v → encode c b=some v → a=b

/-- Check both field bounds before truncating to the packed word. -/
def guardRange (c : Codec) : Prop := ∀ k, (encode c k).isSome=true ↔
  ∃ a b, c.fields k=some (a,b) ∧ a<2^(64-c.lowBits) ∧ b<2^c.lowBits

/-- Stable merge by input ordinal restores the original selection including duplicate row IDs. -/
def fallbackBag (c : Codec) : Prop := ∀ xs, let hot := (fast c xs).map (fun (i,v) => (i,decode c v))
  let slow := (cold c xs).map (fun (k,i) => (i,k))
  (((hot++slow).mergeSort (fun a b => a.1≤b.1)).map Prod.snd)=xs

/-- Use only packed keys to build DISTINCT for admitted rows, decoding representatives once. -/
def distinctGroups (c : Codec) : Prop := ∀ (xs : List Key), (((xs.filterMap (encode c)).eraseDups).map (decode c)) =
  (xs.filter (fun k => (encode c k).isSome)).eraseDups

/-- Use one native word comparison without changing duplicate-preserving join fanout. -/
def joinFanout (c : Codec) : Prop := ∀ (left right : List Key), (left.map (fun k => right.countP (hits c k))).sum =
  (left.map (fun k => right.count k)).sum

/-- Ordinary equality must test both left components before reusing the group comparison kernel. -/
def sqlNullGate (c : Codec) : Prop := ∀ a b, (a.1.isSome && a.2.isSome && hits c a b) =
  (match a,b with | (some x,some y),(some u,some v) => x==u && y==v | _,_ => false)

/-- Strong proposal: the unsigned packed comparison also implements SQL lexicographic order. -/
def reuseForSort (c : Codec) : Prop := ∀ a b x y, encode c a=some x → encode c b=some y → decide (x<y)=lex a b

namespace SignedHalves

/-- Raw int32 halves as used in the radix JIT. -/
def codec : Codec where
  lowBits := 32
  fields := fun k => pick k fun x y => do return (← signedRaw 32 x, ← signedRaw 32 y)
  back := fun (a,b) => pair (BitVec.ofNat 32 a).toInt (BitVec.ofNat 32 b).toInt

def Recover.Goal : Prop := recover codec
def CollisionFree.Goal : Prop := collisionFree codec
def GuardRange.Goal : Prop := guardRange codec
def FallbackBag.Goal : Prop := fallbackBag codec
def DistinctGroups.Goal : Prop := distinctGroups codec
def JoinFanout.Goal : Prop := joinFanout codec
def SqlNullGate.Goal : Prop := sqlNullGate codec
def ReuseForSort.Goal : Prop := reuseForSort codec

def AllOnesKey.Goal : Prop := encode codec (pair (-1) (-1))=some (-1)

def UpperCut.Goal : Prop := ∀ y, encode codec (pair (2^31) y)=none

end SignedHalves

namespace OrderBiased

/-- Bias signed keys before packing so the unsigned order is useful. -/
def codec : Codec where
  lowBits := 32
  fields := fun k => pick k fun x y => do return (← biased 32 x, ← biased 32 y)
  back := fun (a,b) => pair (unbias 32 a) (unbias 32 b)

def Recover.Goal : Prop := recover codec
def CollisionFree.Goal : Prop := collisionFree codec
def GuardRange.Goal : Prop := guardRange codec
def FallbackBag.Goal : Prop := fallbackBag codec
def DistinctGroups.Goal : Prop := distinctGroups codec
def JoinFanout.Goal : Prop := joinFanout codec
def SqlNullGate.Goal : Prop := sqlNullGate codec
def ReuseForSort.Goal : Prop := reuseForSort codec

def ZeroCorner.Goal : Prop := encode codec (pair (-(2^31)) (-(2^31)))=some 0

def SignCross.Goal : Prop := ∀ y u v, encode codec (pair (-1) y)=some u → encode codec (pair 0 y)=some v → u<v

end OrderBiased

namespace MixedDirection

/-- Ascending signed primary with descending secondary. -/
def codec : Codec where
  lowBits := 32
  fields := fun k => pick k fun x y => do return (← biased 32 x, ← biased 32 (-y-1))
  back := fun (a,b) => pair (unbias 32 a) (-unbias 32 b-1)

def Recover.Goal : Prop := recover codec
def CollisionFree.Goal : Prop := collisionFree codec
def GuardRange.Goal : Prop := guardRange codec
def FallbackBag.Goal : Prop := fallbackBag codec
def DistinctGroups.Goal : Prop := distinctGroups codec
def JoinFanout.Goal : Prop := joinFanout codec
def SqlNullGate.Goal : Prop := sqlNullGate codec
def ReuseForSort.Goal : Prop := reuseForSort codec

def ReverseTie.Goal : Prop := ∀ x a b p q, a<b → encode codec (pair x a)=some p → encode codec (pair x b)=some q → q<p

def SymmetricEndpoints.Goal : Prop := encode codec (pair (-(2^31)) (2^31-1))=some 0

end MixedDirection

namespace CorrelatedDelta

/-- Store a 48-bit base and a signed 16-bit intercolumn delta. -/
def codec : Codec where
  lowBits := 16
  fields := fun k => pick k fun x y => do return (← unsigned x, ← biased 16 (y-x))
  back := fun (a,b) => pair a ((a:Int)+unbias 16 b)

def Recover.Goal : Prop := recover codec
def CollisionFree.Goal : Prop := collisionFree codec
def GuardRange.Goal : Prop := guardRange codec
def FallbackBag.Goal : Prop := fallbackBag codec
def DistinctGroups.Goal : Prop := distinctGroups codec
def JoinFanout.Goal : Prop := joinFanout codec
def SqlNullGate.Goal : Prop := sqlNullGate codec
def ReuseForSort.Goal : Prop := reuseForSort codec

def FarApart.Goal : Prop := ∀ x, encode codec (pair x (x+2^15))=none

def WideSecond.Goal : Prop := (encode codec (pair (2^48-1) (2^48))).isSome=true

end CorrelatedDelta

namespace TaggedNull

/-- Reserve zero in the low field for a NULL secondary key. -/
def codec : Codec where
  lowBits := 32
  fields := fun k => do let x ← k.1; let a ← unsigned x; let b ← (match k.2 with | none => some 0 | some y => (unsigned y).map (·+1)); return (a,b)
  back := fun (a,b) => (some (a:Int),if b==0 then none else some ((b-1:Nat):Int))

def Recover.Goal : Prop := recover codec
def CollisionFree.Goal : Prop := collisionFree codec
def GuardRange.Goal : Prop := guardRange codec
def FallbackBag.Goal : Prop := fallbackBag codec
def DistinctGroups.Goal : Prop := distinctGroups codec
def JoinFanout.Goal : Prop := joinFanout codec
def SqlNullGate.Goal : Prop := sqlNullGate codec
def ReuseForSort.Goal : Prop := reuseForSort codec

def NullSlot.Goal : Prop := encode codec (some 0,none)=some 0

def SentinelCliff.Goal : Prop := encode codec (pair 0 (2^32-1))=none

end TaggedNull

namespace ZigzagPair

/-- Center tight domains around zero while keeping both signs. -/
def codec : Codec where
  lowBits := 32
  fields := fun k => pick k fun x y => some (zig x,zig y)
  back := fun (a,b) => pair (unzig a) (unzig b)

def Recover.Goal : Prop := recover codec
def CollisionFree.Goal : Prop := collisionFree codec
def GuardRange.Goal : Prop := guardRange codec
def FallbackBag.Goal : Prop := fallbackBag codec
def DistinctGroups.Goal : Prop := distinctGroups codec
def JoinFanout.Goal : Prop := joinFanout codec
def SqlNullGate.Goal : Prop := sqlNullGate codec
def ReuseForSort.Goal : Prop := reuseForSort codec

def NearbyNegative.Goal : Prop := encode codec (pair 0 (-1))=some 1

def FoldBoundary.Goal : Prop := ∀ x, zig x < 2^32 ↔ -(2^31:Int)≤x ∧ x<2^31

end ZigzagPair

namespace XorSide

/-- Store a narrow XOR residual around the first 40-bit column. -/
def codec : Codec where
  lowBits := 24
  fields := fun k => pick k fun x y => do let a ← unsigned x; let b ← unsigned y; return (a,a ^^^ b)
  back := fun (a,b) => pair a (a ^^^ b)

def Recover.Goal : Prop := recover codec
def CollisionFree.Goal : Prop := collisionFree codec
def GuardRange.Goal : Prop := guardRange codec
def FallbackBag.Goal : Prop := fallbackBag codec
def DistinctGroups.Goal : Prop := distinctGroups codec
def JoinFanout.Goal : Prop := joinFanout codec
def SqlNullGate.Goal : Prop := sqlNullGate codec
def ReuseForSort.Goal : Prop := reuseForSort codec

def SameHigh.Goal : Prop := ∀ x y, (encode codec (pair (x:Nat) (y:Nat))).isSome=true ↔ x<2^40 ∧ x/2^24=y/2^24

def EqualKeys.Goal : Prop := ∀ x, x<2^40 → encode codec (pair (x:Nat) x)=some (BitVec.ofNat 64 (x*2^24))

end XorSide

namespace DeltaMagnitude

/-- Store small signed distances via zigzag, reserving 12 low bits. -/
def codec : Codec where
  lowBits := 12
  fields := fun k => pick k fun x y => do return (← unsigned x,zig (y-x))
  back := fun (a,b) => pair a ((a:Int)+unzig b)

def Recover.Goal : Prop := recover codec
def CollisionFree.Goal : Prop := collisionFree codec
def GuardRange.Goal : Prop := guardRange codec
def FallbackBag.Goal : Prop := fallbackBag codec
def DistinctGroups.Goal : Prop := distinctGroups codec
def JoinFanout.Goal : Prop := joinFanout codec
def SqlNullGate.Goal : Prop := sqlNullGate codec
def ReuseForSort.Goal : Prop := reuseForSort codec

def AdjacentNegative.Goal : Prop := ∀ x, 0≤x → x<2^52 → encode codec (pair x (x-1))=some (BitVec.ofNat 64 (x.toNat*4096+1))

def RangeAsymmetry.Goal : Prop := (encode codec (pair 0 (-2048))).isSome=true ∧ encode codec (pair 0 2048)=none

end DeltaMagnitude

namespace ScaledKey

/-- Factor a known unit of 100 out of the left key after an exact divisibility check. -/
def codec : Codec where
  lowBits := 32
  fields := fun k => pick k fun x y => if x%100==0 then do return (← unsigned (x/100), ← unsigned y) else none
  back := fun (a,b) => pair (100*(a:Int)) b

def Recover.Goal : Prop := recover codec
def CollisionFree.Goal : Prop := collisionFree codec
def GuardRange.Goal : Prop := guardRange codec
def FallbackBag.Goal : Prop := fallbackBag codec
def DistinctGroups.Goal : Prop := distinctGroups codec
def JoinFanout.Goal : Prop := joinFanout codec
def SqlNullGate.Goal : Prop := sqlNullGate codec
def ReuseForSort.Goal : Prop := reuseForSort codec

def RejectRemainder.Goal : Prop := ∀ x y, x%100≠0 → encode codec (pair x y)=none

def Stride.Goal : Prop := ∀ k, k<2^32 → encode codec (pair (100*(k:Nat)) 0)=some (BitVec.ofNat 64 (k*2^32))

end ScaledKey

namespace ShardLocal

/-- Use the left shard and right local ordinal only if both rows belong to the same shard. -/
def codec : Codec where
  lowBits := 32
  fields := fun k => pick k fun x y => do let u ← unsigned x; let v ← unsigned y; if u/65536==v/65536 then some (u,v%65536) else none
  back := fun (a,b) => pair a ((a/65536)*65536+b)

def Recover.Goal : Prop := recover codec
def CollisionFree.Goal : Prop := collisionFree codec
def GuardRange.Goal : Prop := guardRange codec
def FallbackBag.Goal : Prop := fallbackBag codec
def DistinctGroups.Goal : Prop := distinctGroups codec
def JoinFanout.Goal : Prop := joinFanout codec
def SqlNullGate.Goal : Prop := sqlNullGate codec
def ReuseForSort.Goal : Prop := reuseForSort codec

def WrongShard.Goal : Prop := ∀ x y, x/65536≠y/65536 → encode codec (pair (x:Nat) (y:Nat))=none

def LastLocal.Goal : Prop := (encode codec (pair 65536 131071)).isSome=true

end ShardLocal

#guard ((encode SignedHalves.codec (pair (-1) (-1))).map BitVec.toNat) == some (2^64-1)
#guard encode TaggedNull.codec (some 0,none) != encode TaggedNull.codec (pair 0 0)

end TinylambOpt.ResearchGoals.JitFrontiers.PackedKeys
