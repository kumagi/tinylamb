import Std

/-!
A vectorized execution path must respect active lanes, valid row positions, and the
modeled memory access bounds.
-/

namespace TinylambOpt.ResearchGoals.JitFrontiers.VectorMemory

/-! Affine and indirect lane footprints. Buffers are one flat array of words;
different logical base pointers may overlap. Source failure and destination
failure both abort the tentative vector transaction before publication. -/
structure Job where
  src : Nat
  dst : Nat
  stride : Nat
  window : Nat
  ids : List Nat
  mask : List Bool
structure Kernel where
  loads : Job → Nat → List Nat
  store : Job → Nat → Nat
  value : Nat → List Nat → Nat
def load (k : Kernel) (j : Job) (mem : List Nat) (i : Nat) :=
  ((k.loads j i).mapM (fun addr => mem[addr]?)).map (k.value i)
def put (mem : List Nat) (addr x : Nat) := if addr<mem.length then some (mem.set addr x) else none
def scalar (k : Kernel) (j : Job) (mem ids : List Nat) := ids.foldlM
  (fun m i => (load k j m i).bind (put m (k.store j i))) mem
def vector (k : Kernel) (j : Job) (mem ids : List Nat) := do
  let changes ← ids.mapM (fun i => (load k j mem i).map (k.store j i,·))
  changes.foldlM (fun m (addr,x) => put m addr x) mem
def rawFree (k : Kernel) (j : Job) : List Nat → Prop
  | [] => True
  | i::is => (∀ t∈is, k.store j i ∉ k.loads j t) ∧ rawFree k j is
def valid (k : Kernel) (j : Job) (len : Nat) (ids : List Nat) :=
  ∀ i∈ids, k.store j i<len ∧ ∀ a∈k.loads j i, a<len
def selected (j : Job) (ids : List Nat) := ids.filter (fun i => j.mask[i]?.getD false)
def batches (width : Nat) (ids : List Nat) :=
  (List.range ((ids.length+width-1)/width)).map (fun t => (ids.drop (t*width)).take width)
def addresses (k : Kernel) (j : Job) (ids : List Nat) := (ids.flatMap (k.loads j)).eraseDups

/-- A lane may not overwrite a later lane’s input before grouped loads finish. -/
def dependenceGuard (k : Kernel) : Prop := ∀ j mem ids, rawFree k j ids → vector k j mem ids=scalar k j mem ids

/-- Only intra-vector RAW dependencies require guarding across independently committed vectors. -/
def chunkLocal (k : Kernel) : Prop := ∀ j mem ids w, 0<w → (∀ b∈batches w ids, rawFree k j b) →
  (batches w ids).foldlM (vector k j) mem=scalar k j mem ids

/-- A masked partial vector only needs the active lanes to be addressable. -/
def tailExact (k : Kernel) : Prop := ∀ j mem ids, (vector k j mem (selected j ids)).isSome=true ↔ valid k j mem.length (selected j ids)

/-- Proposed full-vector speculation with no alias guard; overlaps can invalidate values. -/
def snapshotForward (k : Kernel) : Prop := ∀ j mem ids, valid k j mem.length ids → vector k j mem ids=scalar k j mem ids

/-- A vector scatter is last-writer-wins at duplicate addresses. -/
def deadStore (k : Kernel) : Prop := ∀ j mem ids out i, vector k j mem ids=some out → i∈ids →
  let last := (ids.filter (fun t => k.store j t == k.store j i)).getLast?.getD i
  out[k.store j i]?=load k j mem last

/-- Common loads may be cached once even for repeated ordinals. -/
def gatherCache (k : Kernel) : Prop := ∀ j ids (m n : List Nat), (∀ a∈addresses k j ids, m[a]?=n[a]?) →
  ids.map (load k j m)=ids.map (load k j n)

/-- Reordering independent stores in a snapshot vector preserves the committed buffer. -/
def unorderedScatter (k : Kernel) : Prop := ∀ j m a b, a.Perm b → (a.map (k.store j)).Nodup → vector k j m a=vector k j m b

/-- Aggressive tail widening: legality of selected lanes suffices to read all physical lanes. -/
def tailOverread (k : Kernel) : Prop := ∀ j n ids, valid k j n (selected j ids) →
  ∀ a∈addresses k j ids, a<n

namespace ForwardCopy

/-- Overlapping contiguous copy, scalar order forward. -/
def kernel : Kernel :=
  ⟨fun j i => [j.src+i], fun j i => j.dst+i, (fun _ => List.sum)⟩

def DependenceGuard.Goal : Prop := dependenceGuard kernel
def ChunkLocal.Goal : Prop := chunkLocal kernel
def TailExact.Goal : Prop := tailExact kernel
def SnapshotForward.Goal : Prop := snapshotForward kernel
def DeadStore.Goal : Prop := deadStore kernel
def GatherCache.Goal : Prop := gatherCache kernel
def UnorderedScatter.Goal : Prop := unorderedScatter kernel
def TailOverread.Goal : Prop := tailOverread kernel

def SafeDirection.Goal : Prop := ∀ j n, j.dst≤j.src → rawFree kernel j (List.range n)

def DisjointDistance.Goal : Prop := ∀ j n, j.src+n≤j.dst → rawFree kernel j (List.range n)

end ForwardCopy

namespace ReverseInput

/-- Reverse a block into an output span. -/
def kernel : Kernel :=
  ⟨fun j i => [j.src+(j.window-1-i)], fun j i => j.dst+i, (fun _ => List.sum)⟩

def DependenceGuard.Goal : Prop := dependenceGuard kernel
def ChunkLocal.Goal : Prop := chunkLocal kernel
def TailExact.Goal : Prop := tailExact kernel
def SnapshotForward.Goal : Prop := snapshotForward kernel
def DeadStore.Goal : Prop := deadStore kernel
def GatherCache.Goal : Prop := gatherCache kernel
def UnorderedScatter.Goal : Prop := unorderedScatter kernel
def TailOverread.Goal : Prop := tailOverread kernel

def Reflection.Goal : Prop := ∀ j i, i<j.window → kernel.loads j (j.window-1-i)=[j.src+i]

def InPlacePair.Goal : Prop := ∀ j, j.src=j.dst → j.window=2 → ¬ rawFree kernel j [0,1]

end ReverseInput

namespace StrideRead

/-- Gather every stride-th column element to dense output. -/
def kernel : Kernel :=
  ⟨fun j i => [j.src+i*j.stride], fun j i => j.dst+i, (fun _ => List.sum)⟩

def DependenceGuard.Goal : Prop := dependenceGuard kernel
def ChunkLocal.Goal : Prop := chunkLocal kernel
def TailExact.Goal : Prop := tailExact kernel
def SnapshotForward.Goal : Prop := snapshotForward kernel
def DeadStore.Goal : Prop := deadStore kernel
def GatherCache.Goal : Prop := gatherCache kernel
def UnorderedScatter.Goal : Prop := unorderedScatter kernel
def TailOverread.Goal : Prop := tailOverread kernel

def CompressInPlace.Goal : Prop := ∀ j n, j.dst≤j.src → 0<j.stride → rawFree kernel j (List.range n)

def BroadcastRead.Goal : Prop := ∀ j ids, j.stride=0 → ids≠[] → addresses kernel j ids=[j.src]

end StrideRead

namespace StrideStore

/-- Scatter consecutive input values to interleaved output. -/
def kernel : Kernel :=
  ⟨fun j i => [j.src+i], fun j i => j.dst+i*j.stride, (fun _ => List.sum)⟩

def DependenceGuard.Goal : Prop := dependenceGuard kernel
def ChunkLocal.Goal : Prop := chunkLocal kernel
def TailExact.Goal : Prop := tailExact kernel
def SnapshotForward.Goal : Prop := snapshotForward kernel
def DeadStore.Goal : Prop := deadStore kernel
def GatherCache.Goal : Prop := gatherCache kernel
def UnorderedScatter.Goal : Prop := unorderedScatter kernel
def TailOverread.Goal : Prop := tailOverread kernel

def DistinctScatter.Goal : Prop := ∀ j n, 0<j.stride → ((List.range n).map (kernel.store j)).Nodup

def RepeatedAddress.Goal : Prop := ∀ j i, j.stride=0 → kernel.store j i=j.dst

end StrideStore

namespace NeighborSum

/-- A two-tap forward stencil reads adjacent lanes. -/
def kernel : Kernel :=
  ⟨fun j i => [j.src+i,j.src+i+1], fun j i => j.dst+i, (fun _ => List.sum)⟩

def DependenceGuard.Goal : Prop := dependenceGuard kernel
def ChunkLocal.Goal : Prop := chunkLocal kernel
def TailExact.Goal : Prop := tailExact kernel
def SnapshotForward.Goal : Prop := snapshotForward kernel
def DeadStore.Goal : Prop := deadStore kernel
def GatherCache.Goal : Prop := gatherCache kernel
def UnorderedScatter.Goal : Prop := unorderedScatter kernel
def TailOverread.Goal : Prop := tailOverread kernel

def SharedHalo.Goal : Prop := ∀ j n, 0<n → (addresses kernel j (List.range n)).length=n+1

def InPlaceForward.Goal : Prop := ∀ j n, j.src=j.dst → rawFree kernel j (List.range n)

end NeighborSum

namespace BackwardDelta

/-- Read current and previous words; first lane clamps at zero. -/
def kernel : Kernel :=
  ⟨fun j i => [j.src+i,j.src+(i-1)], fun j i => j.dst+i, (fun _ => fun vs => (vs[0]?.getD 0) ^^^ (vs[1]?.getD 0))⟩

def DependenceGuard.Goal : Prop := dependenceGuard kernel
def ChunkLocal.Goal : Prop := chunkLocal kernel
def TailExact.Goal : Prop := tailExact kernel
def SnapshotForward.Goal : Prop := snapshotForward kernel
def DeadStore.Goal : Prop := deadStore kernel
def GatherCache.Goal : Prop := gatherCache kernel
def UnorderedScatter.Goal : Prop := unorderedScatter kernel
def TailOverread.Goal : Prop := tailOverread kernel

def ReverseSafe.Goal : Prop := ∀ j n, j.src=j.dst → rawFree kernel j (List.range n).reverse

def FirstZero.Goal : Prop := ∀ j mem, j.src<mem.length → load kernel j mem 0=some 0

end BackwardDelta

namespace IndexedGather

/-- Read dictionary slots via a local code vector. -/
def kernel : Kernel :=
  ⟨fun j i => [j.src+j.ids[i]?.getD 0], fun j i => j.dst+i, (fun _ => List.sum)⟩

def DependenceGuard.Goal : Prop := dependenceGuard kernel
def ChunkLocal.Goal : Prop := chunkLocal kernel
def TailExact.Goal : Prop := tailExact kernel
def SnapshotForward.Goal : Prop := snapshotForward kernel
def DeadStore.Goal : Prop := deadStore kernel
def GatherCache.Goal : Prop := gatherCache kernel
def UnorderedScatter.Goal : Prop := unorderedScatter kernel
def TailOverread.Goal : Prop := tailOverread kernel

def SameCodes.Goal : Prop := ∀ j mem i t, j.ids[i]?=j.ids[t]? → load kernel j mem i=load kernel j mem t

def UniqueReads.Goal : Prop := ∀ j n, n≤j.ids.length → (addresses kernel j (List.range n)).length=(j.ids.take n).eraseDups.length

end IndexedGather

namespace IndexedScatter

/-- Bins selected by possibly duplicate group IDs. -/
def kernel : Kernel :=
  ⟨fun j i => [j.src+i], fun j i => j.dst+j.ids[i]?.getD 0, (fun _ => List.sum)⟩

def DependenceGuard.Goal : Prop := dependenceGuard kernel
def ChunkLocal.Goal : Prop := chunkLocal kernel
def TailExact.Goal : Prop := tailExact kernel
def SnapshotForward.Goal : Prop := snapshotForward kernel
def DeadStore.Goal : Prop := deadStore kernel
def GatherCache.Goal : Prop := gatherCache kernel
def UnorderedScatter.Goal : Prop := unorderedScatter kernel
def TailOverread.Goal : Prop := tailOverread kernel

def StableCollisions.Goal : Prop := ∀ j i t, j.ids[i]?=j.ids[t]? → kernel.store j i=kernel.store j t

def AllDistinct.Goal : Prop := ∀ j n, n≤j.ids.length → (j.ids.take n).Nodup → ((List.range n).map (kernel.store j)).Nodup

end IndexedScatter

namespace RingWindow

/-- Wrap stencil positions around a finite ring. -/
def kernel : Kernel :=
  ⟨fun j i => [j.src+i%(j.window+1),j.src+(i+1)%(j.window+1)], fun j i => j.dst+i, (fun _ => List.sum)⟩

def DependenceGuard.Goal : Prop := dependenceGuard kernel
def ChunkLocal.Goal : Prop := chunkLocal kernel
def TailExact.Goal : Prop := tailExact kernel
def SnapshotForward.Goal : Prop := snapshotForward kernel
def DeadStore.Goal : Prop := deadStore kernel
def GatherCache.Goal : Prop := gatherCache kernel
def UnorderedScatter.Goal : Prop := unorderedScatter kernel
def TailOverread.Goal : Prop := tailOverread kernel

def PeriodicGather.Goal : Prop := ∀ j mem i, load kernel j mem (i+j.window+1)=load kernel j mem i

def RingFootprint.Goal : Prop := ∀ j ids, (addresses kernel j ids).length≤j.window+1

end RingWindow

namespace PackedBits

/-- Extract one validity bit per lane from a packed source word. -/
def kernel : Kernel :=
  ⟨fun j i => [j.src+i/64], fun j i => j.dst+i, fun i vs => ((vs.head?.getD 0).testBit (i%64)).toNat⟩

def DependenceGuard.Goal : Prop := dependenceGuard kernel
def ChunkLocal.Goal : Prop := chunkLocal kernel
def TailExact.Goal : Prop := tailExact kernel
def SnapshotForward.Goal : Prop := snapshotForward kernel
def DeadStore.Goal : Prop := deadStore kernel
def GatherCache.Goal : Prop := gatherCache kernel
def UnorderedScatter.Goal : Prop := unorderedScatter kernel
def TailOverread.Goal : Prop := tailOverread kernel

def WordCohort.Goal : Prop := ∀ j, (addresses kernel j (List.range 64)).length=1

def WordBoundary.Goal : Prop := ∀ j n, (addresses kernel j (List.range n)).length=(n+63)/64

end PackedBits

private def overlap : Job := ⟨0,1,1,2,[],[]⟩
#guard scalar ForwardCopy.kernel overlap [1,2,3] [0,1] == some [1,1,1]
#guard vector ForwardCopy.kernel overlap [1,2,3] [0,1] == some [1,1,2]

end TinylambOpt.ResearchGoals.JitFrontiers.VectorMemory
