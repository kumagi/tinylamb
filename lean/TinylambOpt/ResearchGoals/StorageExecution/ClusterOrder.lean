import TinylambOpt.ResearchGoals.Compression.Wire

namespace TinylambOpt.ResearchGoals.StorageExecution.ClusterOrder
open TinylambOpt.ResearchGoals.Compression.Wire

/-! A row group temporarily sorts physical rows to improve compression. Persist
the physical-to-logical permutation in packed bits, shared by both columns.
Key ties break on the original ordinal. Restoring order matters for LIMIT,
ordered groups, masks and late materialization through independent files. -/
abbrev Row := Nat × Nat
abbrev Key := Row → Row
def perm (key : Key) (rs : List Row) :=
  (rs.zipIdx.mergeSort (fun (x,i) (y,j) =>
    let a := key x; let b := key y
    a.1 < b.1 || (a.1==b.1 && (a.2 < b.2 || (a.2==b.2 && i≤j))))).map Prod.snd
def physical (key : Key) (rs : List Row) := (perm key rs).map (fun i => rs[i]?.getD (0,0))
def orderBytes (n : Nat) := byteCount (n*width (n-1))
def side (key : Key) (rs : List Row) := pack (width (rs.length-1)) (perm key rs)
def ordinal (key : Key) (rs : List Row) (i : Nat) := takeAt (side key rs) (width (rs.length-1)) i
def unravel (key : Key) (rs : List Row) (data : List Nat) :=
  ((List.range rs.length).map (fun i => (ordinal key rs i,data[i]?.getD 0))).mergeSort (fun a b => a.1≤b.1)
    |>.map Prod.snd
def rle (xs : List Nat) := xs.foldr (fun x (rest : List (Nat × Nat)) => match rest with
  | [] => [(x,1)]
  | (y,k)::ys => if x==y then (x,k+1)::ys else (x,1)::rest) []
def rleBytes (xs : List Nat) := ((rle xs).map (fun (v,k) => ubytes v + ubytes k)).sum
def both (rs : List Row) := rleBytes (rs.map Prod.fst)+rleBytes (rs.map Prod.snd)
def layoutBytes (key : Key) (rs : List Row) := 1 + (side key rs).length + both (physical key rs)
def morton (x y : Nat) :=
  ((List.range (max (width x) (width y))).map (fun i =>
    (if x.testBit i then 2^(2*i) else 0)+(if y.testBit i then 2^(2*i+1) else 0))).sum

/-- Restore the projected column through the serialized ordinal map. -/
def lateGather (key : Key) : Prop := ∀ rs (project : Row → Nat),
  unravel key rs ((physical key rs).map project) = rs.map project

/-- A filtered LIMIT in original order can be reconstructed from selected ordinals. -/
def stableLimit (key : Key) : Prop := ∀ rs (p : Row → Bool) k,
  let ids := ((physical key rs).zipIdx.filterMap (fun (r,i) => if p r then some (ordinal key rs i) else none)).mergeSort
  (ids.take k).map (fun j => rs[j]?.getD (0,0)) = (rs.filter p).take k

/-- One packed ordinal sidecar applies a logical delete set to both reordered columns. -/
def sharedDelete (key : Key) : Prop := ∀ rs (ds : List Nat),
  (((physical key rs).zipIdx.filter (fun (_,i) => !ds.contains (ordinal key rs i))).map Prod.fst).Perm
   ((rs.zipIdx.filter (fun (_,i) => !ds.contains i)).map Prod.fst)

/-- Gather an independently stored late payload through the same permutation. -/
def payloadAlignment (key : Key) : Prop := ∀ rs (payload : List Nat), rs.length=payload.length →
  ((physical key rs).zip ((perm key rs).map (fun i => payload[i]?.getD 0))).Perm (rs.zip payload)

/-- Decide whether RLE savings cover the materialized permutation and format tag. -/
def sidecarBudget (key : Key) : Prop := ∀ rs, (layoutBytes key rs < both rs ↔
  both (physical key rs)+orderBytes rs.length+1 < both rs)

/-- Apply an update to the old physical layout via the inverse permutation before reclustering. -/
def patchByOrdinal (key : Key) : Prop := ∀ rs i v, i<rs.length →
  let ids := perm key rs
  (physical key rs).set (ids.idxOf i) v = ids.map (fun j => (rs.set i v)[j]?.getD (0,0))

/-- A stable merge of independently clustered shards recovers the global physical order. -/
def mergeShards (key : Key) : Prop := ∀ a b,
  List.merge (physical key a) (physical key b) (fun x y => let a := key x; let b := key y; a.1<b.1 || (a.1==b.1 && a.2≤b.2)) = physical key (a++b)

/-- Aggressive hypothesis: the combined RLE payload always compensates for the permutation on nonconstant pages. -/
def twoColumnWin (key : Key) : Prop := ∀ rs, 1<rs.eraseDups.length → layoutBytes key rs ≤ both rs

namespace Lexicographic

/-- Primary dimension followed by its dependent attribute. -/
def key : Key := id

def StreamingFrequency.Goal : Prop :=
  ∀ rs v, let col := (physical key rs).map Prod.fst; (col.dropWhile (· < v) |>.takeWhile (· == v)).length = (rs.map Prod.fst).count v
def LateGather.Goal : Prop := lateGather key
def StableLimit.Goal : Prop := stableLimit key
def SharedDelete.Goal : Prop := sharedDelete key
def PayloadAlignment.Goal : Prop := payloadAlignment key
def SidecarBudget.Goal : Prop := sidecarBudget key
def PatchByOrdinal.Goal : Prop := patchByOrdinal key
def MergeShards.Goal : Prop := mergeShards key
def TwoColumnWin.Goal : Prop := twoColumnWin key

def PrimaryRuns.Goal : Prop := ∀ rs, (rle ((physical key rs).map Prod.fst)).length = (rs.map Prod.fst).eraseDups.length

end Lexicographic

namespace AttributeFirst

/-- Group right-hand payload values first. -/
def key : Key := fun (x,y) => (y,x)

def AttributeCut.Goal : Prop :=
  ∀ rs v, let ps:=physical key rs; (ps.takeWhile (fun p => p.2<v)).length=rs.countP (fun p => p.2<v)
def LateGather.Goal : Prop := lateGather key
def StableLimit.Goal : Prop := stableLimit key
def SharedDelete.Goal : Prop := sharedDelete key
def PayloadAlignment.Goal : Prop := payloadAlignment key
def SidecarBudget.Goal : Prop := sidecarBudget key
def PatchByOrdinal.Goal : Prop := patchByOrdinal key
def MergeShards.Goal : Prop := mergeShards key
def TwoColumnWin.Goal : Prop := twoColumnWin key

def AttributeRuns.Goal : Prop := ∀ rs, (rle ((physical key rs).map Prod.snd)).length = (rs.map Prod.snd).eraseDups.length

end AttributeFirst

namespace Interleaved

/-- Morton-order clustering of two dimensions. -/
def key : Key := fun (x,y) => (morton x y,0)

def SquareTile.Goal : Prop :=
  ∀ h x y bx by_, ((key (x,y)).1/4^h = morton bx by_ ↔ x/2^h=bx ∧ y/2^h=by_)
def LateGather.Goal : Prop := lateGather key
def StableLimit.Goal : Prop := stableLimit key
def SharedDelete.Goal : Prop := sharedDelete key
def PayloadAlignment.Goal : Prop := payloadAlignment key
def SidecarBudget.Goal : Prop := sidecarBudget key
def PatchByOrdinal.Goal : Prop := patchByOrdinal key
def MergeShards.Goal : Prop := mergeShards key
def TwoColumnWin.Goal : Prop := twoColumnWin key

def QuadrantPrefix.Goal : Prop := ∀ x y h, (key (x,y)).1/4^h = morton (x/2^h) (y/2^h)

end Interleaved

namespace GrayPair

/-- Gray-code ordering favors local xor deltas in the leading dimension. -/
def key : Key := fun (x,y) => (x ^^^ (x/2),y)

def NeighborMask.Goal : Prop :=
  ∀ rs x y, let s := physical key rs; ((s.map key).count (key (x,y))) = rs.count (x,y)
def LateGather.Goal : Prop := lateGather key
def StableLimit.Goal : Prop := stableLimit key
def SharedDelete.Goal : Prop := sharedDelete key
def PayloadAlignment.Goal : Prop := payloadAlignment key
def SidecarBudget.Goal : Prop := sidecarBudget key
def PatchByOrdinal.Goal : Prop := patchByOrdinal key
def MergeShards.Goal : Prop := mergeShards key
def TwoColumnWin.Goal : Prop := twoColumnWin key

def AdjacentGray.Goal : Prop := ∀ x y, ∃ b, (key (x,y)).1 ^^^ (key (x+1,y)).1 = 2^b

end GrayPair

namespace Diagonal

/-- Traverse diagonals of equal composite totals. -/
def key : Key := fun (x,y) => (x+y,x)

def TriangularPrefix.Goal : Prop :=
  ∀ rs t, ((physical key rs).takeWhile (fun (x,y) => x+y<t)).length=rs.countP (fun (x,y) => x+y<t)
def LateGather.Goal : Prop := lateGather key
def StableLimit.Goal : Prop := stableLimit key
def SharedDelete.Goal : Prop := sharedDelete key
def PayloadAlignment.Goal : Prop := payloadAlignment key
def SidecarBudget.Goal : Prop := sidecarBudget key
def PatchByOrdinal.Goal : Prop := patchByOrdinal key
def MergeShards.Goal : Prop := mergeShards key
def TwoColumnWin.Goal : Prop := twoColumnWin key

def EqualSumRuns.Goal : Prop := ∀ rs, (rle ((physical key rs).map (fun (x,y) => x+y))).length = (rs.map (fun (x,y) => x+y)).eraseDups.length

end Diagonal

namespace UnorderedPair

/-- Expose symmetric key pairs for undirected graph joins. -/
def key : Key := fun (x,y) => (min x y,max x y)

def SymmetricMultiplicity.Goal : Prop :=
  ∀ rs a b, a<b → ((physical key rs).map key).count (a,b) = rs.count (a,b) + rs.count (b,a)
def LateGather.Goal : Prop := lateGather key
def StableLimit.Goal : Prop := stableLimit key
def SharedDelete.Goal : Prop := sharedDelete key
def PayloadAlignment.Goal : Prop := payloadAlignment key
def SidecarBudget.Goal : Prop := sidecarBudget key
def PatchByOrdinal.Goal : Prop := patchByOrdinal key
def MergeShards.Goal : Prop := mergeShards key
def TwoColumnWin.Goal : Prop := twoColumnWin key

def OrientationTies.Goal : Prop := ∀ x y, key (x,y) = key (y,x)

end UnorderedPair

namespace RatioClass

/-- Cluster by quotient and remainder; distinct denominators can share a key. -/
def key : Key := fun (x,y) => (x/(y+1),x%(y+1))

def EquivalentDenominators.Goal : Prop :=
  ∀ x y z, x≤y → x≤z → key (x,y)=key (x,z)
def LateGather.Goal : Prop := lateGather key
def StableLimit.Goal : Prop := stableLimit key
def SharedDelete.Goal : Prop := sharedDelete key
def PayloadAlignment.Goal : Prop := payloadAlignment key
def SidecarBudget.Goal : Prop := sidecarBudget key
def PatchByOrdinal.Goal : Prop := patchByOrdinal key
def MergeShards.Goal : Prop := mergeShards key
def TwoColumnWin.Goal : Prop := twoColumnWin key

def Reconstruction.Goal : Prop := ∀ x y, (key (x,y)).1*(y+1)+(key (x,y)).2=x

end RatioClass

namespace PowerBucket

/-- Group integer widths before secondary payload for frame packing. -/
def key : Key := fun (x,y) => (width x,y)

def DenseBitBounds.Goal : Prop :=
  ∀ rs w, (((physical key rs).filter (fun p => (key p).1==w)).map Prod.fst).all (fun x => x<2^w)=true
def LateGather.Goal : Prop := lateGather key
def StableLimit.Goal : Prop := stableLimit key
def SharedDelete.Goal : Prop := sharedDelete key
def PayloadAlignment.Goal : Prop := payloadAlignment key
def SidecarBudget.Goal : Prop := sidecarBudget key
def PatchByOrdinal.Goal : Prop := patchByOrdinal key
def MergeShards.Goal : Prop := mergeShards key
def TwoColumnWin.Goal : Prop := twoColumnWin key

def WidthRunCount.Goal : Prop := ∀ rs, (rle ((physical key rs).map (fun p => width p.1))).length = (rs.map (fun (p : Row) => width p.1)).eraseDups.length

end PowerBucket

namespace DyadicTile

/-- Keep coarse 16×16 tiles contiguous and order inside each tile by low nibbles. -/
def key : Key := fun (x,y) => (morton (x/16) (y/16),16*(x%16)+y%16)

def TileBijection.Goal : Prop :=
  ∀ x y a b, key (x,y)=key (a,b) ↔ x=a ∧ y=b
def LateGather.Goal : Prop := lateGather key
def StableLimit.Goal : Prop := stableLimit key
def SharedDelete.Goal : Prop := sharedDelete key
def PayloadAlignment.Goal : Prop := payloadAlignment key
def SidecarBudget.Goal : Prop := sidecarBudget key
def PatchByOrdinal.Goal : Prop := patchByOrdinal key
def MergeShards.Goal : Prop := mergeShards key
def TwoColumnWin.Goal : Prop := twoColumnWin key

def TileDecode.Goal : Prop := ∀ x y, (key (x,y)).2/16 = x%16 ∧ (key (x,y)).2%16 = y%16

end DyadicTile

namespace XorClass

/-- Co-locate pairs sharing the same bitwise difference. -/
def key : Key := fun (x,y) => (x ^^^ y,x)

def ZeroClass.Goal : Prop :=
  ∀ rs, ((physical key rs).takeWhile (fun p => (key p).1==0)).length = rs.countP (fun (x,y) => x==y)
def LateGather.Goal : Prop := lateGather key
def StableLimit.Goal : Prop := stableLimit key
def SharedDelete.Goal : Prop := sharedDelete key
def PayloadAlignment.Goal : Prop := payloadAlignment key
def SidecarBudget.Goal : Prop := sidecarBudget key
def PatchByOrdinal.Goal : Prop := patchByOrdinal key
def MergeShards.Goal : Prop := mergeShards key
def TwoColumnWin.Goal : Prop := twoColumnWin key

def DecodeRight.Goal : Prop := ∀ x y, (key (x,y)).1 ^^^ (key (x,y)).2 = y

end XorClass

#guard perm Diagonal.key [(2,1),(1,2),(1,2)] == [1,2,0]
#guard layoutBytes Lexicographic.key [(0,0),(1,1)] > both [(0,0),(1,1)]

end TinylambOpt.ResearchGoals.StorageExecution.ClusterOrder
