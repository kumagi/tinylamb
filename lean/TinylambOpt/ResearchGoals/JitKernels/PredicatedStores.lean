import Std

/-! Vectorization across potentially aliased output buffers. Compare store forwarding, masked reads and dead-store elimination at byte precision, including trace observations between stores. -/
namespace TinylambOpt.ResearchGoals.JitKernels.PredicatedStores

abbrev Mem := Nat → UInt8
def store (m : Mem) (base : Nat) (bs : List UInt8) : Mem := fun i =>
  if base ≤ i then bs[i-base]?.getD (m i) else m i
def read (m : Mem) (base n : Nat) := (List.range n).map (fun i => m (base+i))
def masked (m : Mem) (base : Nat) (bs : List (Option UInt8)) : Mem := fun i =>
  if base ≤ i then bs[i-base]?.join.getD (m i) else m i

/-- Forward a subword directly from a wider preceding store without issuing another load. -/
def ForwardSubwordLoad.Goal : Prop := ∀ m base (bs : List UInt8) off n, off+n ≤ bs.length →
  read (store m base bs) (base+off) n = (bs.drop off).take n

/-- Reorder differently sized generated stores when their byte intervals are disjoint. -/
def StorePairDisjointReorder.Goal : Prop := ∀ m a b (xs ys : List UInt8), a+xs.length ≤ b ∨ b+ys.length ≤ a →
  store (store m a xs) b ys = store (store m b ys) a xs

/-- Use distinct, four-byte-aligned base addresses as a no-alias check for stores of up to eight bytes. -/
def AddressInequalityIsNoAlias.Goal : Prop := ∀ m a b (xs ys : List UInt8), a ≠ b → a%4 = 0 → b%4 = 0 → xs.length ≤ 8 → ys.length ≤ 8 → store (store m a xs) b ys = store (store m b ys) a xs

/-- Discard a store covered by the next store even when a deopt snapshot reads the intervening byte range. -/
def OverwrittenStoreAcrossObservation.Goal : Prop := ∀ m a (xs ys : List UInt8), xs.length ≤ ys.length →
  read (store m a xs) a xs.length = read m a xs.length

/-- Pack adjacent stores in address order without changing bytes outside either destination. -/
def MergeAdjacentNarrowStores.Goal : Prop := ∀ m a xs ys, store (store m a xs) (a+xs.length) ys = store m a (xs++ys)

/-- Merge a pair of same-base partial stores by choosing the last active writer per lane. -/
def LastWriterMaskedMerge.Goal : Prop := ∀ m a (xs ys : List (Option UInt8)), xs.length = ys.length →
  masked (masked m a xs) a ys = masked m a (xs.zipWith (fun x y => y.orElse (fun _ => x)) ys)

/-- Replace a masked store by an unconditional zero-filled vector store if the next reader checks validity. -/
def ZeroFillInactiveOutput.Goal : Prop := ∀ m a (xs : List (Option UInt8)) i, (xs[i]?.join).isSome →
  masked m a xs (a+i) = store m a (xs.map (·.getD 0)) (a+i)

/-- A zero-filled store also preserves bytes of a live aliased column when that store's own validity lane is false. -/
def SharedNullByteClobber.Goal : Prop := ∀ m a (xs : List (Option UInt8)) i, xs[i]? = some none →
  store m a (xs.map (·.getD 0)) (a+i) = m (a+i)

/-- Assemble a wide load from two adjacent readable ranges instead of speculating across a page edge. -/
def SplitLoadPageCrossing.Goal : Prop := ∀ m a n k, k ≤ n → read m a k ++ read m (a+k) (n-k) = read m a n

/-- A load can be hoisted across a masked store when every actually overlapping store lane is inactive. -/
def ReusePreStoreLoadThroughAlias.Goal : Prop := ∀ m a b xs n, (∀ i < n, a ≤ b+i → xs[b+i-a]?.join = none) →
  read (masked m a xs) b n = read m b n

end TinylambOpt.ResearchGoals.JitKernels.PredicatedStores
