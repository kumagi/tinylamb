import Std

namespace TinylambOpt.ResearchGoals.VersionedIndexes

/-! Immutable, committed versions with a total commit sequence per key. A tombstone
is distinct from "no candidate in this run". All observations use a fixed snapshot. -/
structure Entry where
  key : Nat
  seq : Nat
  value : Option Int
  deriving BEq, DecidableEq, Inhabited

def ordered (xs : List Entry) := xs.Pairwise (fun a b => b.seq ≤ a.seq)
def uniqueSeq (xs : List Entry) := (xs.map (fun e => (e.key,e.seq))).Nodup

def visible (xs : List Entry) (k t : Nat) :=
  xs.find? fun e => e.key == k && e.seq ≤ t

def valueAt (xs : List Entry) (k t : Nat) := (visible xs k t).bind Entry.value

/-- Beyond the oldest pinned sequence, keep all newer versions plus the first older
anchor per key, including its tombstone. -/
def compact (xs : List Entry) (floor_ : Nat) : List Entry :=
  (xs.foldl (fun (seen,out) e =>
    if floor_ < e.seq then (seen,e::out)
    else if seen.contains e.key then (seen,out)
    else (e.key::seen,e::out)) ([], [])).2.reverse

def SnapshotAnchor.Goal : Prop := ∀ xs oldest k t, ordered xs → oldest ≤ t →
  visible (compact xs oldest) k t = visible xs k t

/-- For a finite pin set, retain exactly the winners at those snapshots plus every
version newer than the latest pin. An older anchor also covers the gap after that pin. -/
def keepPinned (xs : List Entry) (pins : List Nat) (latest : Nat) := xs.filter fun e =>
  latest < e.seq || (latest::pins).any fun t => visible xs e.key t == some e

def SparsePins.Goal : Prop := ∀ xs pins latest k t, ordered xs →
  (t ∈ pins ∨ latest ≤ t) → visible (keepPinned xs pins latest) k t = visible xs k t

/-- A range-delete marker [lo,hi) wins against an older point value. Query active
range markers independently of point runs. Sequence ties favor deletes. -/
structure Marker where
  lo : Nat
  hi : Nat
  seq : Nat

def covers (m : Marker) (k t : Nat) := m.lo ≤ k && k < m.hi && m.seq ≤ t

def applyMarkers (xs : List Entry) (ms : List Marker) (k : Nat) :=
  (xs ++ ((ms.filter fun m => covers m k m.seq).map fun m => Entry.mk k m.seq none)).mergeSort
    (fun a b => a.seq > b.seq || (a.seq == b.seq && (a.value.isNone || b.value.isSome)))

def RangeTombstone.Goal : Prop := ∀ (xs : List Entry) ms k t, ordered xs → uniqueSeq xs →
  valueAt (applyMarkers xs ms k) k t =
    ((visible xs k t).bind fun e =>
      if (ms.filter (fun m => covers m k t)).any (fun m => e.seq ≤ m.seq) then none else e.value)

/-- A value predicate can be pushed to an index with a newer-version shadow check.
Filtering out tombstones and nonmatching versions before deduplication alone is unsafe. -/
def CoveredPredicate.Goal : Prop := ∀ (xs : List Entry) (p : Int → Bool) k t,
  ordered xs → uniqueSeq xs →
  ((valueAt xs k t).any p) =
    xs.any fun e => e.key == k && e.seq ≤ t && e.value.any p &&
      !(xs.any fun f => f.key == k && e.seq < f.seq && f.seq ≤ t)

/-- Coalesce repeated probe keys at one snapshot, then scatter results back to caller order. -/
def probeBatch (xs : List Entry) (ks : List Nat) (t : Nat) :=
  let cache := ks.eraseDups.foldl (fun (m : Std.HashMap Nat (Option Int)) k =>
    m.insert k (valueAt xs k t)) {}
  ks.map fun k => cache.getD k none

def BatchedLookup.Goal : Prop := ∀ xs ks t,
  probeBatch xs ks t = ks.map (fun k => valueAt xs k t)

/-- Skip an entire older run when its certified maximum sequence cannot beat an already
visible winner. Comparing payloads here loses deletions. -/
def newest (xs : List Entry) := xs.mergeSort (fun a b => b.seq ≤ a.seq)

def DominatedRun.Goal : Prop := ∀ (a b : List Entry) k t e,
  visible (newest a) k t = some e → (∀ f ∈ b, f.seq < e.seq) →
  visible (newest (a ++ b)) k t = some e

/-- At bottommost compaction an old deletion anchor may be dropped with all older
versions for that key. The observer projects the SQL row value. -/
def BottomTombstone.Goal : Prop := ∀ xs oldest k t, ordered xs → oldest ≤ t →
  valueAt ((compact xs oldest).filter (fun e => oldest < e.seq || e.value.isSome)) k t =
    valueAt xs k t

/-- Covering aggregate over a newer run and an older run: newer visible tombstones
suppress the older contribution just as newer live values do. -/
def total (xs : List Entry) (keys : List Nat) (t : Nat) :=
  (keys.filterMap fun k => valueAt xs k t).sum

def OverlayAggregate.Goal : Prop := ∀ a b ks t,
  total (a ++ b) ks t = total a ks t +
    total b (ks.filter fun k => (visible a k t).isNone) t

private def rows : List Entry := [⟨1,8,some 4⟩,⟨1,5,none⟩,⟨2,3,some 9⟩,⟨1,1,some 7⟩]
#guard (compact rows 6).length == 3
#guard valueAt (keepPinned rows [1] 6) 1 1 == some 7
#guard valueAt (applyMarkers rows [⟨1,2,8⟩] 1) 1 8 == none
#guard (valueAt rows 1 6).isNone
#guard probeBatch rows [2,1,2,3] 5 == [some 9,none,some 9,none]
#guard (visible (newest rows) 1 6).map Entry.seq == some 5
#guard valueAt ((compact rows 6).filter (fun e => 6 < e.seq || e.value.isSome)) 1 6 == none
#guard total rows [1,2] 6 == 9

end TinylambOpt.ResearchGoals.VersionedIndexes
