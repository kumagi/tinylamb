import Std

/-! Single-key histories in descending commit order. A selected version may itself
be a tombstone. Compare dense oldest-snapshot retention with sparse pins, partial
compaction, and bounded sweeps of a sharded history table. -/
namespace TinylambOpt.ResearchGoals.ResourcePolicies.VersionVacuum

structure Version where
  stamp : Nat
  value : Option Nat
  deriving DecidableEq, Repr

def winner (vs : List Version) (s : Nat) := vs.find? (fun v => v.stamp ≤ s)
def readAt (vs : List Version) (s : Nat) := (winner vs s).bind Version.value
def dense (vs : List Version) (floor : Nat) :=
  vs.filter (fun v => floor < v.stamp) ++ (winner vs floor).toList
def sparse (vs : List Version) (pins : List Nat) :=
  vs.filter (fun v => pins.any (fun s => winner vs s == some v))
def ordered (vs : List Version) := vs.Pairwise (fun a b => b.stamp < a.stamp)

/-- One predecessor suffices across all future snapshot times above the oldest pin. -/
def DenseFloorFutureReads.Goal : Prop := ∀ vs f s, ordered vs → f ≤ s → readAt (dense vs f) s = readAt vs s

/-- Retaining actual winners can preserve pinned snapshots even when there are long gaps between them. -/
def SparseSnapshotWinnerRetention.Goal : Prop := ∀ vs pins s, s ∈ pins →
  winner (sparse vs pins) s = winner vs s

/-- Shrinking the pin set can compact an already pruned chain without resurrecting discarded versions. -/
def PinReleaseIncrementalPruning.Goal : Prop := ∀ vs (p q : List Nat), (∀ s ∈ q, s ∈ p) →
  sparse (sparse vs p) q = sparse vs q

/-- At most one version per distinct pin is needed for pinned reads when commit stamps are unique. -/
def DistinctPinStorageCap.Goal : Prop := ∀ vs pins, ordered vs →
  (sparse vs pins).length ≤ pins.eraseDups.length

/-- Keeping only pins at the endpoints of a continuous snapshot interval preserves each interior read. -/
def EndpointSnapshotInterpolation.Goal : Prop := ∀ vs lo hi s, ordered vs → lo ≤ s → s ≤ hi →
  readAt (sparse vs [lo,hi]) s = readAt vs s

/-- A sparse live-set collector may leave out the head only if future reads cannot observe a new value there. -/
def IncludeNewestForFutureReads.Goal : Prop := ∀ vs pins tip s, ordered vs →
  (∀ v ∈ vs, v.stamp ≤ tip) → tip ≤ s → readAt (sparse vs (tip::pins)) s = readAt vs s

/-- Vacuum of different physical fragments at the same floor may be performed independently before joining histories. -/
def FragmentLocalSnapshotFloor.Goal : Prop := ∀ a b f s, ordered (a++b) → f ≤ s →
  readAt (dense a f ++ dense b f) s = readAt (a++b) s

/-- Try discarding tombstones in the upper fragment without checking whether a lower fragment still carries a shadowed value. -/
def BottomCoverageBeforeTombstoneDrop.Goal : Prop := ∀ upper lower s,
  ordered (upper++lower) → (winner upper s).isSome →
  readAt ((upper.filter (fun v => v.value.isSome))++lower) s = readAt (upper++lower) s

/-- A second collection at a higher floor needs no copy of the original uncollected chain. -/
def AdvancingFloorComposition.Goal : Prop := ∀ vs a b, ordered vs → a ≤ b →
  dense (dense vs a) b = dense vs b

def visits (shards stride start budget : Nat) :=
  (List.range budget).map (fun i => (start+i*stride)%shards)

/-- A rotating GC start covers every shard before repeating if the stride and shard count are coprime. -/
def CoprimeVacuumSweep.Goal : Prop := ∀ n stride start, 0 < n → Nat.gcd stride n = 1 →
  (visits n stride start n).Perm (List.range n)

/-- Slicing the sweep into transactions leaves the sequence of visited shards unchanged. -/
def BudgetedVacuumResume.Goal : Prop := ∀ n stride start a b, 0 < n →
  visits n stride start a ++ visits n stride ((start+a*stride)%n) b = visits n stride start (a+b)

/-- An eviction watermark is a sufficient certificate of no removed conflicting write strictly after a snapshot. -/
def EvictedWriteConflictCertificate.Goal : Prop := ∀ (evicted : List (Nat × Nat)) s k,
  ((evicted.map Prod.snd).max?).getD 0 ≤ s → ∀ t, (k,t) ∈ evicted → t ≤ s

/-- One shard-wide eviction watermark can replace exact key history without adding false write conflicts. -/
def ShardWatermarkExactConflict.Goal : Prop := ∀ (evicted : List (Nat × Nat)) s k,
  s < ((evicted.map Prod.snd).max?).getD 0 → ∃ t, (k,t) ∈ evicted ∧ s < t

#guard readAt [⟨7,none⟩,⟨2,some 9⟩] 8 == none
#guard visits 6 2 0 6 == [0,2,4,0,2,4]

#guard readAt [⟨2,some 9⟩] 8 == some 9

end TinylambOpt.ResearchGoals.ResourcePolicies.VersionVacuum
