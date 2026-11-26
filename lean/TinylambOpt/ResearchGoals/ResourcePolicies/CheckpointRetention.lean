import Std

/-! Retention of WAL segments and immutable snapshot files through a publication
cut. Segment ends and record ends are exclusive. Reader pins can outlive manifest
replacement. Checkpoint selection uses durable, complete generations only. -/
namespace TinylambOpt.ResearchGoals.ResourcePolicies.CheckpointRetention

structure Span where
  lo : Nat
  hi : Nat
  deriving DecidableEq, Repr

def intersects (a b : Span) := decide (a.lo < b.hi ∧ b.lo < a.hi)
def retained (segments : List Span) (floor : Nat) := segments.filter (fun s => floor < s.hi)
def floorAt (checkpoint : Nat) (active : List Nat) := active.foldl min checkpoint

/-- Recycling whole segments below a start LSN preserves every fragment of every later nonempty record. -/
def WholeSegmentReplayCoverage.Goal : Prop := ∀ segs cp pins r, floorAt cp pins ≤ r.lo →
  (retained segs (floorAt cp pins)).filter (intersects r) = segs.filter (intersects r)

/-- A record that ends after the floor may be reconstructed solely from segments retained at that floor. -/
def StraddlingRecordFloor.Goal : Prop := ∀ segs floor r, r.lo < r.hi → floor < r.hi →
  (retained segs floor).filter (intersects r) = segs.filter (intersects r)

/-- After a new transaction starts later than the existing floor, tracking it does not delay the current recycle decision. -/
def LatePinRecycleInvariance.Goal : Prop := ∀ segs cp pins x, floorAt cp pins ≤ x →
  retained segs (floorAt cp (x::pins)) = retained segs (floorAt cp pins)

/-- Removing pins lets retention advance, with includes determined by identities rather than queue order. -/
def RetiredPinRetentionMonotonicity.Goal : Prop := ∀ segs cp (old fresh : List Nat),
  (∀ p ∈ fresh, p ∈ old) → ∀ seg ∈ retained segs (floorAt cp fresh), seg ∈ retained segs (floorAt cp old)

structure Generation where
  epoch : Nat
  cut : Nat
  files : List Nat
  deriving DecidableEq, Repr

def ready (wal : Nat) (disk : List Nat) (g : Generation) :=
  decide (g.cut ≤ wal) && g.files.all (fun f => disk.contains f)

def choose (newestFirst : List Generation) (wal : Nat) (disk : List Nat) :=
  newestFirst.find? (ready wal disk)

/-- Retrying an unpublished or incomplete manifest preserves the last restorable generation. -/
def IncompleteManifestFallback.Goal : Prop := ∀ gs disk wal g, ready wal disk g = false →
  choose (g::gs) wal disk = choose gs wal disk

/-- The chosen epoch never regresses when more WAL and immutable files become durable. -/
def DurablePublicationEpochMonotonicity.Goal : Prop := ∀ gs w₁ w₂ (a b : List Nat) x y,
  gs.Pairwise (fun p q => q.epoch < p.epoch) → w₁ ≤ w₂ → (∀ f ∈ a, f ∈ b) →
  choose gs w₁ a = some x → choose gs w₂ b = some y → x.epoch ≤ y.epoch

/-- An epoch-ordered manifest list can be truncated to its first entry before verifying the file set. -/
def NewestManifestOnlyRecovery.Goal : Prop := ∀ gs w disk, choose (gs.take 1) w disk = choose gs w disk

def sweep (disk : List Nat) (roots : List Generation) :=
  disk.filter (fun f => roots.any (fun g => g.files.contains f))

/-- Reclaim files using all readers and recovery roots while preserving every ready pinned manifest. -/
def PinnedGenerationFileClosure.Goal : Prop := ∀ disk roots g w, g ∈ roots →
  ready w (sweep disk roots) g = ready w disk g

/-- A file id present in several generations must be charged once in retained snapshot space. -/
def SharedFileIncrementalCharge.Goal : Prop := ∀ (roots : List Generation) g (bytes : Nat → Nat),
  let live := (roots.flatMap Generation.files).eraseDups
  let added := g.files.eraseDups.filter (fun f => !live.contains f)
  (((g::roots).flatMap Generation.files).eraseDups.map bytes).sum = (live.map bytes).sum + (added.map bytes).sum

/-- Retiring one manifest can unlink its files if each is absent from the immediate successor. -/
def AdjacentManifestUnlink.Goal : Prop := ∀ (older : List Generation) (current successor : Generation) f,
  f ∈ current.files → ¬ f ∈ successor.files → ¬ f ∈ ((successor::older).flatMap Generation.files)

/-- A selected manifest certifies both the durable replay cut and the presence of all its file dependencies. -/
def ManifestCutCannotExceedDurability.Goal : Prop := ∀ gs wal disk g,
  choose gs wal disk = some g → g.cut ≤ wal ∧ ∀ f ∈ g.files, f ∈ disk

#guard retained [⟨0,8⟩,⟨8,16⟩] (floorAt 12 [6]) == [⟨0,8⟩,⟨8,16⟩]

#guard choose [⟨2,12,[5]⟩,⟨1,8,[4]⟩] 12 [4] == some ⟨1,8,[4]⟩

end TinylambOpt.ResearchGoals.ResourcePolicies.CheckpointRetention
