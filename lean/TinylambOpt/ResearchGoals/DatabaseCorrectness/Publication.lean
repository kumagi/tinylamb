import Std

/-! Model the pending-timestamp mutex and subsequent monotone update of the
stable watermark separately. A resolved ticket denotes either full publication
or complete removal of a canceled version. The release candidate can be delayed. -/
namespace TinylambOpt.ResearchGoals.DatabaseCorrectness.Publication

structure Clock where
  allocated : Nat
  pending : List Nat
  stable : Nat
  deriving DecidableEq, Repr

def newTicket (c : Clock) : Clock :=
  {c with allocated := c.allocated+1, pending := (c.allocated+1)::c.pending}
def resolve (c : Clock) (t : Nat) : Clock := {c with pending := c.pending.erase t}
def candidate (c : Clock) := (c.pending.min?.map (·-1)).getD c.allocated
def announce (c : Clock) (observed : Nat) := {c with stable := max c.stable observed}
def sound (c : Clock) := c.pending.Nodup ∧ c.stable ≤ c.allocated ∧
  ∀ t ∈ c.pending, c.stable < t ∧ t ≤ c.allocated

/-- Registering a commit before releasing its allocator lock cannot expose its timestamp to a reader. -/
def AllocateBeforeSnapshotAdmission.Goal : Prop := ∀ c, sound c →
  sound (newTicket c) ∧ candidate (newTicket c) = candidate c

/-- Resolving a later publisher leaves the first unresolved timestamp as a hard reader fence. -/
def PublicationCannotLeapAcrossHole.Goal : Prop := ∀ c earlier later, sound c →
  earlier ∈ c.pending → later ∈ c.pending → earlier < later →
  candidate (resolve c later) < earlier

/-- Installing a release candidate after a different publication completed cannot regress the stable snapshot. -/
def DelayedReleaseOrderIndependence.Goal : Prop := ∀ c x y,
  announce (announce c x) y = announce (announce c y) x

inductive Event where
  | allocate | finish (ticket : Nat)
  deriving Repr

def change (c : Clock) : Event → Clock
  | .allocate => newTicket c
  | .finish t => resolve c t

/-- A candidate recorded under the mutex remains safe after further allocations and resolutions,
even if a different publisher installed a larger safe watermark in the meantime. -/
def StaleCandidateSafeAfterTail.Goal : Prop := ∀ c (events : List Event) stableNow, sound c →
  let later := events.foldl change c
  stableNow ≤ candidate later → sound (announce {later with stable := stableNow} (candidate c))

/-- Draining all existing tickets lets a new snapshot reach the allocator high water independently of finish order. -/
def PublicationDrainWithoutGap.Goal : Prop := ∀ c order, sound c → c.pending.Perm order →
  let final := order.foldl resolve c
  candidate final = c.allocated ∧ final.pending = []

structure ChangeRow where
  ticket : Nat
  row : Nat
  value : Option Nat
  deriving DecidableEq, Repr

/-- Per-key lookup picks the latest eligible ticket, regardless of shard insertion order. -/
def readAt (writes : List ChangeRow) (snap row : Nat) :=
  let eligible := writes.filter (fun v => v.row == row && v.ticket ≤ snap)
  ((eligible.mergeSort (fun a b => decide (b.ticket ≤ a.ticket))).head?).bind ChangeRow.value

/-- Physically installing a writer's rows one at a time cannot fracture a snapshot below its pending ticket. -/
def PartialShardInstallInvisible.Goal : Prop := ∀ c t old batch snap row, sound c → t ∈ c.pending →
  snap ≤ c.stable → (∀ w ∈ batch, w.ticket = t) →
  readAt (batch++old) snap row = readAt old snap row

/-- A newer committed delete must continue to hide an older replacement installed by a delayed shard worker. -/
def LateInstallCannotResurrectDeletedRow.Goal : Prop := ∀ r v a b snap,
  a < b → b ≤ snap → readAt [⟨a,r,some v⟩,⟨b,r,none⟩] snap r = none

#guard candidate (resolve ⟨9,[9,7],6⟩ 9) == 6

end TinylambOpt.ResearchGoals.DatabaseCorrectness.Publication
