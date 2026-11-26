import Std

namespace TinylambOpt.ResearchGoals.EngineBoundaries.SpillLeases

/-! One reusable physical block of a spill store. A logical drop prevents new
read admissions immediately. A pinned reader can finish against the same
immutable payload; the block becomes reusable only after the last release.
Tickets represent distinct in-flight reads and epochs here never wrap. -/
structure Block where
  epoch : Nat := 0
  live : Bool := false
  payload : Nat := 0
  pins : List Nat := []
  deriving BEq, DecidableEq, Inhabited
structure Lease where
  epoch : Nat
  ticket : Nat
  deriving BEq

def acquire (s : Block) (t : Nat) : Option (Lease × Block) :=
  if s.live && !s.pins.contains t then some (⟨s.epoch,t⟩, {s with pins := t::s.pins}) else none
def readLease (s : Block) (l : Lease) :=
  if s.epoch == l.epoch && s.pins.contains l.ticket then some s.payload else none
def release (s : Block) (l : Lease) :=
  if s.epoch == l.epoch then {s with pins := s.pins.erase l.ticket} else s
def dropBlock (s : Block) := {s with live := false}
def reclaimable (s : Block) := !s.live && s.pins.isEmpty
def reuse (s : Block) (v : Nat) : Option Block :=
  if reclaimable s then some ⟨s.epoch+1,true,v,[]⟩ else none

/-- Logical drop immediately blocks new readers while preserving every admitted lease's bytes. -/
def HideBeforeFree.Goal : Prop := ∀ s t l,
  acquire (dropBlock s) t = none ∧ readLease (dropBlock s) l = readLease s l

/-- Recycling the same physical block cannot satisfy a read from its former generation. -/
def GenerationFence.Goal : Prop := ∀ s t l v, l.epoch ≤ s.epoch → reuse s v = some t →
  readLease t l = none

/-- An admitted reader prevents reuse even after the logical segment is dropped. -/
def PinBlocksReuse.Goal : Prop := ∀ s t l next v, acquire s t = some (l,next) →
  reuse (dropBlock next) v = none

/-- Releasing one independent read leaves every other ticket readable. -/
def IndependentRelease.Goal : Prop := ∀ s a b, a.ticket ≠ b.ticket →
  readLease (release s a) b = readLease s b

/-- The last release after a drop is the precise point where the physical space is available. -/
def LastRelease.Goal : Prop := ∀ s l, s.pins.Nodup → readLease s l ≠ none →
  (reclaimable (release (dropBlock s) l) = true ↔ s.pins.length = 1)

/-- A stale release from an old generation cannot decrement the new block's pin count. -/
def LateRelease.Goal : Prop := ∀ s l, l.epoch < s.epoch → release s l = s

/-- Multiple cleanup paths that drop and unpin the same block commute. -/
def DropReleaseOrder.Goal : Prop := ∀ s l, dropBlock (release s l) = release (dropBlock s) l

/-- Can the epoch alone distinguish two reads when a ticket is reused within one generation? -/
def SameEpochRetry.Goal : Prop := ∀ s id l a b fresh, acquire s id = some (l,a) →
  acquire (release a l) id = some (fresh,b) → readLease (release b l) fresh = some b.payload

/-- Limiting the stored generation to k bits keeps generations distinguishable for all lifetimes. -/
def WrappedEpoch.Goal : Prop := ∀ (k a b : Nat), 0 < k → a < b → a % 2^k ≠ b % 2^k

/-- Retiring a block with no outstanding lease permits an immediate fresh allocation. -/
def ReuseAfterDrop.Goal : Prop := ∀ s v, s.pins = [] →
  reuse (dropBlock s) v = some ⟨s.epoch+1,true,v,[]⟩

private def active : Block := ⟨4,true,42,[]⟩
private def ticket : Lease := ⟨4,7⟩
#guard acquire active 7 == some (ticket, {active with pins := [7]})
#guard readLease (release {active with pins := [7]} ticket) ticket == none
#guard 1 % 4 == 5 % 4

end TinylambOpt.ResearchGoals.EngineBoundaries.SpillLeases
