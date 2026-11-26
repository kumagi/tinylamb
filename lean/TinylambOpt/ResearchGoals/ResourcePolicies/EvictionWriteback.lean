import Std

/-! Eviction writeback races for one reusable page slot. A flush ticket captures
the slot incarnation, content revision, WAL dependency, and image. The durable
backing image may advance while a pinned resident page remains dirty. -/
namespace TinylambOpt.ResearchGoals.ResourcePolicies.EvictionWriteback

structure Image where
  incarnation : Nat
  revision : Nat
  wal : Nat
  payload : Nat
  deriving DecidableEq, Repr
structure Slot where
  live : Image
  disk : Option Image
  dirty : Bool
  pins : Nat
  deriving DecidableEq, Repr

def finish (s : Slot) (ticket : Image) (durable : Nat) :=
  if ticket.incarnation != s.live.incarnation || durable < ticket.wal ||
      s.disk.any (fun d => d.incarnation == ticket.incarnation && ticket.revision < d.revision)
  then s else
    {s with disk := some ticket, dirty := s.live != ticket}
def reclaimable (s : Slot) := s.pins == 0 && !s.dirty && s.disk == some s.live
def mutate (s : Slot) (lsn data : Nat) :=
  {s with live := {s.live with revision := s.live.revision+1, wal := lsn, payload := data}, dirty := true}

/-- Late completion after a concurrent mutation cannot make the changed page reclaimable. -/
def FlushRacingMutation.Goal : Prop := ∀ s lsn data durable,
  reclaimable (finish (mutate s lsn data) s.live durable) = false

/-- A stale write to a recycled physical slot may not evict or replace the new incarnation. -/
def RecycledSlotCompletionFence.Goal : Prop := ∀ s t wal, s.live.incarnation ≠ t.incarnation →
  finish s t wal = s

/-- Once a newer image is durable, completing an older ticket again cannot roll the disk revision back. -/
def OutOfOrderWriteback.Goal : Prop := ∀ s old new wal, old.incarnation = new.incarnation →
  s.live.incarnation = new.incarnation → old.revision < new.revision → new.wal ≤ wal →
  (s.disk.all (fun d => d.incarnation != new.incarnation || d.revision ≤ new.revision)) = true →
  (finish (finish s new wal) old wal).disk = some new

/-- Eviction after a complete flush recovers the exact live byte image and respects the WAL fence. -/
def EvictionReadbackCertificate.Goal : Prop := ∀ s t wal, s.dirty = true →
  reclaimable (finish s t wal) = true → t = s.live ∧ t.wal ≤ wal ∧ s.pins = 0

/-- A second completion callback for a successful ticket is harmless, including its dirty-bit transition. -/
def RepeatedFlushCompletion.Goal : Prop := ∀ s t wal, finish (finish s t wal) t wal = finish s t wal

/-- Dropping an earlier flush in favor of the newest dirty revision preserves the
recoverable backing image even when the later revision has a higher WAL dependency. -/
def MaxRevisionFlushCoalescing.Goal : Prop := ∀ s a b wal,
  s.live = b → a.incarnation = b.incarnation → a.revision < b.revision → a.wal ≤ b.wal →
  (finish (finish s a wal) b wal).disk = (finish s b wal).disk

/-- If a page is pinned only after dispatch, a valid completion may update backing bytes but must leave it resident. -/
def PinDuringWriteback.Goal : Prop := ∀ s t wal, 0 < s.pins → reclaimable (finish s t wal) = false

/-- A durable backing image with the same payload can replace a live page despite distinct redo dependencies. -/
def EqualPayloadSkipsWalFence.Goal : Prop := ∀ (s : Slot) (d : Image), s.disk = some d → d.payload = s.live.payload →
  d.incarnation = s.live.incarnation → d.wal ≥ s.live.wal

def tag (modulus : Nat) (i : Image) := (i.incarnation%modulus, i.revision%modulus)

/-- Compact ticket tags distinguish all still-live tickets inside a bounded window on each counter. -/
def BoundedTicketTagAlias.Goal : Prop := ∀ modulus a b, 0 < modulus →
  max a.incarnation b.incarnation - min a.incarnation b.incarnation < modulus →
  max a.revision b.revision - min a.revision b.revision < modulus →
  tag modulus a = tag modulus b → a.incarnation = b.incarnation ∧ a.revision = b.revision

#guard
  let a := Image.mk 1 3 40 10
  let b := Image.mk 1 4 50 11
  let s := Slot.mk b none true 0
  (finish (finish s a 40) b 40).disk == some a && (finish s b 40).disk == none

end TinylambOpt.ResearchGoals.ResourcePolicies.EvictionWriteback
