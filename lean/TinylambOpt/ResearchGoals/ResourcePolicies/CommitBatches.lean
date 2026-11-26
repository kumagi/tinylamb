import Std

/-! Group commit under single-stream WAL ordering. A waiter carries an exclusive
end offset. Acknowledgement depends on a confirmed contiguous durable prefix;
partial writes, cancellation, and out-of-order completions are modeled separately. -/
namespace TinylambOpt.ResearchGoals.ResourcePolicies.CommitBatches

structure Waiter where
  client : Nat
  stop : Nat
  deriving Repr, DecidableEq
def wake (waiters : List Waiter) (durable : Nat) := waiters.filter (fun w => w.stop ≤ durable)
def pending (waiters : List Waiter) (durable : Nat) := waiters.filter (fun w => durable < w.stop)

/-- Wake only newly durable waiters on each callback, preserving multiplicity across two fsyncs. -/
def BatchedDurabilityWakeups.Goal : Prop := ∀ ws a b, a ≤ b →
  (wake ws a ++ wake (pending ws a) b).Perm (wake ws b)

/-- Scanning waiters until the first undurable record suffices when enqueues respect record end order. -/
def OrderedCommitWaitPrefix.Goal : Prop := ∀ ws d, ws.Pairwise (fun a b => a.stop ≤ b.stop) →
  ws.takeWhile (fun w => w.stop ≤ d) = wake ws d

/-- Canceling a client before notification may elide its wakeup without changing remaining client order. -/
def CanceledWaiterWakeIsolation.Goal : Prop := ∀ ws d c,
  wake (ws.filter (fun w => w.client != c)) d = (wake ws d).filter (fun w => w.client != c)

structure Write where
  start : Nat
  count : Nat
  deriving Repr, DecidableEq

def cover (writes : List Write) (byte : Nat) :=
  writes.any (fun w => w.start ≤ byte && byte < w.start+w.count)
def prefixEnd (writes : List Write) (base limit : Nat) :=
  base + ((List.range (limit-base)).takeWhile (fun i => cover writes (base+i))).length

/-- An out-of-order completion list yields a safe contiguous fsync target by stopping at the first hole. -/
def CompletedWriteContiguousCut.Goal : Prop := ∀ ws base limit t, base ≤ t → t ≤ limit →
  (t ≤ prefixEnd ws base limit ↔ ∀ i, base ≤ i → i < t → cover ws i = true)

/-- The largest completed write end can safely wake all earlier WAL waiters even with concurrent submissions. -/
def LargestCompletionWakesPrefix.Goal : Prop := ∀ (ws : List Write) t,
  ws ≠ [] → (ws.map (fun w => w.start+w.count)).max? = some t → prefixEnd ws 0 t = t

/-- Retries overlapping previously confirmed writes do not advance the safe cut a second time. -/
def OverlapRetryDurableCut.Goal : Prop := ∀ ws extra base limit,
  (∀ i, cover extra i = true → cover ws i = true) →
  prefixEnd (ws++extra) base limit = prefixEnd ws base limit

/-- An existing confirmed prefix can be skipped while scanning for the next durable wakeup. -/
def IncrementalCompletionScan.Goal : Prop := ∀ a b base limit, base ≤ limit →
  prefixEnd (a++b) (prefixEnd a base limit) limit = prefixEnd (a++b) base limit

/-- Keep only completions beginning beyond the previous durable cut when resuming a partial prefix scan. -/
def StartOnlyCompletionPrune.Goal : Prop := ∀ ws base limit,
  prefixEnd (ws.filter (fun w => base ≤ w.start)) base limit = prefixEnd ws base limit

/-- A bound on pending WAL bytes alone caps the number of client acknowledgements that can await the same fsync. -/
def WALBytesBoundWaitingClients.Goal : Prop := ∀ (ws : List Waiter) durable tip,
  (∀ w ∈ ws, durable < w.stop ∧ w.stop ≤ tip) → ws.length ≤ tip-durable

/-- Charging one acknowledgement slot per distinct exclusive end is sufficient for byte-based credit accounting. -/
def DistinctCommitEndsCredit.Goal : Prop := ∀ (ws : List Waiter) durable tip,
  (∀ w ∈ ws, durable < w.stop ∧ w.stop ≤ tip) → (ws.map Waiter.stop).eraseDups.length ≤ tip-durable

#guard prefixEnd [⟨4,3⟩,⟨0,2⟩] 0 7 == 2

#guard prefixEnd [⟨4,3⟩,⟨0,2⟩,⟨2,2⟩] 0 7 == 7

end TinylambOpt.ResearchGoals.ResourcePolicies.CommitBatches
