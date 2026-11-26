import Std
import TinylambOpt.ResearchGoals.ExecutionStrategies.VersionedIndexes

namespace TinylambOpt.ResearchGoals.EngineDecisions.MVCC

namespace PublicationHoles
/-! Sealed write sets with unique commit stamps, newest first. A pending
transaction may publish all of its writes or none. A closed read footprint
includes negative lookups. The observation identifies the visible version,
including a tombstone's stamp. -/
structure Tx where
  stamp : Nat
  keys : List Nat
  published : Bool
def winner (txs : List Tx) (snap k : Nat) (includePending : Nat → Bool) :=
  ((txs.find? fun t => t.stamp ≤ snap && t.keys.contains k &&
    (t.published || includePending t.stamp))).map Tx.stamp
def certified (txs : List Tx) (snap : Nat) (keys : List Nat) :=
  keys.all fun k => txs.all fun t =>
    t.published || snap < t.stamp || !t.keys.contains k ||
      (winner txs snap k (fun _ => false)).any (t.stamp < ·)

/-- A read-only query may cross an unpublished timestamp on a certified
footprint. Even a pending write to a queried key is harmless if a newer
published version masks it at this snapshot. -/
def Goal : Prop := ∀ txs snap keys, txs.Pairwise (fun a b => b.stamp < a.stamp) →
  (certified txs snap keys = true ↔
    ∀ finishes k, k ∈ keys →
      winner txs snap k finishes = winner txs snap k (fun _ => false))

private def hole : List Tx := [⟨8,[1],true⟩,⟨7,[1,2],false⟩]
#guard certified hole 8 [1] && !(certified hole 8 [1,2])
end PublicationHoles

namespace UndoPeak
/-! In a single page, an undo step consumes a signed number of bytes; negative
steps release space. Each owner undoes in its own fixed reverse-log order.
Allocation and fragmentation constraints beyond byte accounting are excluded. -/
def peak (steps : List Int) := ((steps.scanl (·+·) 0).max?.getD 0).toNat
abbrev Event := Nat × Int
def legal (schedule : List Event) (owners : List (List Int)) : Prop :=
  (∀ e ∈ schedule, e.1 < owners.length) ∧
  ∀ i ∈ List.range owners.length,
    ((schedule.filter (fun e => e.1 == i)).map Prod.snd) = owners[i]?.getD []

/-- Reserve the sum of the owners' maximum prefix demands. This is the
least sufficient space if rollbacks can interleave in any legal order. -/
def Goal : Prop := ∀ owners free,
  ((owners.map peak).sum ≤ free ↔
    ∀ schedule, legal schedule owners → peak (schedule.map Prod.snd) ≤ free)

#guard peak [5,-5,5] == 5
#guard peak [5,4,-5,-4] == 9
end UndoPeak

namespace ChainSummary
/-! Oldest first. Keep payload changes and unflushed WAL dependencies in
separate timestamped runs. First-committer-wins also needs the head timestamp,
even when every write stored identical bytes. -/
structure Version where
  ts : Nat
  payload : Option Int
  walEnd : Nat
def changes {α : Type} [BEq α] (f : Version → α) : List Version → List Version
  | [] => []
  | v::vs =>
    v :: ((v::vs).zip vs).filterMap (fun (a,b) => if f a == f b then none else some b)
def atSnap (xs : List Version) (t : Nat) := (xs.filter (fun v => v.ts ≤ t)).getLast?
def dependency (durable : Nat) (v : Version) := if v.walEnd ≤ durable then 0 else v.walEnd
def failedWrite (head : Option Nat) (t : Nat) := head.any (t < ·)

/-- Intern a run of identical values without losing (1) the visible payload,
(2) the commit dependency of a read, or (3) the conflict of a stale writer. -/
def Goal : Prop := ∀ (xs : List Version) durable snap, xs.Pairwise (fun a b => a.ts < b.ts) →
  let payloads := changes Version.payload xs
  let deps := changes (dependency durable) xs
  let head := xs.getLast?.map Version.ts
  ((atSnap payloads snap).bind Version.payload,
    (atSnap deps snap).map (dependency durable) |>.getD 0, failedWrite head snap) =
  ((atSnap xs snap).bind Version.payload,
    (atSnap xs snap).map (dependency durable) |>.getD 0, xs.any (fun v => snap < v.ts))

private def sameValue : List Version := [⟨4,some 9,20⟩,⟨8,some 9,70⟩]
#guard (changes Version.payload sameValue).length == 1
#guard (changes (dependency 30) sameValue).length == 2
#guard failedWrite (sameValue.getLast?.map Version.ts) 5
end ChainSummary

namespace HistoricalCover
/-! The current covering index supplies key, payload and stable row ID.
A complete oldest-first log covers every row change since the target snapshot.
Reuse of a physical slot needs an incarnation in the ID for this model. -/
abbrev Image := Option (Nat × Int)
structure Change where
  rid : Nat
  before : Image
  afterwards : Image
def applyLog (s : Nat → Image) (cs : List Change) (k : Nat) : Image :=
  ((cs.filter (fun c => c.rid == k)).getLast?.map Change.afterwards).getD (s k)
def consistent (s : Nat → Image) : List Change → Prop
  | [] => True
  | c::cs => c.before = s c.rid ∧ consistent (fun k => if k == c.rid then c.afterwards else s k) cs
def hits (p : Nat → Bool) (id : Nat) (img : Image) : Option (Nat × Nat × Int) := do
  let (k,v) ← img
  if p k then some (id,k,v) else none
def repair (p : Nat → Bool) (currentHits : List (Nat × Nat × Int)) (cs : List Change) :=
  let dirty := (cs.map Change.rid).eraseDups
  (currentHits.filter (fun row => !dirty.contains row.1)) ++
    dirty.filterMap fun k => (cs.find? (fun c => c.rid == k)).bind (fun c => hits p k c.before)

/-- Recover a historical covering scan from today's index plus the first
before-image per touched row. This restores keys moved out of range as well
as changed INCLUDE values, without a historical full-table scan. -/
def Goal : Prop := ∀ p s cs n, consistent s cs → (∀ c ∈ cs, c.rid < n) →
  let ids := List.range n
  let currentHits := ids.filterMap fun k => hits p k (applyLog s cs k)
  (repair p currentHits cs).Perm (ids.filterMap fun k => hits p k (s k))

#guard repair (· == 7) [] [⟨0,some (7,42),some (8,42)⟩] == [(0,7,42)]
end HistoricalCover

namespace GcWakeup
/-! Frontier-only rescheduling for a single committed chain, oldest first.
Any modification of a chain or its pending flag independently schedules it.
The heap is the authoritative newest image; LSN zero is already durable. -/
structure V where
  ts : Nat
  lsn : Nat
  deriving BEq, DecidableEq
def vacuum (chain : List V) (pinned : Bool) (snap durable : Nat) :=
  let head := chain.getLast?
  if !pinned && head.any (fun v => v.ts ≤ snap && v.lsn ≤ durable) then []
  else ((chain.filter (fun v => v.ts ≤ snap)).getLast?).toList ++ chain.filter (fun v => snap < v.ts)
def due (live : List V) (pending : Bool) (snap durable : Nat) :=
  (live[1]?).any (fun v => v.ts ≤ snap) ||
  (!pending && live.getLast?.any (fun v => v.ts ≤ snap && v.lsn ≤ durable))

/-- After one sweep, watch the second version's timestamp and the last
version's joint snapshot/durability fence. Revisit exactly when another sweep
would release an entry, even when only WAL durability advances. -/
def Goal : Prop := ∀ chain pending h d h' d', chain.Pairwise (fun a b => a.ts < b.ts) →
  h ≤ h' → d ≤ d' → let live := vacuum chain pending h d
  (due live pending h' d' = true ↔ vacuum live pending h' d' ≠ live)

#guard !(due [⟨3,40⟩] false 9 39) && due [⟨3,40⟩] false 9 40
end GcWakeup

namespace OverlayCache
open TinylambOpt.ResearchGoals.VersionedIndexes (Entry valueAt)
/-! A transaction sees its staged writes before its committed snapshot.
A staged tombstone is different from absence of a staged write. The committed
snapshot stays fixed; no DML head reads or durability observations are cached. -/
abbrev Edit := Nat × Option Int
def read (history : List Entry) (snap key : Nat) (newestFirst : List Edit) :=
  ((newestFirst.lookup key).getD (valueAt history key snap))
def retain (old : Nat → Option Int) (delta : List Edit) (key : Nat) :=
  match delta.lookup key with
  | none => old key
  | some v => v

/-- A batch of workers can reuse a transaction-private lookup cache over a
fixed snapshot by patching each hot key once with the *last* staged edit.
Negative cache hits are patched as well as positive ones. -/
def Goal : Prop := ∀ history snap old (delta : List Edit) (requests : List Nat),
  let cached := fun k => read history snap k old
  let patch := delta.reverse
  let compact := (patch.map Prod.fst).eraseDups.filterMap fun k => (patch.lookup k).map (k,·)
  (requests.map (retain cached compact)) =
    requests.map (fun k => read history snap k (delta.reverse ++ old))

#guard retain (fun _ => some 5) [(3,none)] 3 == none
end OverlayCache

namespace GuardedDelete
/-! Optional commit-time guard on a counting invariant. Two transactions
at the same snapshot may remove different rows. A lower quota applies to the
qualifying rows *after* the pending delete. The identity set is authoritative. -/
def survivors (xs removed : List Nat) := xs.filter (fun k => !removed.contains k)
def quotaOK (snapshot ownDeletes concurrentDeletes inserts : List Nat) (quota : Nat) : Bool :=
  let baseCount := snapshot.length - (ownDeletes.eraseDups.filter snapshot.contains).length
  let lost := (concurrentDeletes.eraseDups.filter (fun k => snapshot.contains k && !ownDeletes.contains k)).length
  let gained := (survivors inserts ownDeletes).length
  quota + lost ≤ baseCount + gained

/-- Validate a quota-guarded delete from the statement's old count and
incremental gains/losses at publication. Cross-transaction write-set
disjointness alone allows write skew. This is an additional commit guard. -/
def Goal : Prop := ∀ snapshot own other inserted quota,
  snapshot.Nodup → inserted.Nodup → (∀ k ∈ inserted, k ∉ snapshot) →
  (quotaOK snapshot own other inserted quota = true ↔
   quota ≤ (survivors (survivors snapshot other ++ inserted) own).length)

#guard quotaOK [10,20] [10] [] [] 1
#guard !(quotaOK [10,20] [10] [20] [] 1)
end GuardedDelete

namespace ExistsDurability
/-! A read-only EXISTS at one snapshot with a closed predicate footprint.
Start at a durable base and replay the relevant visible transactions as atomic
batches in increasing, positive WAL-end order. Edits are post-write membership.
Only one Boolean is returned. No other reads or session dependencies are modeled. -/
structure Batch where
  walEnd : Nat
  edits : List (Nat × Bool)
def applyBatch (live : List Nat) (batch : Batch) :=
  batch.edits.foldl (fun s (k,qualifies) =>
    let rest := s.filter (· != k)
    if qualifies then k::rest else rest) live
def observation (initial : List Nat) (log : List Batch) :=
  !((log.foldl applyBatch initial).isEmpty)
def lastFlip (live : List Nat) (fence : Nat) : List Batch → Nat
  | [] => fence
  | b::bs =>
    let next := applyBatch live b
    lastFlip next (if next.isEmpty == live.isEmpty then fence else b.walEnd) bs

/-- The WAL end of the last *committed* truth change is the minimum fence
that makes this reply reproducible at every possible subsequent WAL prefix. -/
def Goal : Prop := ∀ initial log fence,
  (∀ b ∈ log, 0 < b.walEnd) → log.Pairwise (fun a b => a.walEnd < b.walEnd) →
  (lastFlip initial 0 log ≤ fence ↔ ∀ durable, fence ≤ durable →
    observation initial (log.filter (fun b => b.walEnd ≤ durable)) = observation initial log)

private def swapWitness : List Batch := [⟨3,[(1,true)]⟩,⟨9,[(1,false),(2,true)]⟩]
#guard lastFlip [] 0 swapWitness == 3
#guard lastFlip [] 0 (swapWitness ++ [⟨12,[(2,false)]⟩]) == 12
end ExistsDurability

end TinylambOpt.ResearchGoals.EngineDecisions.MVCC
