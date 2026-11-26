import Std

/-! Compare transaction-local staged LSM writes with the lower-memory WAL path.
LSNs are byte positions (not list indices). A write at offset zero is a real head.
The validated links of one owner can jump over arbitrary other owners' records. -/
namespace TinylambOpt.ResearchGoals.DatabaseCorrectness.WalReread

structure Edit where
  root : Nat
  key : Nat
  value : Option Nat
  deriving DecidableEq, Repr
structure Record where
  start : Nat
  prev : Nat
  owner : Nat
  edit : Option Edit
  deriving DecidableEq, Repr
def ownEdits (rs : List Record) (owner lo hi : Nat) :=
  (rs.filter (fun r => r.owner == owner && lo ≤ r.start && r.start < hi)).filterMap Record.edit
def pending (edits : List Edit) (root key : Nat) : Option (Option Nat) :=
  (edits.reverse.find? (fun e => e.root == root && e.key == key)).map Edit.value
def backward (wal : List Record) (owner first : Nat) : Nat → Nat → List Record
  | 0, _ => []
  | fuel+1, cur =>
    match wal.find? (fun r => r.start == cur && r.owner == owner) with
    | none => []
    | some r => r :: (if cur == first || cur ≤ r.prev || r.prev < first then []
                      else backward wal owner first fuel r.prev)
def linked : List Record → Prop
  | a::b::rs => b.prev = a.start ∧ a.start < b.start ∧ linked (b::rs)
  | _ => True

/-- Reverse WAL lookup on a valid chain returns exactly the staged writes in reverse order,
including the earliest write at zero and links that jump over other transactions. -/
def ReverseOwnerChainMatchesForwardWindow.Goal : Prop := ∀ log owner first last endPos,
  (log.map Record.start).Nodup → log.Pairwise (fun a b => a.start < b.start) →
  let own := log.filter (fun r => r.owner == owner && first ≤ r.start && r.start < endPos)
  (own.head?.map Record.start) = some first → (own.getLast?.map Record.start) = some last → linked own →
  (backward log owner first own.length last).filterMap Record.edit = (ownEdits log owner first endPos).reverse

/-- Inserting log records of other transactions inside a window cannot change a read-your-writes lookup. -/
def InterleavedForeignWritesDoNotShadowOwn.Goal : Prop := ∀ a foreign b owner lo hi root key,
  (∀ r ∈ foreign, r.owner ≠ owner) →
  pending (ownEdits (a++foreign++b) owner lo hi) root key = pending (ownEdits (a++b) owner lo hi) root key

/-- The write at FirstLSN=0 participates both in undo enumeration and in lookup after switching to WAL mode. -/
def ZeroFirstLsnIsARealRecord.Goal : Prop := ∀ owner e tail, let first : Record := ⟨0,0,owner,some e⟩
  (backward (first::tail) owner 0 1 0).filterMap Record.edit = [e]

/-- A staged tombstone shadows a read in both modes; no pending write and a deletion have distinct observations. -/
def PendingTombstoneShadowsValue.Goal : Prop := ∀ es root key, pending (es++[⟨root,key,none⟩]) root key = some none

/-- Committing in WAL mode may scan consecutive ranges separately, but must apply edits in owner-local chronological order. -/
def SegmentedOwnerCommitOrder.Goal : Prop := ∀ log owner lo mid hi, lo ≤ mid → mid ≤ hi →
  log.Pairwise (fun a b => a.start < b.start) →
  ownEdits log owner lo mid ++ ownEdits log owner mid hi = ownEdits log owner lo hi

/-- A broken nondecreasing prev link terminates the fallback traversal after the record that contains it. -/
def BadBacklinkNeverReplaysRecordTwice.Goal : Prop := ∀ log owner first fuel cur,
  ((backward log owner first fuel cur).map Record.start).Nodup

end TinylambOpt.ResearchGoals.DatabaseCorrectness.WalReread
