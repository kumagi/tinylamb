import TinylambOpt.ResearchGoals.RestartRaces.Recovery

/-! Two-pass replay on validated records for one abstract after-image per key.
Input order is WAL order. The first pass reuses the existing commit-owner summary.
A delete is represented by a present edit with no value, not by skipping the edit.
The cutoff is a record boundary; byte-level damage is modeled separately. -/
namespace TinylambOpt.ResearchGoals.DatabaseCorrectness.WinnerRecovery
open TinylambOpt.ResearchGoals.RestartRaces.Recovery.DestroySummary (Rec Op owners)

inductive Action where
  | put (key : Nat) (value : Option Nat)
  | commit | abort | marker
  deriving DecidableEq, Repr
structure Entry where
  owner : Nat
  action : Action
  deriving DecidableEq, Repr
def header (r : Entry) : Rec :=
  ⟨r.owner,0,match r.action with | .commit => .commit | _ => .write⟩
def winners (log : List Entry) := owners (log.map header)
def apply (known : List Nat) (k : Nat) (s : Option Nat) (r : Entry) :=
  match r.action with
  | .put key value => if key == k && known.contains r.owner then value else s
  | _ => s
def replay (log : List Entry) (k : Nat) (base : Option Nat) :=
  log.foldl (apply (winners log) k) base
def edits (known : List Nat) (k : Nat) (log : List Entry) : List (Option Nat) :=
  log.filterMap (fun r => match r.action with
    | .put key v => if key == k && known.contains r.owner then some v else none
    | _ => none)

/-- Independently scanning segment headers for winners still replays cross-segment transactions in physical WAL order. -/
def SegmentedWinnerReplayAgreement.Goal : Prop := ∀ (segs : List (List Entry)) k base,
  segs.foldl (fun s seg => seg.foldl (apply (segs.flatMap winners) k) s) base = replay segs.flatten k base

/-- A recovered lookup is the last committed after-image for that key; a final delete shadows earlier inserts. -/
def ReplayLastCommittedImage.Goal : Prop := ∀ log k base,
  replay log k base = ((edits (winners log) k log).getLast?).getD base

/-- A crash before a transaction's commit cannot expose any of its tentative writes, even if they straddle segment cuts. -/
def RemoveCrashLoserWithoutEffect.Goal : Prop := ∀ log k base owner, owner ∉ winners log →
  replay (log.filter (fun r => r.owner != owner)) k base = replay log k base

/-- A checkpoint plus tail replay equals replay from the beginning when no owner of a prefix edit first commits in the tail. -/
def ClosedCheckpointCutReplay.Goal : Prop := ∀ pre post k base,
  (∀ owner key v, Entry.mk owner (.put key v) ∈ pre → owner ∈ winners post → owner ∈ winners pre) →
  (∀ owner key v, Entry.mk owner (.put key v) ∈ post → owner ∈ winners pre → owner ∈ winners post) →
  replay post k (replay pre k base) = replay (pre++post) k base

/-- Restarting twice from the same after-image log cannot apply a logical replacement or tombstone twice. -/
def RestartAfterImageIdempotence.Goal : Prop := ∀ log k base,
  replay log k (replay log k base) = replay log k base

/-- Rearrangements preserving a key's eligible edit order and multiplicity are invisible to a reader of that key. -/
def ReorderedSegmentsPerKeyCriterion.Goal : Prop := ∀ a b k base, a.Perm b →
  edits (winners a) k a = edits (winners b) k b → replay a k base = replay b k base

/-- Seeding the next transaction id must cover losers too, while checkpoint markers do not introduce a transaction id. -/
def RestartAllocatorAboveLosers.Goal : Prop := ∀ (log : List Entry) r, r ∈ log → r.action ≠ .marker →
  r.owner < (((log.filter (fun e => e.action != .marker)).map Entry.owner).max?.getD 0)+1

#guard replay [⟨1,.put 0 (some 4)⟩,⟨2,.put 0 none⟩,⟨1,.commit⟩] 0 none == some 4

end TinylambOpt.ResearchGoals.DatabaseCorrectness.WinnerRecovery
