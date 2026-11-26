import TinylambOpt.Research.AdaptiveRadixReplay

/-!
A token addresses one live batch. Splitting retires it and issues fresh tokens even if a
child has the same radix prefix as its parent.

Modeled claims

- `Goal` — `∀ (rows : List Occ) (cmds : List Cmd), (occIds rows).Nodup → let s :=
  cmds.foldl step (start rows) ((consumedIds s.events ++ s.live.flatMap (fun b =>
  occIds b.pending)).Perm (occIds rows)) ∧ (s.live.map Batch.token).Nodup ∧ (∀ b ∈
  s.live, b.token < s.fresh ∧ ∀ o ∈ b.pending, o.key % 2^b.depth = b.bucket) ∧ (∀ o ∈
  rows, consumptionCount (finish s) o.id = 1) ∧ (∀ k, optAgg rows (finish s) k =
  refAgg rows k)`
-/

namespace TinylambOpt.ResearchGoals.RadixReplayOwnership
open Research.AdaptiveRadixReplay

/-- A token addresses one live batch. Splitting retires it and issues fresh
    tokens even if a child has the same radix prefix as its parent. -/
structure Batch where
  token : Nat
  depth : Nat
  bucket : Nat
  pending : List Occ
  deriving DecidableEq

structure State where
  fresh : Nat
  live : List Batch
  events : List Event
  deriving DecidableEq

inductive Cmd where
  | drain (token budget : Nat)
  | split (token : Nat)
  deriving DecidableEq

def start (rows : List Occ) : State := ⟨1, [⟨0, 0, 0, rows⟩], []⟩

def record (b : Batch) (kind : EKind) (rows : List Occ) : Event :=
  ⟨b.token, b.bucket, occIds rows, kind⟩

def step (s : State) (cmd : Cmd) : State := Id.run do
  let token := match cmd with | .drain t _ | .split t => t
  let some b := s.live.find? (fun b => b.token == token) | return s
  let others := s.live.filter fun j => j.token != token
  match cmd with
  | .drain _ budget =>
    -- Retire the old token even on partial consumption: a redelivered
    -- command must not consume the next prefix on behalf of the old request.
    let tail := b.pending.drop budget
    let live := if tail.isEmpty then others else
      { b with token := s.fresh, pending := tail } :: others
    return ⟨s.fresh + 1, live,
      s.events ++ [record b .part (b.pending.take budget)]⟩
  | .split _ =>
    let (ones, zeros) := b.pending.partition fun o => o.key.testBit b.depth
    let left : Batch := ⟨s.fresh, b.depth+1, b.bucket, zeros⟩
    let right : Batch := ⟨s.fresh+1, b.depth+1, b.bucket + 2^b.depth, ones⟩
    return ⟨s.fresh+2, left :: right :: others, s.events ++ [record b .spill b.pending]⟩

def finish (s : State) : List Event :=
  s.events ++ s.live.map (fun b => record b .sealed b.pending)

def Goal : Prop := ∀ (rows : List Occ) (cmds : List Cmd), (occIds rows).Nodup →
  let s := cmds.foldl step (start rows)
  ((consumedIds s.events ++ s.live.flatMap (fun b => occIds b.pending)).Perm (occIds rows)) ∧
  (s.live.map Batch.token).Nodup ∧
  (∀ b ∈ s.live, b.token < s.fresh ∧ ∀ o ∈ b.pending, o.key % 2^b.depth = b.bucket) ∧
  (∀ o ∈ rows, consumptionCount (finish s) o.id = 1) ∧
  (∀ k, optAgg rows (finish s) k = refAgg rows k)

private def rows : List Occ := [⟨11,0,none⟩, ⟨12,1,some 7⟩, ⟨13,2,some 7⟩]
private def partialRun := [Cmd.drain 0 1, .split 1, .drain 3 1].foldl step (start rows)
example : consumedIds partialRun.events = [11,12] := by decide
example : step partialRun (.split 1) = partialRun := by decide
example : consumedIds (finish partialRun) = [11,12,13] := by decide

end TinylambOpt.ResearchGoals.RadixReplayOwnership
