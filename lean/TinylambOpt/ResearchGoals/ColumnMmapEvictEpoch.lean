import Std

namespace TinylambOpt.ResearchGoals.ColumnMmapEvictEpoch

structure Entry where
  id : Nat
  pinned : Nat
  lruTick : Nat
  bytes : Nat
  resident : Bool
  deriving DecidableEq

def residentBytes (xs : List Entry) : Nat :=
  (xs.filter Entry.resident).map Entry.bytes |>.sum

def earlierVictim (a b : Entry) : Bool :=
  a.lruTick < b.lruTick || (a.lruTick == b.lruTick && a.id < b.id)

def pickVictim (xs : List Entry) : Option Entry :=
  xs.foldl
    (fun best e =>
      if !(e.resident && e.pinned == 0) then best
      else
        match best with
        | none => some e
        | some b => if earlierVictim e b then some e else some b)
    none

def evictOne (vid : Nat) (xs : List Entry) : List Entry :=
  xs.map fun e => if e.id == vid then { e with resident := false } else e

structure EvictResult where
  entries : List Entry
  reclaimed : Nat
  deriving DecidableEq

def evictToTarget (target : Nat) (xs : List Entry) : EvictResult :=
  let rec go (fuel : Nat) (cur : List Entry) (acc : Nat) : EvictResult :=
    match fuel with
    | 0 => ⟨cur, acc⟩
    | f + 1 =>
        if target ≤ acc then ⟨cur, acc⟩
        else
          match pickVictim cur with
          | none => ⟨cur, acc⟩
          | some v => go f (evictOne v.id cur) (acc + v.bytes)
  go xs.length xs 0

def lruLe (a b : Entry) : Prop :=
  a.lruTick < b.lruTick ∨ (a.lruTick = b.lruTick ∧ a.id ≤ b.id)

/-- Pinned-block-safe LRU reclamation (`table/tlcs_file.cpp`
    `EvictDecodedCacheForReclaim` / `EvictIdleColumnarHeadroom`): evicts the
    oldest unpinned resident blocks until `target` bytes are reclaimed without
    ever touching pinned blocks. -/
def Goal : Prop :=
  ∀ (target : Nat) (entries : List Entry),
    (entries.map Entry.id).Nodup →
      let res := evictToTarget target entries
      res.entries.length = entries.length ∧
      (∀ i : Nat, ∀ e e' : Entry,
        entries[i]? = some e → res.entries[i]? = some e' →
          e'.id = e.id ∧ e'.pinned = e.pinned ∧ e'.lruTick = e.lruTick ∧
          e'.bytes = e.bytes ∧ (0 < e.pinned → e'.resident = e.resident)) ∧
      res.reclaimed + residentBytes res.entries = residentBytes entries ∧
      (target ≤ res.reclaimed ∨ ∀ e' ∈ res.entries, e'.resident = true → 0 < e'.pinned) ∧
      (∀ e e' rem : Entry,
        e ∈ entries → e' ∈ res.entries → rem ∈ res.entries →
          e.id = e'.id → e.resident = true → e'.resident = false →
          rem.resident = true → rem.pinned = 0 → lruLe e rem)

private def e1 : Entry := ⟨1, 1, 1, 100, true⟩
private def e2 : Entry := ⟨2, 0, 5, 40, true⟩
private def e3 : Entry := ⟨3, 0, 2, 50, true⟩
private def e4 : Entry := ⟨4, 0, 1, 80, false⟩

example :
    evictToTarget 60 [e1, e2, e3, e4] =
      ⟨[e1, { e2 with resident := false }, { e3 with resident := false }, e4], 90⟩ := by
  decide

example :
    evictToTarget 500 [e1, e2, e3, e4] =
      ⟨[e1, { e2 with resident := false }, { e3 with resident := false }, e4], 90⟩ := by
  decide

end TinylambOpt.ResearchGoals.ColumnMmapEvictEpoch
