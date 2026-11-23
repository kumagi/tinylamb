import Std.Tactic

/-! Pinned-version compaction and late gather
    (C++ counterpart: `table/full_scan_iterator.cpp`).

    A physical heap stores versions (key, value, RID, birth timestamp,
    tombstone flag, generation).  Each version chain is kept newest-first, so
    `visible` is the first element born at or before a snapshot's timestamp.
    Several readers register snapshots and pin RIDs; inserts and deletes append
    versions with `born = clock + 1`, hence they are invisible to every
    registered snapshot.  Compaction below the visibility watermark renumbers
    physical slots but must never reuse a pinned RID; pinned versions keep
    their RID and generation, unpinned ones move to fresh slots.

    `save` captures the eagerly resolved version together with the key and
    timestamp.  `gather` revalidates the saved version by identity; on a stale
    (missing) version it re-resolves by key and timestamp.  Eager and late
    results agree for every operation sequence, and `unmatched` is never
    confused with a delete tombstone.

    The regression is a delete plus same-key reinsert while an old reader is
    pinned: reusing the old RID for the new physical slot swaps the value under
    a naive RID-only gather.  A `Reclaimable` predicate characterises when a
    version may be collected after the last pin is released.
-/

namespace TinylambOpt.Research.MvccCompactionPin

structure Ver where
  key : Nat
  val : Nat
  rid : Nat
  born : Nat
  deleted : Bool
  gen : Nat
  deriving DecidableEq, Repr

structure Reader where
  ts : Nat
  pins : List Nat
  deriving DecidableEq, Repr

structure Store where
  heap : List Ver
  readers : List Reader
  nextRid : Nat
  nextGen : Nat
  clock : Nat

/-- Visibility predicate for a snapshot timestamp. -/
def visPred (key ts : Nat) (v : Ver) : Bool := decide (v.key = key) && decide (v.born ≤ ts)

/-- The latest version of `key` visible at timestamp `ts`. -/
def visible (heap : List Ver) (key ts : Nat) : Option Ver :=
  heap.find? (visPred key ts)

/-- A version's value contents, independent of its physical slot. -/
def core (v : Ver) : Nat × Nat × Bool × Nat := (v.key, v.val, v.deleted, v.gen)

def visibleCore (heap : List Ver) (key ts : Nat) : Option (Nat × Nat × Bool × Nat) :=
  (visible heap key ts).map core

inductive Lookup where
  | unmatched
  | deleted
  | value (v : Nat)
  deriving DecidableEq, Repr

def resultOf (v : Ver) : Lookup := if v.deleted then Lookup.deleted else Lookup.value v.val

def coreLookup : Option (Nat × Nat × Bool × Nat) → Lookup
  | some (_, val, del, _) => if del then Lookup.deleted else Lookup.value val
  | none => Lookup.unmatched

def lookup (heap : List Ver) (key ts : Nat) : Lookup :=
  coreLookup (visibleCore heap key ts)

def byRid (heap : List Ver) (rid : Nat) : Option Ver :=
  heap.find? (fun v => v.rid = rid)

def pinnedRids : List Reader → List Nat
  | [] => []
  | r :: rs => r.pins ++ pinnedRids rs

theorem pinnedRids_mem {r : Reader} {readers : List Reader} {rid : Nat}
    (hr : r ∈ readers) (hp : rid ∈ r.pins) : rid ∈ pinnedRids readers := by
  induction readers generalizing r with
  | nil => simp at hr
  | cons x xs ih =>
      simp [pinnedRids] at hr ⊢
      rcases hr with rfl | hr
      · exact Or.inl hp
      · exact Or.inr (ih hr hp)


/-! ## Renumbering during compaction -/

def renumber (pinned : List Nat) (start : Nat) : List Ver → List Ver × Nat
  | [] => ([], start)
  | v :: vs =>
      if v.rid ∈ pinned then
        let r := renumber pinned start vs
        (v :: r.1, r.2)
      else
        let r := renumber pinned (start + 1) vs
        ({ v with rid := start } :: r.1, r.2)

theorem renumber_find_core (pinned : List Nat) (start : Nat) (heap : List Ver)
    (key ts : Nat) :
    ((renumber pinned start heap).1.find? (visPred key ts)).map core =
      (heap.find? (visPred key ts)).map core := by
  induction heap generalizing start with
  | nil => simp [renumber]
  | cons v vs ih =>
      by_cases hp : v.rid ∈ pinned
      · by_cases hvk : v.key = key
        · subst key
          by_cases hvb : v.born ≤ ts
          · simp [renumber, hp, visPred, hvb, core]
          · simpa [renumber, hp, visPred, hvb] using ih start
        · simpa [renumber, hp, visPred, hvk] using ih start
      · by_cases hvk : v.key = key
        · subst key
          by_cases hvb : v.born ≤ ts
          · simp [renumber, hp, visPred, hvb, core]
          · simpa [renumber, hp, visPred, hvb] using ih (start + 1)
        · simpa [renumber, hp, visPred, hvk] using ih (start + 1)

theorem visibleCore_renumber (pinned : List Nat) (start : Nat) (heap : List Ver)
    (key ts : Nat) :
    visibleCore (renumber pinned start heap).1 key ts = visibleCore heap key ts := by
  unfold visibleCore visible
  exact renumber_find_core pinned start heap key ts

/-! ## Operations -/

def minTs (readers : List Reader) (default : Nat) : Nat :=
  (readers.map (fun r => r.ts)).foldl Nat.min default

def compact (s : Store) (w : Nat) : Store :=
  let pinned := pinnedRids s.readers
  if w ≤ minTs s.readers s.clock then
    let r := renumber pinned s.nextRid s.heap
    { s with heap := r.1, nextRid := r.2 }
  else s

theorem lookup_compact (s : Store) (w key ts : Nat)
    (hw : w ≤ minTs s.readers s.clock) :
    lookup (compact s w).heap key ts = lookup s.heap key ts := by
  dsimp [lookup]
  rw [show visibleCore (compact s w).heap key ts = visibleCore s.heap key ts by
    simp [compact, hw, visibleCore_renumber]]

def appendVer (key val : Nat) (deleted : Bool) (s : Store) : Store :=
  let v := { key := key, val := val, rid := s.nextRid, born := s.clock + 1, deleted := deleted, gen := s.nextGen }
  { s with heap := v :: s.heap, nextRid := s.nextRid + 1, nextGen := s.nextGen + 1, clock := s.clock + 1 }

def updAt {α : Type} (i : Nat) (f : α → α) : List α → List α
  | [] => []
  | x :: xs => if i = 0 then f x :: xs else x :: updAt (i - 1) f xs

def pin (idx rid : Nat) (s : Store) : Store :=
  { s with readers := updAt idx (fun r => { r with pins := rid :: r.pins }) s.readers }

def unpin (idx rid : Nat) (s : Store) : Store :=
  { s with readers := updAt idx (fun r => { r with pins := r.pins.filter (fun x => x != rid) }) s.readers }

inductive Step where
  | append (key val : Nat) (deleted : Bool)
  | pin (idx rid : Nat)
  | unpin (idx rid : Nat)
  | compact (w : Nat)

def applyStep (s : Store) : Step → Store
  | Step.append k v d => appendVer k v d s
  | Step.pin i r => pin i r s
  | Step.unpin i r => unpin i r s
  | Step.compact w => compact s w

def run : List Step → Store → Store
  | [], s => s
  | t :: ts, s => run ts (applyStep s t)

/-! ## Invariants preserved by the operations -/

theorem all_readers_ts_updAt (i : Nat) (f : Reader → Reader)
    (hf : ∀ r, (f r).ts = r.ts) (c : Nat) :
    ∀ (rs : List Reader), (∀ r ∈ rs, r.ts ≤ c) →
      ∀ r, r ∈ updAt i f rs → r.ts ≤ c := by
  intro rs
  induction rs generalizing i with
  | nil => intro h r hr; simp [updAt] at hr
  | cons x xs ih =>
      intro h r hr
      by_cases hi : i = 0
      · subst i
        simp [updAt] at hr
        rcases hr with rfl | hr
        · rw [hf]; exact h x (by simp)
        · exact h r (by simp [hr])
      · simp [updAt, hi] at hr
        rcases hr with hrx | hr
        · subst r
          exact h x (by simp)
        · exact ih (i := i - 1) (fun y hy => h y (by simp [hy])) r hr

theorem exists_reader_ts_updAt (i : Nat) (f : Reader → Reader)
    (hf : ∀ r, (f r).ts = r.ts) :
    ∀ (rs : List Reader) (ts : Nat),
      (∃ r ∈ rs, r.ts = ts) → ∃ r ∈ updAt i f rs, r.ts = ts := by
  intro rs
  induction rs generalizing i with
  | nil => intro ts h; simp at h
  | cons x xs ih =>
      intro ts h
      rcases h with ⟨r, hr, hts⟩
      cases hr with
      | head =>
          by_cases hi : i = 0
          · subst i
            refine ⟨f x, ?_, ?_⟩
            · simp [updAt]
            · rw [hf]; exact hts
          · refine ⟨x, ?_, hts⟩
            simp [updAt, hi]
      | tail _ hr =>
          by_cases hi : i = 0
          · subst i
            refine ⟨r, List.mem_cons_of_mem (f x) hr, hts⟩
          · rcases ih (i := i - 1) ts ⟨r, hr, hts⟩ with ⟨w, hw, hwt⟩
            refine ⟨w, ?_, hwt⟩
            simpa [updAt, hi] using List.mem_cons_of_mem x hw

theorem clock_monotone_step (s : Store) (st : Step)
    (h : ∀ r ∈ s.readers, r.ts ≤ s.clock) :
    ∀ r ∈ (applyStep s st).readers, r.ts ≤ (applyStep s st).clock := by
  cases st with
  | append k v d =>
      intro r hr
      simp [applyStep, appendVer] at hr ⊢
      have := h r hr
      omega
  | pin i rid =>
      simpa [applyStep, pin] using
        (all_readers_ts_updAt i (fun r : Reader => { r with pins := rid :: r.pins })
          (by intro r; rfl) s.clock s.readers h)
  | unpin i rid =>
      simpa [applyStep, unpin] using
        (all_readers_ts_updAt i
          (fun r : Reader => { r with pins := r.pins.filter (fun x => x != rid) })
          (by intro r; rfl) s.clock s.readers h)
  | compact w =>
      by_cases hw : w ≤ minTs s.readers s.clock
      · simpa [applyStep, compact, hw] using h
      · simpa [applyStep, compact, hw] using h

theorem reader_ts_survives_step (s : Store) (st : Step) (ts : Nat)
    (h : ∃ r ∈ s.readers, r.ts = ts) :
    ∃ r ∈ (applyStep s st).readers, r.ts = ts := by
  cases st with
  | append k v d => simpa [applyStep, appendVer] using h
  | pin i rid =>
      simpa [applyStep, pin] using exists_reader_ts_updAt i
        (fun r : Reader => { r with pins := rid :: r.pins }) (by intro r; rfl)
        s.readers ts h
  | unpin i rid =>
      simpa [applyStep, unpin] using exists_reader_ts_updAt i
        (fun r : Reader => { r with pins := r.pins.filter (fun x => x != rid) })
        (by intro r; rfl) s.readers ts h
  | compact w =>
      by_cases hw : w ≤ minTs s.readers s.clock
      · simpa [applyStep, compact, hw] using h
      · simpa [applyStep, compact, hw] using h

theorem lookup_step_stable (s : Store) (st : Step) (key ts : Nat)
    (hclk : ∀ r ∈ s.readers, r.ts ≤ s.clock)
    (hreq : ∃ r ∈ s.readers, r.ts = ts) :
    lookup (applyStep s st).heap key ts = lookup s.heap key ts := by
  cases st with
  | append k v d =>
      have hborn : ¬ (s.clock + 1 ≤ ts) := by
        rcases hreq with ⟨r, hr, rfl⟩
        have hle := hclk r hr
        omega
      simp only [applyStep]
      dsimp [lookup]
      rw [show visibleCore (appendVer k v d s).heap key ts = visibleCore s.heap key ts by
        unfold visibleCore visible
        simp [appendVer, visPred, hborn]]
  | pin i rid => rfl
  | unpin i rid => rfl
  | compact w =>
      by_cases hw : w ≤ minTs s.readers s.clock
      · simpa [applyStep] using lookup_compact s w key ts hw
      · simp [applyStep, compact, hw]

theorem run_invariant (s₀ : Store) (steps : List Step) (key ts : Nat)
    (hclk : ∀ r ∈ s₀.readers, r.ts ≤ s₀.clock)
    (hreq : ∃ r ∈ s₀.readers, r.ts = ts) :
    (∀ r ∈ (run steps s₀).readers, r.ts ≤ (run steps s₀).clock) ∧
    (∃ r ∈ (run steps s₀).readers, r.ts = ts) ∧
    lookup (run steps s₀).heap key ts = lookup s₀.heap key ts := by
  induction steps generalizing s₀ with
  | nil => exact ⟨hclk, hreq, rfl⟩
  | cons st rest ih =>
      have hs1 := clock_monotone_step s₀ st hclk
      have hs2 := reader_ts_survives_step s₀ st ts hreq
      have hs3 := lookup_step_stable s₀ st key ts hclk hreq
      have tail := ih (applyStep s₀ st) hs1 hs2
      exact ⟨tail.1, tail.2.1, tail.2.2.trans hs3⟩

theorem lookup_run_stable (s₀ : Store) (steps : List Step) (key ts : Nat)
    (hclk : ∀ r ∈ s₀.readers, r.ts ≤ s₀.clock)
    (hreq : ∃ r ∈ s₀.readers, r.ts = ts) :
    lookup (run steps s₀).heap key ts = lookup s₀.heap key ts :=
  (run_invariant s₀ steps key ts hclk hreq).2.2

/-! ## Eager save, revalidating gather, and the main theorem -/

abbrev SavedReq := Nat × Nat × Option Ver

def save (s : Store) (reqs : List (Nat × Nat)) : List SavedReq :=
  reqs.map (fun p => (p.1, p.2, visible s.heap p.1 p.2))

def eager (s : Store) (reqs : List (Nat × Nat)) : List Lookup :=
  reqs.map (fun p => lookup s.heap p.1 p.2)

/-- Revalidation on a stale read: accept the eagerly captured version only if
    it is still physically present; otherwise re-resolve by key and timestamp. -/
def gather (s : Store) (saved : List SavedReq) : List Lookup :=
  saved.map (fun p =>
    match p.2.2 with
    | none => lookup s.heap p.1 p.2.1
    | some v => if decide (v ∈ s.heap) then resultOf v else lookup s.heap p.1 p.2.1)

theorem gather_save_eq_eager (s₀ s : Store) (reqs : List (Nat × Nat))
    (hstable : ∀ p ∈ reqs, lookup s.heap p.1 p.2 = lookup s₀.heap p.1 p.2) :
    gather s (save s₀ reqs) = eager s₀ reqs := by
  simp only [gather, save, eager, List.map_map]
  apply List.map_congr_left
  intro p hp
  dsimp
  have hst := hstable p hp
  cases hv : visible s₀.heap p.1 p.2 with
  | none =>
      simpa [hv] using hst
  | some v =>
      have horig : lookup s₀.heap p.1 p.2 = resultOf v := by
        simp [lookup, coreLookup, visibleCore, core, hv, resultOf]
      by_cases hmem : v ∈ s.heap
      · simpa [hv, hmem] using horig.symm
      · simpa [hv, hmem] using hst

/-- **MAIN THEOREM.** Eager and late gather agree for every valid operation
    sequence: values, unmatched rows and tombstones are preserved. -/
theorem mvcc_compaction_pin_sound (s₀ : Store) (steps : List Step)
    (reqs : List (Nat × Nat))
    (hclk : ∀ r ∈ s₀.readers, r.ts ≤ s₀.clock)
    (hreq : ∀ p ∈ reqs, ∃ r ∈ s₀.readers, r.ts = p.2) :
    gather (run steps s₀) (save s₀ reqs) = eager s₀ reqs := by
  apply gather_save_eq_eager
  intro p hp
  exact lookup_run_stable s₀ steps p.1 p.2 hclk (hreq p hp)

/-! ## Naive RID-only gather and the slot-reuse regression -/

def gatherNaive (s : Store) (saved : List SavedReq) : List Lookup :=
  saved.map (fun p =>
    match p.2.2 with
    | none => lookup s.heap p.1 p.2.1
    | some v =>
        match byRid s.heap v.rid with
        | some v' => resultOf v'
        | none => Lookup.unmatched)

def oldV : Ver := { key := 1, val := 100, rid := 7, born := 0, deleted := false, gen := 0 }
def newV : Ver := { key := 1, val := 200, rid := 8, born := 3, deleted := false, gen := 2 }
def reusedV : Ver := { key := 1, val := 200, rid := 7, born := 3, deleted := false, gen := 2 }
def tombV : Ver := { key := 2, val := 0, rid := 9, born := 1, deleted := true, gen := 1 }

def oldReader : Reader := { ts := 1, pins := [7] }

def oldStore : Store :=
  { heap := [oldV], readers := [oldReader], nextRid := 8, nextGen := 1, clock := 1 }

def goodStore : Store :=
  { heap := [newV, oldV], readers := [oldReader], nextRid := 9, nextGen := 3, clock := 3 }

def badStore : Store :=
  { heap := [reusedV], readers := [oldReader], nextRid := 9, nextGen := 3, clock := 3 }

/-- Reusing the pinned old RID for the new physical slot swaps the value under
    a naive RID-only gather; identity revalidation rejects the stale reference. -/
theorem rid_reuse_counterexample :
    gather goodStore (save oldStore [(1, 1)]) = [Lookup.value 100] ∧
    gatherNaive badStore (save oldStore [(1, 1)]) = [Lookup.value 200] ∧
    gather badStore (save oldStore [(1, 1)]) = eager badStore [(1, 1)] := by
  native_decide

/-- A delete tombstone and an unmatched key are different observations. -/
theorem tombstone_vs_unmatched_counterexample :
    lookup [tombV] 2 1 = Lookup.deleted ∧
    lookup [tombV] 3 1 = Lookup.unmatched ∧
    lookup [tombV] 2 1 ≠ lookup [tombV] 3 1 := by
  native_decide

/-- A pinned version is never renumbered: its RID and generation survive. -/
theorem pin_prevents_rid_reuse :
    let r := renumber (pinnedRids [oldReader]) 8 [oldV, newV]
    byRid r.1 7 = some oldV ∧ r.2 = 9 := by
  native_decide

/-! ## Characterising when a version is reclaimable -/

def visibleAt (heap : List Ver) (v : Ver) (ts : Nat) : Prop :=
  match visible heap v.key ts with
  | some u => u.gen = v.gen
  | none => False

/-- A version may be collected when no reader pins its RID and no registered
    snapshot can see it. -/
def Reclaimable (s : Store) (v : Ver) : Prop :=
  v.rid ∉ pinnedRids s.readers ∧ ∀ r ∈ s.readers, ¬ visibleAt s.heap v r.ts

theorem reclaimable_after_last_pin (s : Store) (idx rid : Nat) (v : Ver)
    (hno : rid ∉ pinnedRids (unpin idx rid s).readers)
    (hvis : ∀ r ∈ (unpin idx rid s).readers, ¬ visibleAt s.heap v r.ts)
    (hrid : v.rid = rid) : Reclaimable (unpin idx rid s) v := by
  constructor
  · simpa [hrid] using hno
  · exact hvis

def retiredV : Ver := { key := 3, val := 1, rid := 10, born := 5, deleted := false, gen := 5 }

def retiredStore : Store :=
  { heap := [retiredV], readers := [{ ts := 1, pins := [10] }]
  , nextRid := 11, nextGen := 6, clock := 4 }

/-- A version that is pinned is not reclaimable; once the last pin is released,
    a version invisible to every registered snapshot is reclaimable. -/
theorem reclaimable_requires_no_pin :
    ¬ Reclaimable retiredStore retiredV ∧
    Reclaimable (unpin 0 10 retiredStore) retiredV := by
  constructor
  · intro h
    have hp := h.1
    simp [retiredV, retiredStore, pinnedRids] at hp
  · constructor
    · simp [retiredV, unpin, retiredStore, pinnedRids, updAt]
    · intro r hr
      have hr' : r = { ts := 1, pins := [] } := by
        simpa [unpin, retiredStore, updAt] using hr
      subst r
      simp [visibleAt, retiredStore, retiredV, visible, visPred, unpin, updAt]

end TinylambOpt.Research.MvccCompactionPin
