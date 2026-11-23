/-
  TinylambOpt/SnapshotZoneMap.lean

  Zone-map block skipping in an MVCC snapshot that has visible update deltas.

  Abstract semantics (stated explicitly):
    * a version is `(key, value, commit timestamp)`;
    * the base block has one generation `base.gen`; every base version has
      `ts = base.gen`; each delta batch has its own generation `gen` and a
      list of versions (updates);
    * the MVCC reference scan keeps versions with `ts <= snap` and, for each
      key, the last visible one in list order (newer versions are appended
      later).  This is exactly the fold-based `referenceScan` below;
    * a zone summary carries its generation plus an inclusive [lo, hi] range.
      A missing delta summary is represented by `none` and forces a read.

  Soundness hypotheses (sufficient):
    (H1) the base summary covers every base version (value in [lo, hi] and
         ts equal to the base generation);
    (H2) every delta that has a summary covers all of its versions;
    (H3) `snapshotSkip` requires: base generation visible, base summary
         excludes the predicate point, and EVERY visible delta either has a
         higher generation (hence invisible) or an excluding summary.
         A delta with no summary makes `snapshotSkip` false (full read).

  `snapshot_zone_map_sound` proves that under H1-H3 no visible row of the
  block satisfies `x = c`, so skipping agrees with the reference scan.
  The boundary theorem shows that base statistics alone are unsound when a
  visible delta carries the matching value.

  C++ realization: table/full_scan_iterator.cpp and executor/zone_map.cpp.
  The skip guard must consult the summary attached to each visible delta
  generation; missing/unknown summaries must take the "read it" path.
-/

namespace TinylambOpt.SnapshotZoneMap

/-- Inclusive range summary with the generation it describes. -/
structure ZoneSummary where
  gen : Nat
  lo : Int
  hi : Int
  deriving DecidableEq, Repr

/-- One physical row version. -/
structure Version where
  key : Nat
  val : Int
  ts : Nat
  deriving DecidableEq, Repr

/-- A delta batch: its generation, its versions, and an optional summary. -/
structure DeltaBatch where
  gen : Nat
  versions : List Version
  summary : Option ZoneSummary
  deriving DecidableEq, Repr

/-- All versions visible at `snap`, in evaluation order: base first, then
    the versions of every visible delta batch. -/
def visibleVersions (snap : Nat) (base : List Version)
    (deltas : List DeltaBatch) : List Version :=
  (base ++ (deltas.filter (fun d => d.gen ≤ snap)).flatMap (fun d => d.versions))
    |>.filter (fun v => v.ts ≤ snap)

/-- One fold step of the reference scan: the new (visible) version replaces
    the previous value of the same key. -/
def latestStep (acc : List (Nat × Int)) (v : Version) : List (Nat × Int) :=
  (acc.filter (fun p : Nat × Int => p.1 != v.key)) ++ [(v.key, v.val)]

/-- MVCC reference scan result: latest visible (key, value) pairs. -/
def referenceScan (snap : Nat) (base : List Version)
    (deltas : List DeltaBatch) : List (Nat × Int) :=
  (visibleVersions snap base deltas).foldl latestStep []

/-- Helper: every pair produced by the fold originates from one of the
    folded versions. -/
theorem latestStep_mem {kv : Nat × Int} (vs : List Version) (acc : List (Nat × Int))
    (h : kv ∈ vs.foldl latestStep acc) :
    kv ∈ acc ∨ ∃ v ∈ vs, kv.2 = v.val := by
  induction vs generalizing acc with
  | nil =>
      simpa using h
  | cons v vs ih =>
      have h' := ih ((acc.filter (fun p : Nat × Int => p.1 != v.key)) ++ [(v.key, v.val)]) h
      rcases h' with hmem | hex
      · simp only [List.mem_append, List.mem_filter, List.mem_singleton] at hmem
        rcases hmem with ⟨hacc, _⟩ | heq
        · exact Or.inl hacc
        · right
          refine ⟨v, by simp, ?_⟩
          simp [heq]
      · rcases hex with ⟨w, hw, hval⟩
        exact Or.inr ⟨w, List.Mem.tail v hw, hval⟩

/-- Every reference-scan pair comes from a visible version. -/
theorem referenceScan_mem (snap : Nat) (base : List Version) (deltas : List DeltaBatch)
    {kv : Nat × Int} (h : kv ∈ referenceScan snap base deltas) :
    ∃ v ∈ visibleVersions snap base deltas, kv.2 = v.val := by
  unfold referenceScan at h
  have h' := latestStep_mem (visibleVersions snap base deltas) [] h
  rcases h' with hnil | hex
  · simp at hnil
  · exact hex

/-- Every visible version is either a base version or belongs to a delta
    whose generation and timestamp are visible. -/
theorem visibleVersions_mem (snap : Nat) (base : List Version) (deltas : List DeltaBatch)
    {v : Version} (h : v ∈ visibleVersions snap base deltas) :
    (v ∈ base ∧ v.ts ≤ snap) ∨
    ∃ d ∈ deltas, d.gen ≤ snap ∧ v ∈ d.versions ∧ v.ts ≤ snap := by
  unfold visibleVersions at h
  simp only [List.mem_filter, List.mem_append, List.mem_flatMap] at h
  rcases h with ⟨hsrc, hts⟩
  rcases hsrc with hbase | hdelta
  · exact Or.inl ⟨hbase, by simpa using hts⟩
  · rcases hdelta with ⟨d, ⟨hdmem, hdgen⟩, hvver⟩
    exact Or.inr ⟨d, hdmem, by simpa using hdgen, hvver, by simpa using hts⟩

/-- Range-exclusion lemma (the page-interval exclusion primitive). -/
theorem zone_excludes_point (lo hi c v : Int)
    (h : c < lo ∨ hi < c) (hl : lo ≤ v) (hv : v ≤ hi) : v ≠ c := by
  omega

/-- Skip decision for a point predicate `x = c`.  A `none` delta summary
    never allows skipping: the delta must be read in full. -/
def snapshotSkip (snap : Nat) (c : Int) (bs : ZoneSummary)
    (deltas : List DeltaBatch) : Bool :=
  decide (bs.gen ≤ snap) &&
  decide (c < bs.lo ∨ bs.hi < c) &&
  deltas.all (fun d =>
    decide (d.gen > snap) ||
    match d.summary with
    | some s => decide (c < s.lo ∨ s.hi < c)
    | none => false)

/-- MAIN: after the skip decision, no row of the reference scan matches
    `x = c`; therefore skipping the block preserves the MVCC scan result. -/
theorem visible_values_excluded (snap : Nat) (c : Int) (base : List Version)
    (deltas : List DeltaBatch) (bs : ZoneSummary)
    (hbase : ∀ v ∈ base, v.ts = bs.gen ∧ bs.lo ≤ v.val ∧ v.val ≤ bs.hi)
    (hdelta : ∀ d ∈ deltas, ∀ v ∈ d.versions,
        match d.summary with
        | some s => s.lo ≤ v.val ∧ v.val ≤ s.hi
        | none => True)
    (hskip : snapshotSkip snap c bs deltas = true) :
    ∀ v ∈ visibleVersions snap base deltas, v.val ≠ c := by
  simp [snapshotSkip] at hskip
  intro v hvvis hc
  have hvsrc := visibleVersions_mem snap base deltas hvvis
  rcases hvsrc with hbaseSrc | hdeltaSrc
  · rcases hbaseSrc with ⟨hvbase, hvts⟩
    have hbs := hbase v hvbase
    have hexcl : c < bs.lo ∨ bs.hi < c := hskip.1.2
    have hz : v.val ≠ c := zone_excludes_point bs.lo bs.hi c v.val hexcl hbs.2.1 hbs.2.2
    exact hz (by omega)
  · rcases hdeltaSrc with ⟨d, hdmem, hdgen, hvver, hvts⟩
    have hds := hdelta d hdmem v hvver
    have hdskip := hskip.2 d hdmem
    have hnot : ¬ snap < d.gen := by omega
    simp [hnot] at hdskip
    cases hs : d.summary with
    | none =>
        simp [hs] at hdskip
    | some s =>
        simp [hs] at hds hdskip
        have hz : v.val ≠ c := zone_excludes_point s.lo s.hi c v.val hdskip hds.1 hds.2
        exact hz (by omega)

theorem snapshot_zone_map_sound (snap : Nat) (c : Int) (base : List Version)
    (deltas : List DeltaBatch) (bs : ZoneSummary)
    (hbase : ∀ v ∈ base, v.ts = bs.gen ∧ bs.lo ≤ v.val ∧ v.val ≤ bs.hi)
    (hdelta : ∀ d ∈ deltas, ∀ v ∈ d.versions,
        match d.summary with
        | some s => s.lo ≤ v.val ∧ v.val ≤ s.hi
        | none => True)
    (hskip : snapshotSkip snap c bs deltas = true) :
    ∀ kv ∈ referenceScan snap base deltas, kv.2 ≠ c := by
  intro kv hkv
  obtain ⟨v,hv,eq⟩ := referenceScan_mem snap base deltas hkv
  rw [eq]
  exact visible_values_excluded snap c base deltas bs hbase hdelta hskip v hv

/-- Select the greatest visible commit timestamp per key, including delete
    records. A tombstone hides earlier values. Equal timestamps use stable
    physical order; normal committed histories have unique per-key stamps. -/
def readKeyAt (snap key : Nat) (base : List Version) (ds : List DeltaBatch)
    (deleted : Version → Bool) : Option Int := do
  let ordered := (visibleVersions snap base ds).mergeSort (fun a b => a.ts ≥ b.ts)
  let v ← ordered.find? (fun v => v.key == key)
  if deleted v then none else some v.val

theorem snapshot_skip_with_deletes (snap : Nat) (c : Int) (base : List Version)
    (deltas : List DeltaBatch) (bs : ZoneSummary)
    (hbase : ∀ v ∈ base, v.ts = bs.gen ∧ bs.lo ≤ v.val ∧ v.val ≤ bs.hi)
    (hdelta : ∀ d ∈ deltas, ∀ v ∈ d.versions,
        match d.summary with
        | some s => s.lo ≤ v.val ∧ v.val ≤ s.hi
        | none => True)
    (hskip : snapshotSkip snap c bs deltas = true) :
    ∀ k deleted, readKeyAt snap k base deltas deleted ≠ some c := by
  intro k deleted h
  unfold readKeyAt at h
  dsimp only at h
  generalize he : ((visibleVersions snap base deltas).mergeSort
    (fun a b => decide (a.ts ≥ b.ts))).find? (fun v => v.key == k) = opt at h
  cases opt with
  | none => contradiction
  | some v =>
    have hm := List.mem_of_find?_eq_some he
    simp at hm
    cases hd : deleted v <;> simp [hd] at h
    exact visible_values_excluded snap c base deltas bs hbase hdelta hskip v hm h

/-- Boundary example: base values are all 1, but a visible delta contains
    99.  Base-only statistics would skip the block and lose the match;
    the full guard with the delta summary refuses to skip. -/
theorem base_only_summary_loses_visible_update :
    let base : List Version := [{ key := 1, val := 1, ts := 1 }]
    let d : DeltaBatch := {
      gen := 2
      versions := [{ key := 1, val := 99, ts := 2 }]
      summary := some { gen := 2, lo := 99, hi := 99 }
    }
    let bs : ZoneSummary := { gen := 1, lo := 1, hi := 1 }
    decide (99 < bs.lo ∨ bs.hi < 99) = true ∧
    99 ∈ (referenceScan 5 base [d]).map (fun kv => kv.2) ∧
    snapshotSkip 5 99 bs [d] = false := by
  decide

/-- Missing summary on a visible delta forces the full-read path. -/
theorem missing_summary_forces_read :
    let d : DeltaBatch := {
      gen := 2
      versions := [{ key := 1, val := 99, ts := 2 }]
      summary := none
    }
    let bs : ZoneSummary := { gen := 1, lo := 1, hi := 1 }
    snapshotSkip 5 99 bs [d] = false := by
  decide

/-- A delta of a future generation is invisible and does not block skip. -/
theorem future_delta_does_not_block_skip :
    let d : DeltaBatch := {
      gen := 6
      versions := [{ key := 1, val := 99, ts := 6 }]
      summary := none
    }
    let bs : ZoneSummary := { gen := 1, lo := 1, hi := 1 }
    snapshotSkip 5 99 bs [d] = true := by
  decide

example :
    let xs : List Version := [⟨1,90,9⟩,⟨1,20,2⟩,⟨1,0,6⟩]
    let deleted := fun v : Version => v.ts == 6
    readKeyAt 5 1 xs [] deleted = some 20 ∧ readKeyAt 7 1 xs [] deleted = none ∧
      readKeyAt 10 1 xs [] deleted = some 90 := by cbv

end TinylambOpt.SnapshotZoneMap
