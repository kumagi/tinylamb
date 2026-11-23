import Std.Tactic

/-!
# Lazy row reads from two indexes at different watermarks

C++ counterpart: `table/full_scan_iterator.cpp`.

The **reference** is a snapshot scan: for every RID it reads the latest
version with `ts ≤ snap` (physical order breaks ties) and yields `(rid, val)`
unless that version is a tombstone.  The scan result is filtered by two
predicates `p1` and `p2`.

The **optimized** plan obtains conservative candidate RIDs from two indexes
with possibly different watermarks, intersects the RID lists, and re-reads
each candidate at the snapshot, keeping only rows satisfying both predicates.
An index candidate may be stale; the union with an update-delta candidate list
restores completeness.  Coverage hypotheses state exactly which visible rows
must appear in an index (when its watermark already covers the row) or in the
delta list (when the row is newer than the watermark).

`snapshot_index_intersect_sound` proves **soundness** (no false results) and
**completeness** (no missing rows) as two separate statements.  A covering
condition under which a candidate set is empty (and hence no re-read is
needed) is given, together with a bound on the number of re-read rows.  The
counterexample is the regression case: two old indexes individually contain
matching values for one RID, but no single version satisfies both predicates,
so an RID-only intersection is a false positive.
-/

namespace TinylambOpt.Research.SnapshotIndexIntersect

structure Version where
  rid : Nat
  val : Int
  ts : Nat
  deleted : Bool
  deriving DecidableEq, Repr, BEq

structure IdxEntry where
  rid : Nat
  val : Int
  deriving DecidableEq, Repr, BEq

structure Index where
  wm : Nat
  entries : List IdxEntry
  deriving DecidableEq, Repr

structure Delta where
  rid : Nat
  val : Int
  ts : Nat
  deriving DecidableEq, Repr, BEq

/-! ## Snapshot scan -/

def visibleVersionAt (snap rid : Nat) (versions : List Version) : Option Version :=
  versions.reverse.find? (fun v => v.rid == rid && decide (v.ts ≤ snap))

theorem find?_satisfies {α : Type} (p : α → Bool) (l : List α) {a : α}
    (h : l.find? p = some a) : p a = true := by
  induction l with
  | nil => simp at h
  | cons x xs ih =>
      by_cases hp : p x = true
      · simp [hp] at h
        subst a
        exact hp
      · simp [hp] at h
        exact ih h

theorem visible_mem_of_some {snap rid : Nat} {versions : List Version} {v : Version}
    (h : visibleVersionAt snap rid versions = some v) :
    v ∈ versions ∧ v.rid = rid ∧ v.ts ≤ snap := by
  unfold visibleVersionAt at h
  have hvrev : v ∈ versions.reverse := List.mem_of_find?_eq_some h
  have hv : v ∈ versions := by simpa using hvrev
  have hp := find?_satisfies (fun w : Version => w.rid == rid && decide (w.ts ≤ snap))
    versions.reverse h
  have hp' : v.rid = rid ∧ v.ts ≤ snap := by simpa using hp
  exact ⟨hv, hp'.1, hp'.2⟩

def readAt (snap rid : Nat) (versions : List Version) : Option Int :=
  match visibleVersionAt snap rid versions with
  | some v => if v.deleted then none else some v.val
  | none => none

theorem visible_of_readAt {snap rid : Nat} {versions : List Version} {val : Int}
    (h : readAt snap rid versions = some val) :
    ∃ v : Version, visibleVersionAt snap rid versions = some v ∧
      v.rid = rid ∧ v.ts ≤ snap ∧ (!v.deleted) = true ∧ v.val = val := by
  unfold readAt at h
  generalize hv : visibleVersionAt snap rid versions = ov at h
  cases ov with
  | none => simp at h
  | some v =>
      obtain ⟨hvmem, hvrid, hvts⟩ := visible_mem_of_some hv
      by_cases hd : v.deleted
      · simp [hd] at h
      · simp [hd] at h
        exact ⟨v, rfl, hvrid, hvts, by simpa [hd], by simpa using h⟩

def readPair (snap : Nat) (versions : List Version) (rid : Nat) : Option (Nat × Int) :=
  (readAt snap rid versions).map (fun val => (rid, val))

def snapshotRows (snap : Nat) (versions : List Version) : List (Nat × Int) :=
  (versions.map Version.rid).eraseDups.filterMap (readPair snap versions)

theorem readAt_mem_snapshotRows (snap rid : Nat) (val : Int) (versions : List Version)
    (h : readAt snap rid versions = some val) :
    (rid, val) ∈ snapshotRows snap versions := by
  unfold snapshotRows
  apply List.mem_filterMap.mpr
  refine ⟨rid, ?_, ?_⟩
  · rw [List.mem_eraseDups]
    obtain ⟨v, hvis, hvrid, _, _, hval⟩ := visible_of_readAt h
    have hvmem : v ∈ versions := by
      have := List.mem_of_find?_eq_some hvis
      simpa [visibleVersionAt] using this
    rw [← hvrid]
    exact List.mem_map.mpr ⟨v, hvmem, rfl⟩
  · simp [readPair, h]

theorem mem_snapshotRows_readAt {snap rid : Nat} {val : Int} {versions : List Version}
    (h : (rid, val) ∈ snapshotRows snap versions) :
    readAt snap rid versions = some val := by
  unfold snapshotRows at h
  rcases List.mem_filterMap.mp h with ⟨r, hr, hp⟩
  have hr_eq : r = rid := by
    unfold readPair at hp
    cases hread : readAt snap r versions with
    | none => simp [hread] at hp
    | some vr =>
        simp [hread] at hp
        exact hp.1
  subst r
  simpa [readPair] using hp

def reference (snap : Nat) (p1 p2 : Int → Bool) (versions : List Version) :
    List (Nat × Int) :=
  (snapshotRows snap versions).filter (fun p => p1 p.2 && p2 p.2)

/-! ## Candidate generation and revalidation -/

def idxCand (p : Int → Bool) (idx : Index) : List Nat :=
  ((idx.entries.filter (fun e => p e.val)).map IdxEntry.rid).eraseDups

def deltaCand (p : Int → Bool) (deltas : List Delta) : List Nat :=
  ((deltas.filter (fun d => p d.val)).map Delta.rid).eraseDups

def candidates (p : Int → Bool) (idx : Index) (deltas : List Delta) : List Nat :=
  (idxCand p idx ++ deltaCand p deltas).eraseDups

def intersect (c1 c2 : List Nat) : List Nat :=
  c1.filter (fun r => c2.contains r)

def revalPair (p1 p2 : Int → Bool) (snap : Nat) (versions : List Version)
    (rid : Nat) : Option (Nat × Int) :=
  match readAt snap rid versions with
  | some val => if p1 val && p2 val then some (rid, val) else none
  | none => none

def revalidate (snap : Nat) (p1 p2 : Int → Bool) (versions : List Version)
    (cands : List Nat) : List (Nat × Int) :=
  cands.filterMap (revalPair p1 p2 snap versions)

def reReadCount (cands : List Nat) : Nat := cands.length

theorem length_filter_le {α : Type} (p : α → Bool) (l : List α) :
    (l.filter p).length ≤ l.length := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      by_cases h : p x = true <;> simp [h, ih] <;> omega

theorem reread_bound (c1 c2 : List Nat) :
    reReadCount (intersect c1 c2) ≤ c1.length ∧
    reReadCount (intersect c1 c2) ≤ c1.length + c2.length := by
  unfold reReadCount intersect
  have h := length_filter_le (fun r => c2.contains r) c1
  exact ⟨h, by omega⟩

/-! ## Coverage hypotheses -/

def IndexCovers (snap : Nat) (idx : Index) (versions : List Version) : Prop :=
  ∀ rid, match visibleVersionAt snap rid versions with
    | some v => if !v.deleted && decide (v.ts ≤ idx.wm)
        then IdxEntry.mk rid v.val ∈ idx.entries else True
    | none => True

def DeltaCovers (snap : Nat) (idx : Index) (deltas : List Delta)
    (versions : List Version) : Prop :=
  ∀ rid, match visibleVersionAt snap rid versions with
    | some v => if !v.deleted && decide (idx.wm < v.ts) then
        ∃ d ∈ deltas, d.rid == rid ∧ d.val = v.val ∧ d.ts ≤ snap
      else True
    | none => True

theorem matching_row_is_candidate (snap : Nat) (p : Int → Bool) (idx : Index)
    (deltas : List Delta) (versions : List Version) (rid : Nat) (val : Int)
    (hv : readAt snap rid versions = some val)
    (hp : p val = true)
    (hidx : IndexCovers snap idx versions)
    (hdel : DeltaCovers snap idx deltas versions) :
    rid ∈ candidates p idx deltas := by
  obtain ⟨v, hvis, hvrid, hvts, hnotDel, hval⟩ := visible_of_readAt hv
  unfold candidates
  rw [List.mem_eraseDups]
  apply List.mem_append.mpr
  by_cases hcovered : v.ts ≤ idx.wm
  · left
    have he : IdxEntry.mk rid v.val ∈ idx.entries := by
      have hspec := hidx rid
      rw [hvis] at hspec
      simp [hnotDel, hcovered] at hspec
      exact hspec
    unfold idxCand
    rw [List.mem_eraseDups]
    apply List.mem_map.mpr
    refine ⟨IdxEntry.mk rid v.val, ?_, by rfl⟩
    apply List.mem_filter.mpr
    exact ⟨he, by simpa [← hval] using hp⟩
  · right
    have htsgt : idx.wm < v.ts := by omega
    obtain ⟨d, hdmem, hdrid, hdval, _⟩ := by
      have hspec := hdel rid
      rw [hvis] at hspec
      simp [hnotDel, htsgt] at hspec
      exact hspec
    have hdv : d.val = val := hdval.trans hval
    unfold deltaCand
    rw [List.mem_eraseDups]
    apply List.mem_map.mpr
    refine ⟨d, ?_, by simpa using hdrid⟩
    apply List.mem_filter.mpr
    exact ⟨hdmem, by simpa [hdv] using hp⟩

theorem snapshot_index_intersect_sound (snap : Nat) (p1 p2 : Int → Bool)
    (versions : List Version) (idx1 idx2 : Index) (deltas1 deltas2 : List Delta)
    (hidx1 : IndexCovers snap idx1 versions)
    (hidx2 : IndexCovers snap idx2 versions)
    (hdel1 : DeltaCovers snap idx1 deltas1 versions)
    (hdel2 : DeltaCovers snap idx2 deltas2 versions) :
    let c1 := candidates p1 idx1 deltas1
    let c2 := candidates p2 idx2 deltas2
    (∀ row ∈ revalidate snap p1 p2 versions (intersect c1 c2),
        row ∈ reference snap p1 p2 versions) ∧
    (∀ row ∈ reference snap p1 p2 versions,
        row ∈ revalidate snap p1 p2 versions (intersect c1 c2)) := by
  intro c1 c2
  constructor
  · intro row hrow
    unfold revalidate at hrow
    rcases List.mem_filterMap.mp hrow with ⟨rid, hrid, hpairs⟩
    cases hread : readAt snap rid versions with
    | none => simp [revalPair, hread] at hpairs
    | some val =>
        simp [revalPair, hread] at hpairs
        obtain ⟨⟨hp1, hp2⟩, hrowEq⟩ := hpairs
        subst row
        unfold reference
        apply List.mem_filter.mpr
        exact ⟨readAt_mem_snapshotRows snap rid val versions hread, by simp [hp1, hp2]⟩
  · intro row hrow
    unfold reference at hrow
    rcases List.mem_filter.mp hrow with ⟨hrowSnap, hpboth⟩
    have hrv := mem_snapshotRows_readAt hrowSnap
    obtain ⟨rid, val⟩ := row
    simp at hrv
    have hpboth' : p1 val = true ∧ p2 val = true := by simpa using hpboth
    have hp1 : p1 val = true := hpboth'.1
    have hp2 : p2 val = true := hpboth'.2
    have hc1 := matching_row_is_candidate snap p1 idx1 deltas1 versions rid val hrv hp1 hidx1 hdel1
    have hc2 := matching_row_is_candidate snap p2 idx2 deltas2 versions rid val hrv hp2 hidx2 hdel2
    unfold revalidate
    apply List.mem_filterMap.mpr
    refine ⟨rid, ?_, ?_⟩
    · unfold intersect
      apply List.mem_filter.mpr
      exact ⟨hc1, by simpa using hc2⟩
    · simp [revalPair, hrv, hp1, hp2]

/-! ## Covering condition that eliminates candidates -/

theorem covered_range_eliminates (snap : Nat) (p : Int → Bool) (idx : Index)
    (deltas : List Delta) (lo hi : Int)
    (hidx : ∀ e ∈ idx.entries, lo ≤ e.val ∧ e.val ≤ hi)
    (hdel : ∀ d ∈ deltas, lo ≤ d.val ∧ d.val ≤ hi)
    (hp : ∀ v : Int, lo ≤ v → v ≤ hi → p v = false) :
    candidates p idx deltas = [] := by
  unfold candidates idxCand deltaCand
  have hie : (idx.entries.filter (fun e => p e.val)).map IdxEntry.rid = [] := by
    apply List.eq_nil_iff_forall_not_mem.mpr
    intro r hr
    rcases List.mem_map.mp hr with ⟨e, he, _⟩
    rcases List.mem_filter.mp he with ⟨heent, hpe⟩
    have hb := hidx e heent
    have hf : p e.val = false := hp e.val hb.1 hb.2
    rw [hf] at hpe
    simp at hpe
  have hde : (deltas.filter (fun d => p d.val)).map Delta.rid = [] := by
    apply List.eq_nil_iff_forall_not_mem.mpr
    intro r hr
    rcases List.mem_map.mp hr with ⟨d, hd, _⟩
    rcases List.mem_filter.mp hd with ⟨hdmem, hpd⟩
    have hb := hdel d hdmem
    have hf : p d.val = false := hp d.val hb.1 hb.2
    rw [hf] at hpd
    simp at hpd
  simp [hie, hde]

/-! ## Counterexample: RID-only intersection is a false positive -/

def ceVersions : List Version :=
  [{ rid := 1, val := 1, ts := 1, deleted := false },
   { rid := 1, val := 2, ts := 2, deleted := false }]

def ceIdx1 : Index := { wm := 1, entries := [{ rid := 1, val := 1 }] }
def ceIdx2 : Index := { wm := 2, entries := [{ rid := 1, val := 2 }] }

def ceP1 (v : Int) : Bool := v == 1
def ceP2 (v : Int) : Bool := v == 2

theorem rid_only_intersection_false_positive_counterexample :
    let c1 := idxCand ceP1 ceIdx1
    let c2 := idxCand ceP2 ceIdx2
    c1 = [1] ∧ c2 = [1] ∧ intersect c1 c2 = [1] ∧
    reference 2 ceP1 ceP2 ceVersions = [] ∧
    revalidate 2 ceP1 ceP2 ceVersions (intersect c1 c2) = [] := by
  decide

theorem tombstone_hides_old_version :
    let vs : List Version :=
      [{ rid := 7, val := 3, ts := 1, deleted := false },
       { rid := 7, val := 3, ts := 2, deleted := true }]
    snapshotRows 1 vs = [(7, 3)] ∧ snapshotRows 2 vs = [] ∧
    readAt 2 7 vs = none := by
  decide

theorem rid_reuse_after_delete :
    let vs : List Version :=
      [{ rid := 9, val := 1, ts := 1, deleted := false },
       { rid := 9, val := 1, ts := 2, deleted := true },
       { rid := 9, val := 42, ts := 3, deleted := false }]
    readAt 2 9 vs = none ∧ readAt 3 9 vs = some 42 ∧
    snapshotRows 3 vs = [(9, 42)] := by
  decide

end TinylambOpt.Research.SnapshotIndexIntersect
