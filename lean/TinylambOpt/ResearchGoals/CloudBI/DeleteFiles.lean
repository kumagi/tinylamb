import Std

/-!
These models describe SQL scans and aggregates over file metadata and selected column
data.
-/

namespace TinylambOpt.ResearchGoals.CloudBI.DeleteFiles

/-- File identity and physical ordinal survive scan-time filtering. Partition identity
includes the spec id. 'none' as equality-delete scope denotes an unpartitioned delete file. -/
abbrev Part := Nat × Nat
structure Row where
  file : Nat
  pos : Nat
  part : Part
  seq : Nat
  key : Option Int
  deriving DecidableEq, BEq

structure EqDel where
  scope : Option Part
  seq : Nat
  key : Option Int
  deriving DecidableEq, BEq

structure PosDel where
  file : Nat
  pos : Nat
  part : Part
  seq : Nat
  deriving DecidableEq, BEq

def eqHit (r : Row) (d : EqDel) :=
  d.scope.all (· == r.part) && d.key == r.key && r.seq < d.seq
def posHit (r : Row) (d : PosDel) :=
  r.file == d.file && r.pos == d.pos && r.part == d.part && r.seq ≤ d.seq

/-- Build one maximum equality-delete sequence per (scope,key). Global and local
entries compete; duplicate keys and NULL are legitimate equality-delete keys. -/
def eqIndex (ds : List EqDel) := ds.foldl (fun (m : Std.HashMap (Option Part × Option Int) Nat) d =>
  let k := (d.scope,d.key)
  m.insert k (max (m.getD k 0) d.seq)) {}

def EqualityHorizon.Goal : Prop := ∀ ds (r : Row),
  let m := eqIndex ds
  (r.seq < max (m.getD (none,r.key) 0) (m.getD (some r.part,r.key) 0)) ↔
    ds.any (eqHit r) = true

/-- File-local deduplicated position set can be built once before decoding a file.
Sequence equality applies for position deletes. -/
def ordinalSet (ds : List PosDel) (file : Nat) (part : Part) (seq : Nat) :=
  (ds.filter (fun d => d.file == file && d.part == part && seq ≤ d.seq)).foldl
    (fun (s : Std.HashSet Nat) d => s.insert d.pos) {}
def PositionMask.Goal : Prop := ∀ (rows : List Row) ds file part seq,
  (∀ r ∈ rows, r.file = file ∧ r.part = part ∧ r.seq = seq) →
  rows.filter (fun r => !(ordinalSet ds file part seq).contains r.pos) =
    rows.filter (fun r => !ds.any (posHit r))

/-- Subtract the union of delete masks from a footer count. Duplicated deletes and
overlap between delete kinds must not reduce the number of survivors twice. -/
def OverlapCount.Goal : Prop := ∀ (rows : List Row) (ps : List PosDel) (es : List EqDel),
  let p := fun r => ps.any (posHit r)
  let e := fun r => es.any (eqHit r)
  (rows.filter (fun r => !p r && !e r)).length =
    rows.length - (rows.countP p + rows.countP e - rows.countP (fun r => p r && e r))

/-- Metadata-only COUNT for a file with only position deletes. Ignore out-of-range
ordinals and duplicates in applicable files before subtracting from record_count. -/
def FooterCount.Goal : Prop := ∀ (n file seq : Nat) (part : Part) (ps : List PosDel),
  let ds := (ps.filter (fun d => d.file == file && d.part == part && seq ≤ d.seq)).map PosDel.pos
  n - (ds.filter (· < n)).eraseDups.length =
    ((List.range n).filter (fun p => !ps.any (posHit ⟨file,p,part,seq,none⟩))).length

/-- Manifest-level pruning by partition and sequence. For equality deletes the
maximum must be strictly newer than the file. Missing bounds permit a scan. -/
structure Manifest where
  scope : Option Part
  upper : Option Nat
  entries : List EqDel

def mayOpen (r : Row) (m : Manifest) :=
  m.scope.all (· == r.part) && m.upper.all (r.seq < ·)
def synopsisOK (m : Manifest) : Prop := ∀ d ∈ m.entries,
  (∀ p, m.scope = some p → d.scope = some p) ∧ (∀ u, m.upper = some u → d.seq ≤ u)

def ManifestHorizon.Goal : Prop := ∀ (ms : List Manifest) r,
  (∀ m ∈ ms, synopsisOK m) →
  ((ms.filter (mayOpen r)).flatMap Manifest.entries).any (eqHit r) =
    (ms.flatMap Manifest.entries).any (eqHit r)

/-- Use numeric column bounds for equality-delete pruning only for non-NULL keys;
a NULL-key delete may apply even when the numeric interval excludes all probes. -/
def possible (lo hi : Option Int) (hasNull : Bool) : Option Int → Bool
  | none => hasNull
  | some k => lo.all (· ≤ k) && hi.all (k ≤ ·)

def NullAwarePruning.Goal : Prop := ∀ (rows : List Row) (ds : List EqDel) lo hi hasNull,
  (∀ r ∈ rows, possible lo hi hasNull r.key = true) →
  let candidates := ds.filter (fun d => possible lo hi hasNull d.key)
  rows.filter (fun r => !candidates.any (eqHit r)) = rows.filter (fun r => !ds.any (eqHit r))

/-- Resolve equality-delete field IDs into physical offsets once per file schema.
Optional missing columns in this model have NULL read defaults; record order varies. -/
def FieldIdProbe.Goal : Prop := ∀ (layout fields : List Nat) (row : List (Option Int)),
  row.length = layout.length → layout.Nodup →
  let slots := fields.map layout.idxOf
  slots.map (fun i => (row[i]?).join) =
    fields.map (fun f => (((layout.zip row).find? (fun kv => kv.1 == f)).bind Prod.snd))

/-- After applying pinned deletes, compaction densely renumbers survivors. Translate a
later predicate on old file positions by prefix survivor rank, preserving row identity. -/
def CompactOrdinal.Goal : Prop := ∀ (rows : List Row) (ps : List PosDel) (es : List EqDel) i r,
  rows[i]? = some r →
  let keep := fun x => !ps.any (posHit x) && !es.any (eqHit x)
  keep r = true → (rows.filter keep)[((rows.take i).countP keep)]? = some r

/-- Ordered scan can overfetch by an upper bound on the number of dead rows.
Only qualifying survivors consume LIMIT slots, including when the same row is deleted twice. -/
def DeleteAwareLimit.Goal : Prop := ∀ (ordered : List Row) (ps : List PosDel) (es : List EqDel) k upper,
  let dead := fun r => ps.any (posHit r) || es.any (eqHit r)
  ordered.countP dead ≤ upper →
  (((ordered.take (k+upper)).filter (fun r => !dead r)).take k) =
    ((ordered.filter (fun r => !dead r)).take k)

/-- Skip whole row groups without rebasing delete-file positions. A row group that
is rejected by the data predicate still contributes its full record count to the
base offset of every following group. -/
def groupScan (groups : List (List Int)) (keep : Int → Bool) (deleted : Nat → Bool) :=
  groups.foldl (fun (base,out) g =>
    let live := g.zipIdx.filterMap (fun (v,i) =>
      if keep v && !deleted (base+i) then some v else none)
    (base+g.length, if g.any keep then out ++ live else out)) (0,([] : List Int))
def SkippedOrdinalBase.Goal : Prop := ∀ gs keep deleted,
  (groupScan gs keep deleted).2 = gs.flatten.zipIdx.filterMap (fun (v,i) =>
    if keep v && !deleted i then some v else none)

/-- A key-only bitmap computed for several files cannot be reused past its data
sequence frontier. Adjacent sequence intervals with no applicable delete boundary
have the same equality-delete bitmap, including where the row key is NULL. -/
def BitmapEpoch.Goal : Prop := ∀ (ds : List EqDel) (r : Row) newer,
  r.seq ≤ newer →
  (∀ d ∈ ds, d.scope.all (· == r.part) = true → d.key = r.key →
    ¬ (r.seq < d.seq ∧ d.seq ≤ newer)) →
  ds.any (eqHit r) = ds.any (eqHit {r with seq := newer})

#guard !eqHit ⟨1,0,(1,1),3,none⟩ ⟨none,3,none⟩
#guard posHit ⟨1,0,(1,1),3,none⟩ ⟨1,0,(1,1),3⟩
#guard !eqHit ⟨1,0,(1,1),3,none⟩ ⟨some (2,1),4,none⟩
#guard (eqIndex [⟨none,4,none⟩,⟨none,2,none⟩]).getD (none,none) 0 == 4

end TinylambOpt.ResearchGoals.CloudBI.DeleteFiles
