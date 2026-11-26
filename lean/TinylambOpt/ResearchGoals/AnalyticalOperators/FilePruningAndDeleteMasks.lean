import TinylambOpt.ResearchGoals.CloudBI.DeleteFiles

/-!
These models connect SQL operators to their reference rows, order, or aggregate results.
-/

namespace TinylambOpt.ResearchGoals.AnalyticalOperators.FilePruningAndDeleteMasks
open CloudBI.DeleteFiles

structure File where
  id : Nat
  part : Part
  seq : Nat
  deriving DecidableEq, BEq

abbrev Data := Nat × Option Int × Int -- file id, equality key, measure to sum
def summary (vs : List Int) := (vs.length, if vs.isEmpty then none else some vs.sum)

/-- Base COUNT/SUM can bypass metadata iff every referenced file joins exactly
once. Unreferenced metadata can contain duplicate entries. Empty SUM is NULL. -/
def ReferencedFiles.Goal : Prop := ∀ (rows : List Data) (fs : List File),
  (∀ r ∈ rows, fs.countP (fun f => f.id == r.1) = 1) →
  summary (rows.flatMap (fun (fid,_,v) => (fs.filter (·.id == fid)).map (fun _ => v))) =
    summary (rows.map (fun r => r.2.2))

def applies (f : File) (d : EqDel) := d.scope.all (· == f.part) && f.seq < d.seq
def keysByFile (fs : List File) (ds : List EqDel) :=
  (fs.flatMap (fun f => (ds.filter (applies f)).map (fun d => (f.id,d.key)))).eraseDups

/-- Decorrelate the equality anti-join by compiling pairs (file,key), then test each
data row once. The reference is the file join followed by delete application. -/
def FileKeyAntiJoin.Goal : Prop := ∀ (rows : List Data) fs ds,
  (∀ r ∈ rows, fs.countP (fun f => f.id == r.1) = 1) →
  let pairs := keysByFile fs ds
  let kept := rows.filter (fun (fid,k,_) => !pairs.contains (fid,k))
  kept.map (fun r => r.2.2) = rows.flatMap (fun (fid,k,v) =>
    (fs.filter (fun f => f.id == fid && !ds.any (eqHit ⟨fid,0,f.part,f.seq,k⟩))).map (fun _ => v))

/-- Files with the same (spec,partition,data sequence) share one compiled key set.
Count dictionaries even for a signature whose matching delete set is empty. -/
def signature (f : File) := (f.part,f.seq)
def compile (fs : List File) (ds : List EqDel) :=
  (fs.map signature).eraseDups.map fun (p,s) =>
    ((p,s), ((ds.filter (applies ⟨0,p,s⟩)).map EqDel.key).eraseDups)

def SharedDeleteTable.Goal : Prop := ∀ fs ds f, f ∈ fs →
  let tables := compile fs ds
  (∀ k, ((tables.lookup (signature f)).getD []).contains k =
    ds.any (eqHit ⟨f.id,0,f.part,f.seq,k⟩)) ∧ tables.length ≤ fs.length

/-- Separate partition-local from global entries once when the target partition is
fixed. This yields the same delete pairs even if the two scopes overlap. -/
def PartitionSpecialization.Goal : Prop := ∀ (fs : List File) (ds : List EqDel) (p : Part) (oldest : Nat),
  (∀ f ∈ fs, f.part = p → oldest ≤ f.seq) →
  let target := fs.filter (fun (f : File) => f.part == p)
  let selected := ds.filter (fun (d : EqDel) => (d.scope == some p || d.scope.isNone) && oldest < d.seq)
  keysByFile target selected = keysByFile target ds

/-- Add a delete file to a pinned delete set by filtering only the survivors.
The same equality key can be deleted at different sequence numbers. -/
def incremental (old fresh : List EqDel) (rows : List Row) :=
  let changed := (fresh.map EqDel.key).eraseDups
  (rows.filter (fun r => !old.any (eqHit r))).filter (fun r =>
    !changed.contains r.key || !fresh.any (eqHit r))
def IncrementalSurvivors.Goal : Prop := ∀ old fresh rows, incremental old fresh rows =
  rows.filter (fun r => !(old ++ fresh).any (eqHit r))

/-- Position deletions can suppress key loads before probing equality deletes. Count
the keys requested from the payload, and retain the original physical row IDs. -/
def probeAfterPosition (ps : List PosDel) (es : List EqDel) (rows : List Row) :=
  rows.foldr (fun r (kept,n) =>
    if ps.any (posHit r) then (kept,n)
    else (if es.any (eqHit r) then kept else r::kept,n+1)) ([],0)

def LateEqualityKey.Goal : Prop := ∀ ps es rows,
  probeAfterPosition ps es rows =
    (rows.filter (fun r => !(es.any (eqHit r) || ps.any (posHit r))),
     rows.length - rows.countP (fun r => ps.any (posHit r)))

/-- When file row IDs form an arithmetic progression, derive COUNT/SUM from the
footer and subtract only distinct, in-range deleted positions. Handles complete deletion. -/
def ArithmeticFooter.Goal : Prop := ∀ (n : Nat) (base stride : Int) (deleted : List Nat),
  let val := fun (p : Nat) => base + stride * (p:Int)
  let dead := (deleted.filter (· < n)).eraseDups
  let alive := n - dead.length
  let total := (n:Int)*base + stride * ((n*(n-1)/2 : Nat):Int)
  summary (((List.range n).filter (fun p => !deleted.contains p)).map val) =
    (alive,if alive == 0 then none else some (total - (dead.map val).sum))

#guard keysByFile [⟨1,(0,1),2⟩,⟨2,(0,1),3⟩] [⟨none,3,none⟩,⟨some (0,1),3,none⟩] == [(1,none)]
#guard (probeAfterPosition [⟨2,0,(0,1),4⟩] [⟨none,5,none⟩]
  [⟨2,0,(0,1),4,none⟩,⟨2,1,(0,1),4,none⟩,⟨2,2,(0,1),4,some 1⟩]).2 == 2
#guard summary [] == (0,none) && summary [4,-4] == (2,some 0)

end TinylambOpt.ResearchGoals.AnalyticalOperators.FilePruningAndDeleteMasks
