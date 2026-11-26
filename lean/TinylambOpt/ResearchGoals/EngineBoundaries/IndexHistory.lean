import Std

/-!
Index reads across version changes must still observe the keys appropriate to the SQL
snapshot.
-/

namespace TinylambOpt.ResearchGoals.EngineBoundaries.IndexHistory

/-! A bounded committed mutation journal. A scan asks whether a half-open key
range was touched after its snapshot. Unknown history requests a base scan.
Uncommitted keys and snapshot row visibility belong to separate checks. -/
structure Change where
  root : Nat
  key : Nat
  ts : Nat
  deriving BEq, DecidableEq
structure Query where
  root : Nat
  lo : Nat
  hi : Nat
  snap : Nat

def touches (q : Query) (c : Change) :=
  c.root == q.root && q.lo ≤ c.key && c.key < q.hi && q.snap < c.ts

def latest (cs : List Change) := (cs.map Change.ts).max?.getD 0
def exact (q : Query) (cs : List Change) := cs.any (touches q)
def answer (q : Query) (water : Nat) (kept : List Change) : Option Bool :=
  if q.lo ≥ q.hi then some false else if q.snap < water then none else some (exact q kept)
def relevant (q : Query) (cs : List Change) := cs.filter (fun c => c.root == q.root)
def overlaps (q : Query) (c : Change) := q.lo ≤ c.key && c.key < q.hi

/-- Every precise answer from a bounded journal matches the complete history. -/
def EvictionFence.Goal : Prop := ∀ q lost kept b,
  answer q (latest lost) kept = some b → exact q (lost ++ kept) = b

/-- Per-root watermarks avoid fallback for evictions on another root. -/
def RootLocalFence.Goal : Prop := ∀ q lost kept b,
  answer q (latest (relevant q lost)) kept = some b → exact q (lost ++ kept) = b

/-- An interval-specific eviction summary can recover precision on cold keys. -/
def RangeFence.Goal : Prop := ∀ q lost kept b,
  answer q (latest ((relevant q lost).filter (overlaps q))) kept = some b →
    exact q (lost ++ kept) = b

/-- Taking the minimum forgotten timestamp instead is an aggressive precision shortcut. -/
def EarliestEviction.Goal : Prop := ∀ q lost kept b,
  answer q ((lost.map Change.ts).min?.getD 0) kept = some b → exact q (lost ++ kept) = b

/-- Keep only the latest mutation for a key when checking a historical change, regardless of multiplicity. -/
def LatestPerKey.Goal : Prop := ∀ q cs,
  exact q cs = cs.any (fun c => touches q c &&
    !(cs.any fun d => d.root == c.root && d.key == c.key && c.ts < d.ts))

/-- Combining two eviction batches requires just their two upper timestamps. -/
def MergeWatermarks.Goal : Prop := ∀ a b, latest (a ++ b) = max (latest a) (latest b)

/-- Split a scan into half-open intervals with no blind spot at the pivot. -/
def SplitRange.Goal : Prop := ∀ q mid cs, q.lo ≤ mid → mid ≤ q.hi →
  exact q cs = (exact {q with hi := mid} cs || exact {q with lo := mid} cs)

/-- An observed hit settles the query even when older evidence was discarded. -/
def HitDespiteEviction.Goal : Prop := ∀ q kept lost, exact q kept = true →
  exact q (lost ++ kept) = true

/-- A clean range for an old snapshot is also clean for later snapshots. -/
def AdvanceSnapshot.Goal : Prop := ∀ q t cs, q.snap ≤ t → exact q cs = false →
  exact {q with snap := t} cs = false

/-- Reusing a clean result after new commits requires checking only the appended suffix. -/
def ExtendClean.Goal : Prop := ∀ q old delta, exact q old = false →
  exact q (old ++ delta) = exact q delta

private def witness : Query := ⟨0, 1, 2, 5⟩
#guard answer witness 1 [] == some false
#guard exact witness [⟨0, 0, 1⟩, ⟨0, 1, 9⟩]

end TinylambOpt.ResearchGoals.EngineBoundaries.IndexHistory
