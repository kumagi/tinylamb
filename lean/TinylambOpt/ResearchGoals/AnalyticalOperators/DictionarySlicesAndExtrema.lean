import TinylambOpt.ResearchGoals.CloudBI.PublicBI

/-!
These models connect SQL operators to their reference rows, order, or aggregate results.
-/

namespace TinylambOpt.ResearchGoals.AnalyticalOperators.DictionarySlicesAndExtrema
open CloudBI.PublicBI

/-- Store the shorter of the true/false code lists for a CASE predicate. A SQL NULL
position belongs to ELSE. Each dictionary entry counts as a code, including aliases. -/
def exceptions (d : Array (Option String)) :=
  let ids := List.range d.size
  let yes := ids.filter (fun i => outfield d[i]?.join)
  let no := ids.filter (fun i => !outfield d[i]?.join)
  if yes.length ≤ no.length then (false,yes) else (true,no)

def MinorityCodes.Goal : Prop := ∀ (c : Chunk), (∀ i ∈ c.codes, i < c.dict.size) →
  let (defaultBit,exceptions_) := exceptions c.dict
  c.codes.map (fun i => defaultBit != exceptions_.contains i) = (labels c).map outfield ∧
    2 * exceptions_.length ≤ c.dict.size

/-- On dictionary growth, retain the classification of the original code range.
Only appended entries must be classified; renumbering the old entries is disallowed. -/
def AppendDictionary.Goal : Prop := ∀ (d added : Array (Option String)) (ids : List Nat),
  let (defaultBit,oldExceptions) := exceptions d
  let patch := (List.range added.size).filter (fun i => outfield added[i]?.join != defaultBit)
  let updated := oldExceptions ++ patch.map (d.size + ·)
  ids.map (fun i => decide (i < d.size + added.size) && (defaultBit != updated.contains i)) =
    (labels ⟨d ++ added,ids⟩).map outfield

/-- Materialize the two CASE groups by subtracting positive frequencies from total
league frequencies. A touched league may have only one of the two output groups. -/
def splitCounts (counts : List (Dim × Nat)) (league : Option String) :=
  let selected := counts.filter (fun x => x.1.1 == league)
  let n := (selected.map Prod.snd).sum
  let t := ((selected.filter (fun x => outfield x.1.2)).map Prod.snd).sum
  [(true,t),(false,n-t)].filter (fun x => x.2 > 0)

def ComplementGroups.Goal : Prop := ∀ (counts : List (Dim × Nat)) league flag,
  let refs := counts.flatMap (fun (k,n) => List.replicate n k)
  let n := refs.countP (fun (l,p) => l == league && outfield p == flag)
  (splitCounts counts league).lookup flag = (if n == 0 then none else some n)

/-- Q108: use one comparison per adjacent non-NULL pair to choose which values
can enter MAX and MIN. The odd tail is a candidate on both sides. -/
def pairCandidates : List Int → List Int × List Int
  | [] => ([],[])
  | [x] => ([x],[x])
  | x::y::xs =>
    let (high,low) := pairCandidates xs
    if x < y then (y::high,x::low) else (x::high,y::low)

def PairwiseExtrema.Goal : Prop := ∀ xs,
  let vs := xs.filterMap id
  let (high,low) := pairCandidates vs
  (high.max?,low.min?) = extrema xs ∧
    high.length + low.length = vs.length + vs.length % 2

/-- A page wholly inside current extrema cannot improve them under any residual
row predicate. Missing/unusable bounds go through the ordinary evaluation path. -/
def dominated (prior : Ext) (lo hi : Option Int) : Bool :=
  match prior,lo,hi with
  | (some mx,some mn),some l,some h => decide (mn ≤ l ∧ h ≤ mx)
  | _,_,_ => false

def DominatedColumn.Goal : Prop := ∀ (seen : List (Option Int)) (page : List (Bool × Option Int)) lo hi,
  (∀ r ∈ page, ∀ x, r.2 = some x → lo.all (· ≤ x) = true ∧ hi.all (x ≤ ·) = true) →
  let kept := (page.filter Prod.fst).map Prod.snd
  (if dominated (extrema seen) lo hi then extrema seen else mergeExt (extrema seen) (extrema kept)) =
    extrema (seen ++ kept)

/-- If a selected position is a witness for each unfiltered extreme, return the
stored page extrema. Either witness can be on a different row. -/
def WitnessedExtrema.Goal : Prop := ∀ (xs : List (Option Int)) (ids : List Nat) hi lo,
  extrema xs = (some hi,some lo) →
  let gather := ids.map (fun i => xs[i]?.join)
  gather.contains (some hi) = true → gather.contains (some lo) = true →
  extrema gather = (some hi,some lo)

/-- Q108: materialize sparse row IDs from the most selective posting list. Duplicates
in that list must not cause extra payload reads. The other two lists may be unsorted. -/
def postingScan (n : Nat) (a b c : List Nat) :=
  ((a.eraseDups.filter (fun i => i < n && b.contains i && c.contains i))).mergeSort

def SparsePosting.Goal : Prop := ∀ n a b c (values : Array (Option Int)),
  let rows := (List.range n).filter (fun i => a.contains i && b.contains i && c.contains i)
  let touched := postingScan n a b c
  touched.map (fun i => values[i]?.join) = rows.map (fun i => values[i]?.join) ∧
    touched.length ≤ a.eraseDups.length

#guard exceptions #[some "Right Fielder",some "Left Fielder"] == (true,[])
#guard splitCounts [((some "P",none),2),((some "P",some "Left Fielder"),0)] (some "P") == [(false,2)]
#guard exceptions #[none,some "Right Fielder",some "Left Fielder"] == (true,[0])
#guard pairCandidates [7,-1,4] == ([7,4],[-1,4])
#guard dominated (some 8,some 2) (some 2) (some 8) && !dominated (some 8,none) (some 2) (some 8)
#guard postingScan 5 [9,4,2,2] [2,4] [4,2] == [2,4]

end TinylambOpt.ResearchGoals.AnalyticalOperators.DictionarySlicesAndExtrema
