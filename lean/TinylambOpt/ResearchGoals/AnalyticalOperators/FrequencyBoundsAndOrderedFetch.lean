import TinylambOpt.ResearchGoals.ExecutionStrategies.SearchBounds
import TinylambOpt.ResearchGoals.AnalyticsWorkloads.Common

/-!
These models connect SQL operators to their reference rows, order, or aggregate results.
-/

namespace TinylambOpt.ResearchGoals.AnalyticalOperators.FrequencyBoundsAndOrderedFetch
open AnalyticsWorkloads

/-- Actual cancellation rounds can be much smaller than n/(slots+1). Recover
their count from the surviving Misra–Gries counters by conserving total votes. -/
def debt (xs : List Nat) (slots : Nat) :=
  let h := SearchBounds.candidates xs slots
  (xs.length - (h.toList.map Prod.snd).sum) / (slots+1)

def CancellationDebt.Goal : Prop := ∀ xs slots k,
  let h := SearchBounds.candidates xs slots
  let d := debt xs slots
  xs.length = (h.toList.map Prod.snd).sum + (slots+1)*d ∧
    h.getD k 0 ≤ xs.count k ∧ xs.count k ≤ h.getD k 0 + d

def ranked (rows keys : List Nat) := keys.mergeSort (fun a b =>
  rows.count a > rows.count b || (rows.count a == rows.count b && a ≤ b))

/-- Recount only sketch residents for top-K if at least K of them exceed the
omitted-frequency bound. Strict comparison keeps boundary ties; id resolves ties
consistently in both plans. Otherwise the executor needs the full-count path. -/
def CertifiedWinners.Goal : Prop := ∀ xs slots k,
  let keys := (SearchBounds.candidates xs slots).toList.map Prod.fst
  k ≤ keys.countP (fun g => debt xs slots < xs.count g) →
  (ranked xs keys).take k = (ranked xs xs.eraseDups).take k

/-- Q23: COUNT DISTINCT UserID for winning phrases. Assign each non-NULL member
to one owner shard, deduplicate locally, then add owner counts without a global set. -/
def OwnerDistinct.Goal : Prop := ∀ (rows : List (Nat × Option Nat)) (owner : Nat → Nat)
    (workers winner : Nat), (∀ x, owner x < workers) →
  let ids := (rows.filter (·.1 == winner)).filterMap Prod.snd
  ((List.range workers).map (fun j =>
    ((ids.filter (fun x => owner x == j)).eraseDups).length)).sum = ids.eraseDups.length

/-- Q22/Q23: for each winning phrase and local dictionary, only decode the minimal
eligible code when the dictionary uses the same binary ordering as the query.
The dictionary can have multiple codes for one label; NULLs have no code here. -/
def localMin (dict : Array String) (codes : List Nat) :=
  codes.min?.bind (fun i => dict[i]?)
def OrderedCodeMin.Goal : Prop := ∀ (pages : List (Array String × List Nat)),
  (∀ p ∈ pages, p.1.toList.Pairwise (· ≤ ·) ∧ ∀ i ∈ p.2, i < p.1.size) →
  (pages.filterMap (fun (d,cs) => localMin d cs)).min? =
    (pages.flatMap (fun (d,cs) => cs.filterMap (fun i => d[i]?))).min?

/-- Q21/Q22: front-coded URL dictionaries can share the prefix match. If the
prefix misses, only its last needle.length-1 characters may start a crossing hit. -/
def occurs (s pat : List Char) := (List.range (s.length+1)).any (fun i => pat.isPrefixOf (s.drop i))
def FrontCodedContains.Goal : Prop := ∀ (common needle : List Char) (tails : List (List Char)),
  needle ≠ [] →
  let shard := common.drop (common.length - (needle.length-1))
  (if occurs common needle then List.replicate tails.length true
   else tails.map (fun s => occurs (shard ++ s) needle)) =
    tails.map (fun s => occurs (common ++ s) needle)

/-- Q24: time-ordered row IDs with a covered URL predicate. Only winning rows
fetch wide payloads; the predicate is tested through the kth hit or end-of-stream. -/
def seekHits (hit : Nat → Bool) : Nat → List Nat → List Nat × Nat
  | 0,_ => ([],0)
  | _+1,[] => ([],0)
  | k+1,x::xs =>
    let ok := hit x
    let (out,n) := seekHits hit (if ok then k else k+1) xs
    (if ok then x::out else out,n+1)
termination_by _ xs => xs.length

def OrderedFetch.Goal : Prop := ∀ (ids : List Nat) hit k (payload : Nat → String),
  let (out,n) := seekHits hit k ids
  out.map payload = (((ids.filter hit).take k).map payload) ∧ n ≤ ids.length ∧
    (n > 0 → ((ids.take (n-1)).countP hit < k)) ∧ (n < ids.length → out.length = k)

/-- Q37/Q38: bucketed date postings for a fixed CounterID and flags. Add full
bucket counts directly; only boundary buckets need per-date residual checks. -/
def DatePostings.Goal : Prop := ∀ (rows : List (Nat × Nat)) width lo hi url,
  0 < width →
  let hits := rows.filter (·.2 == url)
  let buckets := histogram (hits.map (fun r => r.1 / width))
  let whole := fun k => decide (lo ≤ k*width ∧ (k+1)*width-1 ≤ hi)
  let interior := buckets.toList.foldl (fun n (k,w) => if whole k then n+w else n) 0
  interior + hits.countP (fun r => !whole (r.1/width) && lo ≤ r.1 && r.1 ≤ hi) =
    hits.countP (fun r => lo ≤ r.1 && r.1 ≤ hi)

/-- Q20: filter a frame-of-reference encoded UserID column in the code domain.
A noncongruent probe rejects the frame without examining packed offsets; negative
bases and duplicate offsets are allowed. -/
def StridedPoint.Goal : Prop := ∀ (codes : List Nat) (base step target : Int), 0 < step →
  let diff := target - base
  let count := if diff < 0 || diff % step != 0 then 0 else codes.count (diff/step).toNat
  count = codes.countP (fun (off : Nat) => base + step*(off:Int) == target)

#guard debt [1,1,1,2,3] 2 == 1 && debt [1,1,1] 2 == 0
#guard occurs "goo".toList "google".toList == false && occurs "google".toList "google".toList
#guard seekHits (· == 3) 1 [2,3,1,3] == ([3],2)
#guard localMin #["a","a","b"] [2,1] == some "a"

end TinylambOpt.ResearchGoals.AnalyticalOperators.FrequencyBoundsAndOrderedFetch
