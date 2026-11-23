import Std

namespace TinylambOpt.ResearchGoals.OffsetHeavyReverseTopNAndQuotientBucket

abbrev Item := Int × Nat  -- (score, tie_id)

def descLe (a b : Item) : Bool :=
  if a.1 != b.1 then decide (b.1 ≤ a.1) else decide (a.2 ≤ b.2)

def ascLe (a b : Item) : Bool :=
  descLe b a

def insertSortedBy {α : Type} (le : α → α → Bool) (x : α) : List α → List α
  | [] => [x]
  | y :: ys => if le x y then x :: y :: ys else y :: insertSortedBy le x ys

def sortBy {α : Type} (le : α → α → Bool) : List α → List α
  | [] => []
  | x :: xs => insertSortedBy le x (sortBy le xs)

def refDescSlice (off lim : Nat) (xs : List Item) : List Item :=
  ((sortBy descLe xs).drop off).take lim

/-- Optimized offset-heavy slice via ascending bottom-`(N - off)` + reverse.
    Early-exits with `[]` whenever `xs.length ≤ off` without sorting. -/
def optReverseAscSlice (off lim : Nat) (xs : List Item) : List Item :=
  if xs.length ≤ off then []
  else
    let keep := xs.length - off
    (((sortBy ascLe xs).take keep).reverse).take lim

def addBucketCnt (m : List (Int × Nat)) (b : Int) : List (Int × Nat) :=
  match m with
  | [] => [(b, 1)]
  | (b', c) :: rest =>
      if b' == b then (b', c + 1) :: rest
      else (b', c) :: addBucketCnt rest b

def refQuotientGroupSort (ts : List Int) : List (Int × Nat) :=
  let m := ts.foldl (fun acc t => addBucketCnt acc (Int.ediv t 60)) []
  sortBy (fun a b => decide (a.1 ≤ b.1)) m

/-- Dense bucket count for monotonic time quotient `Int.ediv t 60 ∈ [lo..hi]`
    emitting groups directly in sorted order without a sort operator. -/
def optDenseMinuteSweep (lo hi : Int) (ts : List Int) : List (Int × Nat) :=
  let bLo := Int.ediv lo 60
  let bHi := Int.ediv hi 60
  let span := if bHi < bLo then 0 else (bHi - bLo).toNat + 1
  (List.range span).filterMap fun idx =>
    let b := bLo + Int.ofNat idx
    let c := (ts.filter (fun t => Int.ediv t 60 == b)).length
    if 0 < c then some (b, c) else none

/-- Offset-heavy reverse Top-N duality (`ORDER BY DESC LIMIT L OFFSET O` with
    `O ≫ L`, ClickBench Q40/Q41/Q42) and monotone `TIMESTAMP_TRUNC` floor
    quotient grouping (ClickBench Q43):
    (1) when `xs.length ≤ off`, output is `[]` without sorting; otherwise
    reversing the `xs.length - off` ascending elements equals dropping `off`
    descending elements (assuming unique `(score, tieId)` or uniform ties);
    (2) `Int.ediv · 60` is monotone and dense bucket iteration over `[lo/60..hi/60]`
    matches hash-group + `ORDER BY`. -/
def Goal : Prop :=
  (∀ (off lim : Nat) (xs : List Item),
    xs.Nodup →
      optReverseAscSlice off lim xs = refDescSlice off lim xs) ∧
  (∀ (t1 t2 : Int), t1 ≤ t2 → Int.ediv t1 60 ≤ Int.ediv t2 60) ∧
  (∀ (lo hi : Int) (ts : List Int),
    (∀ t ∈ ts, lo ≤ t ∧ t ≤ hi) →
      optDenseMinuteSweep lo hi ts = refQuotientGroupSort ts)

example :
    let xs : List Item := [(50, 1), (90, 2), (50, 3), (10, 4), (70, 5)]
    optReverseAscSlice 2 2 xs = refDescSlice 2 2 xs ∧
    optReverseAscSlice 2 2 xs = [(50, 1), (50, 3)] ∧
    optReverseAscSlice 5 10 xs = [] := by
  decide

example :
    let ts : List Int := [185, 62, -5, 120, 61]
    optDenseMinuteSweep (-10) 200 ts = refQuotientGroupSort ts ∧
    optDenseMinuteSweep (-10) 200 ts = [(-1, 1), (1, 2), (2, 1), (3, 1)] := by
  decide

end TinylambOpt.ResearchGoals.OffsetHeavyReverseTopNAndQuotientBucket
