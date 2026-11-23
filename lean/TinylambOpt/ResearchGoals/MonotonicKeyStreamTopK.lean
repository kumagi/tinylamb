import Std

namespace TinylambOpt.ResearchGoals.MonotonicKeyStreamTopK

abbrev Entry := Int × Int  -- (key, sum)

def betterEntry (a b : Entry) : Bool :=
  if a.2 != b.2 then decide (b.2 < a.2) else decide (a.1 < b.1)

def addKeyVal (m : List Entry) (kv : Entry) : List Entry :=
  match m with
  | [] => [kv]
  | (k', s') :: rest =>
      if k' == kv.1 then (k', s' + kv.2) :: rest
      else (k', s') :: addKeyVal rest kv

def insertSortedBy {α : Type} (le : α → α → Bool) (x : α) : List α → List α
  | [] => [x]
  | y :: ys => if le x y then x :: y :: ys else y :: insertSortedBy le x ys

def sortBy {α : Type} (le : α → α → Bool) : List α → List α
  | [] => []
  | x :: xs => insertSortedBy le x (sortBy le xs)

def insertTopK (k : Nat) (e : Entry) (top : List Entry) : List Entry :=
  let sorted := insertSortedBy betterEntry e (sortBy betterEntry top)
  sorted.take k

def mergeTopK (k : Nat) (a b : List Entry) : List Entry :=
  let sorted := sortBy betterEntry (a ++ b)
  sorted.take k

/-- Reference: flatten all chunks, group-sum all keys, sort descending by sum
    (tie-breaking ascending by key), and take `K`. -/
def refTopK (k : Nat) (chunks : List (List Entry)) : List Entry :=
  let grouped := chunks.flatten.foldl addKeyVal []
  (sortBy betterEntry grouped).take k

structure WorkerSummary where
  seams : List Entry
  interiorTop : List Entry
  deriving DecidableEq

/-- Process one worker's monotonic chunk in a single streaming pass keeping at
    most `K` closed entries (`interiorTop`) plus at most 2 seam groups
    (`first` and `last`). -/
def streamWorker (k : Nat) (chunk : List Entry) : WorkerSummary :=
  let rec go (first : Option Entry) (cur : Option Entry) (closed : List Entry) :
      List Entry → WorkerSummary
    | [] =>
        match first, cur with
        | none, none => { seams := [], interiorTop := closed }
        | none, some c => { seams := [c], interiorTop := closed }
        | some f, some c => { seams := [f, c], interiorTop := closed }
        | some f, none => { seams := [f], interiorTop := closed }
    | (key, val) :: rest =>
        match cur with
        | none => go none (some (key, val)) closed rest
        | some (ck, cv) =>
            if key == ck then
              go first (some (ck, cv + val)) closed rest
            else
              match first with
              | none => go (some (ck, cv)) (some (key, val)) closed rest
              | some _ => go first (some (key, val)) (insertTopK k (ck, cv) closed) rest
  go none none [] chunk

def combineTwo (k : Nat) (cur : Option Entry) (top : List Entry)
    (e : Entry) : Option Entry × List Entry :=
  match cur with
  | none => (some e, top)
  | some (ck, cv) =>
      if ck == e.1 then (some (ck, cv + e.2), top)
      else (some e, insertTopK k (ck, cv) top)

/-- Coordinator: merge worker-local bounded Top-K lists and stitch only
    adjacent boundary seams across chunks. -/
def coordMerge (k : Nat) (workers : List WorkerSummary) : List Entry :=
  let (tailOpt, top) :=
    workers.foldl
      (fun (curOpt, topAcc) w =>
        let top1 := mergeTopK k topAcc w.interiorTop
        match w.seams with
        | [] => (curOpt, top1)
        | [single] => combineTwo k curOpt top1 single
        | f :: l :: _ =>
            let (midOpt, top2) := combineTwo k curOpt top1 f
            let top3 :=
              match midOpt with
              | none => top2
              | some m => insertTopK k m top2
            (some l, top3))
      (none, [])
  match tailOpt with
  | none => top
  | some tail => insertTopK k tail top

def optStreamTopK (k : Nat) (chunks : List (List Entry)) : List Entry :=
  coordMerge k (chunks.map (streamWorker k))

/-- Streaming monotonic-key Top-K group-by (TPC-H Q03 on `lineitem.l_orderkey`
    joining `orders`): under globally non-decreasing keys across worker
    chunks, each worker buffers at most `K` closed groups while boundary seam
    stitching reproduces exact full-table grouped Top-K. -/
def Goal : Prop :=
  ∀ (k : Nat) (chunks : List (List Entry)),
    (chunks.flatten.map Prod.fst).Pairwise (fun a b => a ≤ b) →
      optStreamTopK k chunks = refTopK k chunks ∧
      ∀ chunk ∈ chunks, (streamWorker k chunk).interiorTop.length ≤ k

example :
    let chunks : List (List Entry) :=
      [ [(1, 10), (1, 5), (2, 100), (3, 1)]
      , [(3, 49)]
      , [(3, 20), (4, 60), (4, 5)] ]
    optStreamTopK 2 chunks = refTopK 2 chunks ∧
    optStreamTopK 2 chunks = [(2, 100), (3, 70)] := by
  decide

example :
    optStreamTopK 0 [[(1, 10), (2, 20)]] = [] ∧
    optStreamTopK 3 ([[]] : List (List Entry)) = [] := by
  decide

end TinylambOpt.ResearchGoals.MonotonicKeyStreamTopK
