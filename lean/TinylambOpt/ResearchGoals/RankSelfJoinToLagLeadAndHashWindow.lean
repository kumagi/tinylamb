import Std

namespace TinylambOpt.ResearchGoals.RankSelfJoinToLagLeadAndHashWindow

structure WRow where
  part : Int
  ord : Int
  val : Int
  deriving DecidableEq

def leWRow (a b : WRow) : Bool :=
  if a.part != b.part then decide (a.part ≤ b.part) else decide (a.ord ≤ b.ord)

def insertSortedW (x : WRow) : List WRow → List WRow
  | [] => [x]
  | y :: ys => if leWRow x y then x :: y :: ys else y :: insertSortedW x ys

def sortW : List WRow → List WRow
  | [] => []
  | x :: xs => insertSortedW x (sortW xs)

def partRank (sortedPart : List WRow) (targetOrd : Int) : Nat :=
  let rec go (idx prevRank : Nat) (prevOrd : Option Int) : List WRow → Nat
    | [] => 0
    | r :: rest =>
        let rn :=
          match prevOrd with
          | none => 1
          | some po => if po == r.ord then prevRank else idx + 1
        if r.ord == targetOrd then rn else go (idx + 1) rn (some r.ord) rest
  go 0 0 none sortedPart

def uniqueParts (rows : List WRow) : List Int :=
  (rows.map WRow.part).foldl
    (fun acc p => if acc.contains p then acc else acc ++ [p]) []

abbrev LagLeadTriple := WRow × Int × Int

def refRankSelfJoin (rows : List WRow) : List LagLeadTriple :=
  let sorted := sortW rows
  sorted.filterMap fun cur =>
    let pRows := sorted.filter (fun r => r.part == cur.part)
    let rCur := partRank pRows cur.ord
    let lagHit := pRows.find? (fun r => partRank pRows r.ord + 1 == rCur)
    let leadHit := pRows.find? (fun r => rCur + 1 == partRank pRows r.ord)
    match lagHit, leadHit with
    | some lg, some ld => some (cur, lg.val, ld.val)
    | _, _ => none

def adjTriplesForPart : List WRow → List LagLeadTriple
  | a :: b :: c :: rest => (b, a.val, c.val) :: adjTriplesForPart (b :: c :: rest)
  | _ => []

/-- Optimized single-pass partition sweep using adjacent `LAG`/`LEAD` offsets
    without computing `RANK()` or joining `V1 ⋈ V1_lag ⋈ V1_lead`. -/
def optLagLeadSweep (rows : List WRow) : List LagLeadTriple :=
  let sorted := sortW rows
  (uniqueParts sorted).flatMap fun pk =>
    adjTriplesForPart (sorted.filter (fun r => r.part == pk))

def UniquePartOrd (rows : List WRow) : Prop :=
  (rows.map (fun r => (r.part, r.ord))).Nodup

def orderlessSumCntSorted (rows : List WRow) (pk : Int) : Int × Nat :=
  let pRows := (sortW rows).filter (fun r => r.part == pk)
  (pRows.foldl (fun a r => a + r.val) 0, pRows.length)

def orderlessSumCntHash (rows : List WRow) (pk : Int) : Int × Nat :=
  rows.foldl
    (fun (s, c) r => if r.part == pk then (s + r.val, c + 1) else (s, c))
    (0, 0)

/-- Window rank self-join -> adjacent `LAG`/`LEAD` sweep (TPC-DS Q57) and
    orderless partition-window hash aggregation (TPC-DS Q63/Q89/Q86):
    (1) when `(part, ord)` pairs are unique, the 3-way self-join on
    `rn = rn_lag + 1 ∧ rn = rn_lead - 1` equals an adjacent 3-element window
    sweep without self-joins (whereas ties break equivalence because `RANK()`
    skips numbers), and (2) orderless `SUM/COUNT OVER (PARTITION BY part)`
    computed via unsorted hashing matches sorting by `part` first. -/
def Goal : Prop :=
  (∀ (rows : List WRow),
    UniquePartOrd rows →
      optLagLeadSweep rows = refRankSelfJoin rows) ∧
  (∀ (rows : List WRow) (pk : Int),
    orderlessSumCntHash rows pk = orderlessSumCntSorted rows pk) ∧
  (∃ (rows : List WRow), optLagLeadSweep rows ≠ refRankSelfJoin rows)

private def tiedRows : List WRow :=
  [{ part := 1, ord := 10, val := 100 }, { part := 1, ord := 10, val := 200 },
   { part := 1, ord := 20, val := 300 }]

example :
    let rows : List WRow :=
      [ { part := 2, ord := 5, val := 50 }
      , { part := 1, ord := 3, val := 30 }
      , { part := 1, ord := 1, val := 10 }
      , { part := 1, ord := 2, val := 20 }
      , { part := 1, ord := 4, val := 40 } ]
    optLagLeadSweep rows = refRankSelfJoin rows ∧
    orderlessSumCntHash rows 1 = orderlessSumCntSorted rows 1 := by
  decide

example :
    optLagLeadSweep tiedRows ≠ refRankSelfJoin tiedRows := by
  decide

end TinylambOpt.ResearchGoals.RankSelfJoinToLagLeadAndHashWindow
