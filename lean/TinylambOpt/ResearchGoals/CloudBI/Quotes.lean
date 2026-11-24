import TinylambOpt.ResearchGoals.AnalyticsWorkloads.Common

namespace TinylambOpt.ResearchGoals.CloudBI.Quotes
open AnalyticsWorkloads

/-- Commit sequence, event time and a feed tie-breaker are independent. Prices are
exact ticks. The model assumes append-only, committed rows in a pinned snapshot. -/
structure Tick where
  batch : Nat
  ts : Nat
  tie : Nat
  sym : Nat
  bid : Option Int
  ask : Option Int
  deriving BEq, DecidableEq

def stats (rows : List Tick) := summarize (rows.map Tick.bid)
def eligible (sym lo hi : Nat) (r : Tick) := r.sym == sym && lo ≤ r.ts && r.ts < hi

/-- Correct a stale dashboard using commit order even when new quotes are late in event time. -/
def StaleRollup.Goal : Prop := ∀ (rows : List Tick) sym lo hi watermark snapshot,
  watermark ≤ snapshot →
  let hits := rows.filter (eligible sym lo hi)
  plus (stats (hits.filter (·.batch ≤ watermark)))
       (stats (hits.filter (fun r => watermark < r.batch && r.batch ≤ snapshot))) =
    stats (hits.filter (·.batch ≤ snapshot))

/-- Out-of-order workers can publish a sparse set of complete batches. Count each
published tile once and scan holes in the same snapshot. -/
def SparseRefresh.Goal : Prop := ∀ (rows : List Tick) (sealed : List Nat) snapshot sym,
  let live := rows.filter (fun r => r.batch ≤ snapshot && r.sym == sym)
  plus (sealed.eraseDups.foldl (fun a b => plus a (stats (live.filter (·.batch == b)))) zero)
       (stats (live.filter (fun r => !sealed.contains r.batch))) = stats live

/-- OHLC close can merge local winners across reclustered runs. For tied event time,
the exchange sequence breaks ties. The price is never needed as a tie-breaker under
the functional dependency below. NULL bids do not enter this last-non-NULL close. -/
abbrev Close := Nat × Nat × Int

def latest (xs : List Close) : Option Close := xs.foldl (fun best x =>
  match best with
  | none => some x
  | some y => if x.1 > y.1 || (x.1 == y.1 && x.2.1 > y.2.1) then some x else best) none

def lastBid (rows : List Tick) := latest (rows.filterMap fun r => r.bid.map fun p => (r.ts,r.tie,p))

def CloseTournament.Goal : Prop := ∀ (runs : List (List Tick)) (input : List Tick), runs.flatten.Perm input →
  (∀ a ∈ runs.flatten, ∀ b ∈ runs.flatten, a.ts = b.ts → a.tie = b.tie → a.bid = b.bid) →
  latest (runs.filterMap lastBid) = lastBid input

/-- Answer arbitrary half-open time windows from whole fixed-width candles and the
two partial edges. Each event belongs to a tile by its event time, including late arrivals. -/
def CandleFringe.Goal : Prop := ∀ (rows : List Tick) width lo hi, 0 < width →
  let bucket := fun (r : Tick) => r.ts / width
  let whole := fun k => decide (lo ≤ k*width ∧ (k+1)*width ≤ hi)
  let ids := ((rows.map bucket).eraseDups.filter whole)
  plus (ids.foldl (fun a k => plus a (stats (rows.filter (fun r => bucket r == k)))) zero)
       (stats (rows.filter (fun r => lo ≤ r.ts && r.ts < hi && !whole (bucket r)))) =
    stats (rows.filter (fun r => lo ≤ r.ts && r.ts < hi))

/-- A spread aggregate can avoid materializing per-row differences. Count and sum
both columns under the intersection of validity masks so one-sided quotes stay NULL. -/
def SpreadMoments.Goal : Prop := ∀ (rows : List Tick),
  let paired := rows.filterMap (fun r => do return (← r.bid, ← r.ask))
  let ref := summarize (rows.map (fun r => do return (← r.ask) - (← r.bid)))
  ref.cnt = rows.length ∧ ref.nn = paired.length ∧
  ref.sum = (paired.map Prod.snd).sum - (paired.map Prod.fst).sum

/-- Prune a physically clustered run using its symbol fence. An overlapping fence
can have holes; rows still undergo the exact symbol and time predicate. -/
def FenceRead.Goal : Prop := ∀ (runs : List (List Tick)) sym lo hi,
  let opened := runs.filter (fun rs =>
    (rs.map Tick.sym).min?.any (· ≤ sym) && (rs.map Tick.sym).max?.any (sym ≤ ·))
  opened.flatten.filter (eligible sym lo hi) = runs.flatten.filter (eligible sym lo hi) ∧
  opened.length ≤ runs.length

/-- Replace old immutable runs atomically at a generation boundary. A physical
reordering must preserve multiplicities even for a reader pinned before reclustering. -/
def ReclusterSwap.Goal : Prop := ∀ (old fresh rest : List (List Tick)) cut pin sym lo hi,
  old.flatten.Perm fresh.flatten →
  let selected := (if pin < cut then old else fresh) ++ rest
  let readWindow := fun (rs : List Tick) =>
    stats (rs.filter (fun r => r.batch ≤ pin && eligible sym lo hi r))
  readWindow selected.flatten = readWindow (old ++ rest).flatten

/-- Idempotent refresh after retry. A batch identifier names an immutable payload;
two independent batches may have identical payloads and must still both count. -/
def RetryRefresh.Goal : Prop := ∀ (deliveries : List Nat) (payload : Nat → List Tick),
  let out := deliveries.foldl (fun (seen,acc) k =>
    if seen.contains k then (seen,acc) else (k::seen, plus acc (stats (payload k))))
    (([] : List Nat),zero)
  out.2 = stats (deliveries.eraseDups.flatMap payload) ∧ out.1.length = deliveries.eraseDups.length

/-- A reclustering budget can rebuild at most k independent runs at equal unit
cost. Rank by saved physical row reads over the *bag* of dashboard predicates. -/
def readWork (rs : List Tick) (queries : List Nat) :=
  queries.foldl (fun n k => if (rs.map Tick.sym).min?.any (· ≤ k) &&
    (rs.map Tick.sym).max?.any (k ≤ ·) then n + rs.length else n) 0

def ReclusterBudget.Goal : Prop := ∀ (runs : List (List Tick)) (queries : List Nat) k
    (replacement : Nat → List (List Tick)) (other : List Nat),
  (∀ i ∈ List.range runs.length, (replacement i).flatten.Perm (runs[i]?.getD [])) →
  let saving := fun i => readWork (runs[i]?.getD []) queries -
    ((replacement i).map (fun r => readWork r queries)).sum
  let ranked := (List.range runs.length).mergeSort (fun i j => saving i ≥ saving j)
  other.Nodup → (∀ i ∈ other, i < runs.length) → other.length ≤ k →
  (other.map saving).sum ≤ ((ranked.take k).map saving).sum

/-- Full-path score break-even for a more expensive maintained layout. Serving
saves s time units at extra fresh-path cost m, over the same requests, with exact
nonnegative units. Include the cross-term when deciding to buy maintenance. -/
def MaintenanceBreakEven.Goal : Prop := ∀ (fresh query time m s : Nat), s ≤ time →
  ((fresh + query + m) * (time - s) < (fresh + query) * time ↔
    m * (time - s) < (fresh + query) * s)

#guard lastBid [⟨2,7,2,0,some 4,none⟩,⟨1,7,1,0,some 9,none⟩] == some (7,2,4)
#guard (stats [⟨2,7,2,0,none,some 5⟩]) == ⟨1,0,0⟩

end TinylambOpt.ResearchGoals.CloudBI.Quotes
