import TinylambOpt.ResearchGoals.AnalyticsWorkloads.Common

/-!
These models connect SQL operators to their reference rows, order, or aggregate results.
-/

namespace TinylambOpt.ResearchGoals.AnalyticalOperators.RevenueAggregatesAndJoinWitnesses
open AnalyticsWorkloads

/-- Q8: use supplier multiplicities split by country for market-share numerator
and denominator. CASE ELSE 0 contributes a non-NULL zero even for NULL volume.
The resulting pair is sufficient for the original final division. -/
def MarketShareWeights.Goal : Prop := ∀ (sales : List (Nat × Option Int))
    (supplier : List (Nat × Option String)) country,
  let allRows := histogram (supplier.map Prod.fst)
  let chosen := histogram ((supplier.filter (fun s => s.2 == some country)).map Prod.fst)
  let optimized := sales.foldl (fun (num,den) (k,v) =>
    let w := allRows.getD k 0
    let p := chosen.getD k 0
    (plus num (plus (weighted p v) (weighted (w-p) (some 0))),plus den (weighted w v))) (zero,zero)
  let joined := sales.flatMap (fun (k,v) =>
    (supplier.filter (·.1 == k)).map (fun s => (if s.2 == some country then v else some 0,v)))
  optimized = (summarize (joined.map Prod.fst),summarize (joined.map Prod.snd))

/-- Q8: use a route table (order,year) carrying the multiplicity of qualifying
customer/region paths. A fact row probes once per requested output year. -/
def YearRoutes.Goal : Prop := ∀ (orders : List (Nat × Nat × Int)) (qualifiedCustomers : List Nat)
    (lines : List (Nat × Option Int)) year,
  let customers := histogram qualifiedCustomers
  let routes := orders.foldl (fun (m : Std.HashMap (Nat × Int) Nat) (o,c,y) =>
    m.insert (o,y) (m.getD (o,y) 0 + customers.getD c 0)) {}
  lines.foldl (fun st (o,v) => plus st (weighted (routes.getD (o,year) 0) v)) zero =
    summarize (lines.flatMap (fun (o,v) =>
      (orders.filter (fun (k,_,y) => o == k && y == year)).flatMap (fun (_,c,_) =>
        (qualifiedCustomers.filter (· == c)).map (fun _ => v))))

/-- Q7: compile both named nations to direction bits. Discard unmapped labels
(including NULL) before testing the two opposite routes. Each year still has two
distinct directed revenue buckets. -/
def side (a b : String) (s : Option String) : Option Bool :=
  if s == some a then some false else if s == some b then some true else none
abbrev Shipment := Option String × Option String × Int × Option Int
def DirectionBuckets.Goal : Prop := ∀ a b (rows : List Shipment) flag year, a ≠ b →
  let pick := rows.filterMap (fun (s,c,y,v) => do
    let src ← side a b s
    let dst ← side a b c
    if src != dst && src == flag && y == year then some v else none)
  let (fromName,toName) := if flag then (b,a) else (a,b)
  summarize pick = summarize ((rows.filter (fun (s,c,y,_) =>
    s == some fromName && c == some toName && y == year)).map (fun r => r.2.2.2))

/-- Q4: compile late-line EXISTS as a per-order maximum of *paired* date
differences. Independent extrema from different lines can give a spurious witness.
Accept order keys with positive slack, retaining duplicate outer orders. -/
abbrev LineDates := Nat × Option Int × Option Int
def slack (r : LineDates) : Option Int := do return (← r.2.2) - (← r.2.1)
def LateSlack.Goal : Prop := ∀ (lines : List LineDates) (orders : List Nat),
  let latest := lines.foldl (fun (m : Std.HashMap Nat Int) r =>
    match slack r with
    | none => m
    | some d => m.insert r.1 (max (m.getD r.1 d) d)) {}
  orders.filter (fun k => (latest[k]?).any (0 < ·)) =
    orders.filter (fun k => lines.any (fun (o,c,r) => o == k && c.any (fun x => r.any (x < ·))))

/-- Q4: populate a demand set of date-qualified orders; retire each on the first
late line and return its full outer multiplicity per priority, including repeated
order/priority pairs. Once retired a key cannot contribute again. -/
def RetireWitness.Goal : Prop := ∀ (orders : List (Nat × Nat)) (lateKeys : List Nat) priority,
  let pending := (orders.map Prod.fst).foldl (fun (s : Std.HashSet Nat) k => s.insert k) {}
  let weights := histogram ((orders.filter (·.2 == priority)).map Prod.fst)
  let (left,n) := lateKeys.foldl (fun (todo,n) k =>
    if todo.contains k then (todo.erase k,n + weights.getD k 0) else (todo,n)) (pending,0)
  n = orders.countP (fun (k,p) => p == priority && lateKeys.contains k) ∧
    left.size = ((orders.map Prod.fst).eraseDups.filter (fun k => !lateKeys.contains k)).length

/-- Q13: correct a precomputed histogram of order counts using the customers
touched by the excluded comment pattern. Rebucket each such customer once, from
its full count to its retained count. Customer multiplicities and zero survive. -/
def RebucketOrders.Goal : Prop := ∀ (customers : List Nat) (allOrders : List (Nat × Bool)) bucket,
  let full := histogram (allOrders.map Prod.fst)
  let bad := histogram ((allOrders.filter Prod.snd).map Prod.fst)
  let cw := histogram customers
  let old := customers.countP (fun c => full.getD c 0 == bucket)
  let repaired := bad.toList.foldl (fun (n : Int) (c,d) =>
    let count := full.getD c 0
    let w : Int := cw.getD c 0
    n - (if count == bucket then w else 0) + (if count-d == bucket then w else 0)) (old:Int)
  repaired = (customers.countP (fun c =>
    allOrders.countP (fun (k,excluded) => k == c && !excluded) == bucket) : Int)

/-- Q3: when eligible lineitems of an order are contiguous, finish its complete
revenue in one run. Different orders on the same date remain separate. -/
def streamRevenue (rows : List (Nat × Option Int)) :=
  rows.foldl (fun acc (k,v) => match acc with
    | [] => [(k,weighted 1 v)]
    | (g,st)::done_ => if k == g then (g,plus st (weighted 1 v))::done_
                        else (k,weighted 1 v)::acc) []
/-- Local run lists are stored last-key first. Only the new morsel's first
group can straddle the boundary with the accumulated morsels. -/
def mergeBoundary (old incoming : List (Nat × Totals)) :=
  match old,incoming.reverse with
  | [],_ => incoming
  | _,[] => old
  | (k,a)::tail,(j,b)::next =>
    if k == j then next.reverse ++ ((k,plus a b)::tail) else incoming ++ old

def RevenueRuns.Goal : Prop := ∀ (chunks : List (List (Nat × Option Int))) k,
  chunks.flatten.Pairwise (fun a b => a.1 ≤ b.1) →
  let out := (chunks.map streamRevenue).foldl mergeBoundary []
  let vs := (chunks.flatten.filter (·.1 == k)).map Prod.snd
  out = streamRevenue chunks.flatten ∧
    out.lookup k = (if vs.isEmpty then none else some (summarize vs)) ∧
    out.length = (chunks.flatten.map Prod.fst).eraseDups.length

#guard mergeBoundary (streamRevenue [(1,some 3),(2,none)]) (streamRevenue [(2,some 4),(3,none)]) ==
  streamRevenue [(1,some 3),(2,none),(2,some 4),(3,none)]
#guard side "A" "B" none == none && side "A" "B" (some "A") == some false
#guard slack (1,some 3,some 2) == some (-1) && slack (1,none,some 2) == none
#guard (streamRevenue [(1,none),(1,some 0),(2,none)]).lookup 1 == some ⟨2,1,0⟩

end TinylambOpt.ResearchGoals.AnalyticalOperators.RevenueAggregatesAndJoinWitnesses
