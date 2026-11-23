import TinylambOpt.ResearchGoals.AnalyticsWorkloads.Common
import TinylambOpt.ResearchGoals.CheckedAggregateTree
import TinylambOpt.ResearchGoals.ColumnarToPgWireChunkedFramingEquiv

namespace TinylambOpt.ResearchGoals.AnalyticsWorkloads.PhysicalChoices

/-- J2/C1/H5. Keep exact heavy-key frequencies and aggregate the remaining keys
by hash bucket. Colliding tail keys give a safe upper bound. Promoting any one key
must tighten (or preserve) the interval, even if the key occurs on only one side. -/
def joinBounds (l r : List Nat) (heavy : List Nat) (bucket : Nat → Nat) :=
  let lh := histogram l
  let rh := histogram r
  let exact := (heavy.eraseDups.map fun k => lh.getD k 0 * rh.getD k 0).sum
  let tail := fun (xs : List Nat) => histogram ((xs.filter (fun k => !heavy.contains k)).map bucket)
  let right := tail r
  (exact,exact + ((tail l).toList.map fun (b,n) => n * right.getD b 0).sum)

def RefineFanout.Goal : Prop := ∀ l r heavy bucket k,
  let (lo,hi) := joinBounds l r heavy bucket
  let (lo',hi') := joinBounds l r (k::heavy) bucket
  let pairs := (l.map r.count).sum
  lo ≤ lo' ∧ lo' ≤ pairs ∧ pairs ≤ hi' ∧ hi' ≤ hi

/-- J1/B3. Order two pure dimension probes in one batch. Cache hits AND zero
multiplicities. The second cache sees only distinct keys surviving the first probe;
a row-selectivity estimate alone need not choose the cheaper physical order. -/
def lookup (cache : Std.HashMap Nat Nat) (f : Nat → Nat) (k : Nat) :=
  match cache[k]? with
  | some n => (cache,n,0)
  | none => let n := f k; (cache.insert k n,n,1)

structure ProbeRun where
  weights : List Nat
  firstLoads : Nat
  secondLoads : Nat
  deriving BEq, DecidableEq

def probeOrder (rows : List (Nat × Nat)) (a b : Nat → Nat) : ProbeRun :=
  let (_,_,out,ca,cb) := rows.foldl (fun (ma,mb,rev,na,nb) (x,y) =>
    let (ma,w,i) := lookup ma a x
    if w == 0 then (ma,mb,0::rev,na+i,nb) else
      let (mb,v,j) := lookup mb b y
      (ma,mb,(w*v)::rev,na+i,nb+j)) ({},{},[],0,0)
  ⟨out.reverse,ca,cb⟩

def orderCost (rs : List (Nat × Nat)) (first : Nat → Nat) (c1 c2 : Nat) :=
  c1*(rs.map Prod.fst).eraseDups.length +
  c2*((rs.filter (fun r => first r.1 != 0)).map Prod.snd).eraseDups.length

def CachedProbeOrder.Goal : Prop := ∀ rs a b c1 c2,
  let swapped := rs.map Prod.swap
  let ab := probeOrder rs a b
  let ba := probeOrder swapped b a
  ab.weights = rs.map (fun (p,q) => a p * b q) ∧ ba.weights = ab.weights ∧
    c1*ab.firstLoads + c2*ab.secondLoads = orderCost rs a c1 c2 ∧
    c2*ba.firstLoads + c1*ba.secondLoads = orderCost swapped b c2 c1

open TinylambOpt.ResearchGoals.CheckedAggregateTree (Row Summary prepare sequential)
open TinylambOpt.CheckedEagerSum (InRange)

/-- H5/L2. Under a fixed physical pair order, a consecutive replay of the same
build segment can be checked as one operation. Extremal prefixes occur in the
first or last copy depending on the sign of its total, including zero drift. -/
def repeated (s : Summary) (n : Nat) : Summary :=
  if n == 0 then ⟨0,0,0,0⟩ else
    let drift := (n-1 : Nat) * s.total
    ⟨n*s.total,s.low + min 0 drift,s.high + max 0 drift,n*s.count⟩

def RepeatEnvelope.Goal : Prop := ∀ (rs : List Row) (n : Nat) (acc : Option Int), InRange (acc.getD 0) →
  let h := repeated (prepare (.chunk rs)).summary n
  let value := if h.count == 0 then acc else some (acc.getD 0 + h.total)
  (InRange (acc.getD 0 + h.low) ∧ InRange (acc.getD 0 + h.high) ↔
    sequential acc (List.replicate n rs).flatten = .ok value)

open TinylambOpt.ResearchGoals.ColumnarToPgWireChunkedFramingEquiv
  (Cell encodeCellBytes be16 be32 refRowEncode)

/-- H2/H4/L2/C2. Cache complete length-prefixed text fields for a chosen set of
integer values. Preserve DataRow bytes, including NULL, negative values and duplicates.
Count decimal conversions; noninteger fields do not enter this cache. -/
abbrev FieldCache := Std.HashMap Int (List Nat)
def fields (hot : Int → Bool) (cache : FieldCache) (row : List Cell) :=
  row.foldl (fun (h,rev,miss) c => match c with
    | .intVal (some v) =>
      if hot v then match h[v]? with
        | some bytes => (h,bytes::rev,miss)
        | none => let bytes := encodeCellBytes c; (h.insert v bytes,bytes::rev,miss+1)
      else (h,encodeCellBytes c::rev,miss+1)
    | _ => (h,encodeCellBytes c::rev,miss)) (cache,[],0)

def cachedWire (hot : Int → Bool) (rows : List (List Cell)) :=
  let (_,frames,n) := rows.foldl (fun (h,rev,total_) row =>
    let (next,parts,calls) := fields hot h row
    let payload := parts.reverse.flatten
    let frame := 68 :: (be32 (6+payload.length) ++ be16 row.length ++ payload)
    (next,frame::rev,total_+calls)) ({},[],0)
  (frames.reverse.flatten,n)

def FieldBytes.Goal : Prop := ∀ rows hot,
  let ints := rows.flatten.filterMap (fun c => match c with | .intVal v => v | _ => none)
  cachedWire hot rows = (refRowEncode rows,
    (ints.filter hot).eraseDups.length + ints.countP (fun i => !hot i))

/-- L1/L3. Split a high-degree incoming posting list into independent bounded morsels.
A repeated payload in two slots remains two join occurrences. Empty lists schedule
no jobs, and ceiling-rounding costs fewer than one extra job per nonempty list. -/
def slices (cap : Nat) (adj : List (Nat × List Nat)) :=
  adj.flatMap fun (key,vs) => (List.range ((vs.length+cap-1)/cap)).map fun j =>
    (key,(vs.drop (j*cap)).take cap)

def EdgeMorsels.Goal : Prop := ∀ adj cap, 0 < cap →
  let jobs := slices cap adj
  let expand := fun (x : List (Nat × List Nat)) => x.flatMap (fun (k,ns) => ns.map (k, ·))
  expand jobs = expand adj ∧ (∀ j ∈ jobs, 0 < j.2.length ∧ j.2.length ≤ cap) ∧
    jobs.length * cap ≤ (expand adj).length + (cap-1) * adj.countP (fun a => !a.2.isEmpty)

#guard joinBounds [1,1,2] [1,3,3] [] (fun _ => 0) == (0,9) &&
  joinBounds [1,1,2] [1,3,3] [1,1] (fun _ => 0) == (2,4)
#guard probeOrder [(0,1),(0,2),(3,1),(3,1)] (fun k => if k == 3 then 2 else 0) (fun _ => 3)
  == ⟨[0,0,6,6],2,1⟩
#guard (let h := repeated (prepare (.chunk [⟨0,some 4⟩,⟨1,some (-5)⟩])).summary 3
  (h.total,h.low,h.high,h.count) == (-3,-3,4,6))
private def wireRows : List (List Cell) := [[.intVal (some (-1)),.intVal none,.strVal (some [])],
  [.intVal (some (-1)),.intVal (some 5),.intVal (some 5)]]
#guard cachedWire (· < 0) wireRows == (refRowEncode wireRows,3)
#guard slices 2 [(3,[4,4,2]),(1,[]),(3,[4])] == [(3,[4,4]),(3,[2]),(3,[4])]

end TinylambOpt.ResearchGoals.AnalyticsWorkloads.PhysicalChoices
