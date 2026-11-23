import TinylambOpt.ResearchGoals.AnalyticsWorkloads.Common

namespace TinylambOpt.ResearchGoals.AnalyticsWorkloads.H2o
open TinylambOpt.ResearchGoals.BilateralEagerAggJoinPushdown

/-- H1. A register holds a sampled hot id and its count; only misses update the
generic table. Count the actual table updates as well as the output multiplicity. -/
def hotCount (xs : List Nat) (hot : Nat) :=
  xs.foldl (fun (hits,h,updates) k =>
    if k == hot then (hits+1,h,updates) else
      (hits,h.insert k (h.getD k 0+1),updates+1))
    (0,({} : Std.HashMap Nat Nat),0)

def HotRegister.Goal : Prop := ∀ (xs : List Nat) hot k,
  let (n,h,updates) := hotCount xs hot
  (if k == hot then n else h.getD k 0) = xs.count k ∧ updates + xs.count hot = xs.length

/-- H2. Bucket common small counts directly; sort only groups above t. Stable ties
preserve input order. No more than totalRows/(t+1) groups can enter that sort. -/
abbrev GroupCount := (Nat × Nat) × Nat
def hybridOrder (gs : List GroupCount) (t : Nat) :=
  let small := gs.foldl (fun (h : Std.HashMap Nat (List GroupCount)) g =>
    if g.2 ≤ t then h.insert g.2 (g::h.getD g.2 []) else h) {}
  let headRows := (gs.filter (fun g => t < g.2)).mergeSort (fun a b => b.2 ≤ a.2)
  headRows ++ (List.range (t+1)).reverse.flatMap (fun n => (small.getD n []).reverse)

def CountTail.Goal : Prop := ∀ gs t,
  hybridOrder gs t = gs.mergeSort (fun a b => b.2 ≤ a.2) ∧
  (gs.filter (fun g => t < g.2)).length * (t+1) ≤ (gs.map Prod.snd).sum

/-- H3. Columns v1..v3 may have different per-block frame-of-reference bases
and independent NULL masks. Add base*nonNullCount per group, after reducing codes. -/
abbrev Triple := Fin 3 → Option Int
abbrev Block := (Fin 3 → Int) × List (Nat × Triple)
def decodeTotal (b : Block) (g : Nat) (j : Fin 3) :=
  let st := summarize ((b.2.filter (fun row => row.1 == g)).map (fun row => row.2 j))
  {st with sum := st.sum + b.1 j * st.nn}

def ThreeBaseSums.Goal : Prop := ∀ (blocks : List Block) g j,
  (blocks.map (fun b => decodeTotal b g j)).foldl plus zero =
    summarize (blocks.flatMap fun (base,rows) =>
      (rows.filter (fun r => r.1 == g)).map fun r => (r.2 j).map (base j + ·))

/-- H4. A fresh generation lets us reuse a sparse AVG table without clearing it.
Record each touched gid once, even when the first qualifying payload is NULL. -/
abbrev Slots := Std.HashMap (Nat × Nat) (Nat × Totals)
abbrev Input := (Nat × Nat) × Int × Option Int

def accumulate (old : Slots) (epoch : Nat) (rs : List Input) :=
  rs.foldl (fun (h,keys) (k,id3,v) =>
    if id3 ≥ 4 then (h,keys) else
      match h[k]? with
      | some (stamp,s) => if stamp == epoch then (h.insert k (epoch,plus s (weighted 1 v)),keys)
                          else (h.insert k (epoch,weighted 1 v),k::keys)
      | none => (h.insert k (epoch,weighted 1 v),k::keys)) (old,[])

def EpochAverage.Goal : Prop := ∀ old epoch rs k,
  (∀ entry ∈ old.toList, entry.2.1 ≠ epoch) →
  let out := accumulate old epoch rs
  let keys := out.2
  let vals := (rs.filter (fun r => r.1 == k && r.2.1 < 4)).map (fun r => r.2.2)
  keys.Nodup ∧ (k ∈ keys ↔ vals ≠ []) ∧
    (if keys.contains k then (out.1.getD k (epoch,zero)).2 else zero) = summarize vals

/-- H5. Only hot id4 buckets pay for the (id1,id4) preaggregation table.
Cold probes directly consume a per-id4 right summary. Both sums have their own
NULL population; a zero-match group is absent, even if x has rows in it. -/
abbrev Groups := Std.HashMap Int (Totals × Totals)

def contribute (groups : Groups) (g : Int) (l r : Totals) :=
  if l.cnt == 0 || r.cnt == 0 then groups else
    let (a,b) := groups.getD g (zero,zero)
    let pairs := l.cnt*r.cnt
    groups.insert g (plus a ⟨pairs,l.nn*r.cnt,l.sum*r.cnt⟩,
      plus b ⟨pairs,r.nn*l.cnt,r.sum*l.cnt⟩)

def hybridGroups (ls : List LRow) (rs : List RRow) (hot : Int → Bool) :=
  let build := rs.foldl (fun (h : Std.HashMap Int Totals) r => match r.k with
    | none => h
    | some k => h.insert k (plus (h.getD k zero) (weighted 1 r.v))) {}
  let (partialByKey,cold) := ls.foldl (fun (partial_,groups) l => match l.k with
    | none => (partial_,groups)
    | some k =>
      let left := weighted 1 l.v
      if hot k then
        let pair := (l.g,k)
        (partial_.insert pair (plus (partial_.getD pair zero) left),groups)
      else (partial_,contribute groups l.g left (build.getD k zero)))
    (({} : Std.HashMap (Int × Int) Totals),({} : Groups))
  partialByKey.fold (fun groups (g,k) left =>
    contribute groups g left (build.getD k zero)) cold

def finish (groups : Groups) (g : Int) :=
  let (l,r) := groups.getD g (zero,zero)
  let sum := fun (a : Totals) => if a.nn == 0 then none else some a.sum
  (l.cnt,sum l,sum r)

def SkewHybrid.Goal : Prop := ∀ ls rs g hot,
  let out := hybridGroups ls rs hot
  let expected := refGroup ls rs g
  finish out g = expected ∧ out.contains g = decide (0 < expected.1)

/-- H6. A per-id1 cumulative directory over id2 gives an inclusive slice via two
prefixes. Repeated IN literals must not add a second slice. Payloads may be NULL. -/
abbrev Sample := Nat × Int × Option Int
def prefixAt (rs : List Sample) (a : Nat) (limit : Int) :=
  summarize ((rs.filter (fun (x,y,_) => x == a && y < limit)).map (fun r => r.2.2))

def prefixSlices (get : Nat → Int → Totals) (ids : List Nat) (lo hi : Int) :=
  ids.eraseDups.foldl (fun total_ a =>
    let l := get a lo
    let h := get a (hi+1)
    plus total_ ⟨h.cnt-l.cnt,h.nn-l.nn,h.sum-l.sum⟩) zero

def InRangeSynopsis.Goal : Prop := ∀ rows ids lo hi, lo ≤ hi →
  prefixSlices (prefixAt rows) ids lo hi = summarize ((rows.filter
    (fun (a,b,_) => ids.contains a && lo ≤ b && b ≤ hi)).map (fun r => r.2.2))

#guard (let (n,h,u) := hotCount [3,1,3,2,3] 3; (n,h.size,u) == (3,2,2))
#guard hybridOrder [((0,0),2),((1,1),6),((2,2),2),((1,2),1)] 2 ==
  [((1,1),6),((0,0),2),((2,2),2),((1,2),1)]
#guard decodeTotal ((fun _ => -3),[(1,fun _ => some 3),(1,fun _ => none)]) 1 0 == ⟨2,1,0⟩
#guard (
  let (h,ks) := accumulate (Std.HashMap.ofList [((1,2),(3,weighted 9 (some 7)))]) 4
    [((1,2),3,none),((1,2),4,some 80)]
  ks == [(1,2)] && (h.getD (1,2) (0,zero)).2 == ⟨1,0,0⟩)
#guard (
  let groups := hybridGroups [⟨1,some 2,none⟩,⟨1,some 3,some 4⟩]
    [⟨some 2,some 0⟩,⟨some 3,none⟩,⟨some 3,none⟩] (· == 2)
  finish groups 1 == (3,some 8,some 0) && !groups.contains 2)
#guard prefixSlices (prefixAt [(1,10,none),(1,40,some 2),(1,41,some 99)]) [1,1] 10 40 == ⟨2,1,2⟩

end TinylambOpt.ResearchGoals.AnalyticsWorkloads.H2o
