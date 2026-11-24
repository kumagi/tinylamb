import TinylambOpt.ResearchGoals.CloudBI.Quotes

namespace TinylambOpt.ResearchGoals.DuckDBGaps.Refresh
open AnalyticsWorkloads
open CloudBI.Quotes

abbrev Key := Nat × Nat  -- (batch,symbol)
abbrev Groups := Std.HashMap Key Totals
def push (m : Groups) (r : Tick) :=
  let k := (r.batch,r.sym)
  m.insert k (plus (m.getD k zero) (weighted 1 r.bid))
def atKey (xs : List Tick) (k : Key) := stats (xs.filter (fun r => (r.batch,r.sym) == k))

/-- The writer can produce rollup rows while inserting into the raw table. An
append may target a previously published batch. Absent and all-NULL groups differ. -/
def InsertSideState.Goal : Prop := ∀ (old fresh : List Tick) (m : Groups),
  (∀ k, m[k]? = (if (atKey old k).cnt == 0 then none else some (atKey old k))) →
  ∀ k, let a := atKey (old ++ fresh) k
       (fresh.foldl push m)[k]? = (if a.cnt == 0 then none else some a)

/-- Correct a published batch without rescanning other batches. Replace just the
changed symbol contributions, removing an empty group when its count reaches zero. -/
def replaceState (previous removed added : Totals) : Option Totals :=
  let n := previous.cnt - removed.cnt + added.cnt
  if n == 0 then none else some ⟨n,previous.nn-removed.nn+added.nn,
                                   previous.sum-removed.sum+added.sum⟩
def ChangedSymbols.Goal : Prop := ∀ (before after unaffected : List Tick) k,
  replaceState (atKey (unaffected ++ before) k) (atKey before k) (atKey after k) =
    (let a := atKey (unaffected ++ after) k
     if a.cnt == 0 then none else some a)

/-- A retried batch may overlap the published prefix. Fold only the part of its
contiguous ordinal interval above the committed frontier. No assumption about price uniqueness. -/
def OverlapReplay.Goal : Prop := ∀ (source : List Tick) offset acknowledged size k,
  offset ≤ acknowledged → acknowledged ≤ offset+size →
  plus (atKey (source.take acknowledged) k)
    (atKey (((source.drop offset).take size).drop (acknowledged-offset)) k) =
      atKey (source.take (offset+size)) k

/-- Accumulate in ordinal lanes for a dense symbol interval. Split distant symbols
into a spill map; keys in the range are rebased only after the lower bound check. -/
def lane (base width sym : Nat) : Sum Nat Nat :=
  if base ≤ sym && sym < base+width then .inl (sym-base) else .inr sym
def LaneOrSpill.Goal : Prop := ∀ (xs : List Tick) batch base width sym,
  let target := xs.filter (·.batch == batch)
  let (lanes,spill) := target.foldl (fun (a,m) r =>
    let v := weighted 1 r.bid
    match lane base width r.sym with
    | .inl i => (a.modify i (fun st => plus st v),m)
    | .inr k => (a,m.insert k (plus (m.getD k zero) v)))
    (Array.replicate width zero,({} : Std.HashMap Nat Totals))
  (match lane base width sym with
   | .inl i => lanes[i]?.getD zero
   | .inr k => spill.getD k zero) = atKey xs (batch,sym)

/-- Coalesce group writes across several ready batches. A reader with an older pin
must see the prefix state. Coalescing by symbol alone is valid only inside the pin. -/
def VisibleFlush.Goal : Prop := ∀ (chunks : List (Nat × List Tick)) (pin sym : Nat),
  let sealed := chunks.filter (fun c => c.1 ≤ pin)
  let published := sealed.map (fun c => stats (c.2.filter (·.sym == sym)))
  published.foldl plus zero =
    stats ((chunks.flatMap (fun (version,rs) => rs.map (fun r => (version,r)))).filterMap
      (fun (version,r) => if version ≤ pin && r.sym == sym then some r else none))

/-- Publish only changed keys to the summary sink. Repeated symbols require one
write after the morsel fold. The emitted row contains both the old and new contribution. -/
def flush (old : Groups) (xs : List Tick) :=
  let finished := xs.foldl push old
  let touched := (xs.map (fun r => (r.batch,r.sym))).eraseDups
  touched.map (fun k => (k,finished.getD k zero))
def DirtyFlush.Goal : Prop := ∀ old xs k,
  let changes := flush old xs
  (changes.lookup k).getD (old.getD k zero) = plus (old.getD k zero) (atKey xs k) ∧
    (changes.map Prod.fst).Nodup ∧ changes.length ≤ xs.length

#guard lane 100 2 99 != lane 100 2 100 && lane 100 2 102 == .inr 102
#guard replaceState ⟨2,1,-8⟩ ⟨1,1,-8⟩ zero == some ⟨1,0,0⟩
#guard replaceState ⟨1,0,0⟩ ⟨1,0,0⟩ zero == none

end TinylambOpt.ResearchGoals.DuckDBGaps.Refresh
