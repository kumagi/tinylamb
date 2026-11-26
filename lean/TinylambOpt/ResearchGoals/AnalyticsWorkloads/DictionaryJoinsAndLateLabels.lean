import TinylambOpt.ResearchGoals.AnalyticsWorkloads.Common

/-!
These models describe plan alternatives for analytical SQL, with row multiplicity and
aggregate state made explicit.
-/

namespace TinylambOpt.ResearchGoals.AnalyticsWorkloads.DictionaryJoinsAndLateLabels

/-- B1/B2. Canonical page-N URLs become integer keys; all other strings stay in a
disjoint representation. For B1 GROUP BY, NULL is a key; for B2 SQL '=' discard NULL. -/
inductive Url where
  | nullKey | page (id : Nat) | literal (s : String)
  deriving BEq, DecidableEq, Hashable

def encode : Option String → Url
  | none => .nullKey
  | some s => match (s.drop 5).toString.toNat? with
    | some n => if "page-" ++ toString n == s then .page n else .literal s
    | none => .literal s

def decode : Url → Option String
  | .nullKey => none | .page n => some ("page-" ++ toString n) | .literal s => some s

def UrlGrouping.Goal : Prop := ∀ (visits : List (Option String × Option Int)) key,
  let h := visits.foldl (fun (m : Std.HashMap Url Totals) (u,v) =>
    let k := encode u
    m.insert k (plus (m.getD k zero) (weighted 1 v))) {}
  decode (encode key) = key ∧ h.getD (encode key) zero =
    summarize ((visits.filter (fun r => r.1 == key)).map Prod.snd)

/-- B2. Translate a *local* URL code into the number of qualifying ranking rows once
per touched code; store misses explicitly. Code-to-text translation belongs to this
block's dictionary. The counter measures lookups of the string-key build histogram. -/
abbrev Visit := Nat × String × Option Int -- URL code, country, revenue
def translate (dict : Array (Option String)) (ranks : List (Option String × Int))
    (vs : List Visit) :=
  let build := histogram ((ranks.filter (fun r => 40 < r.2)).filterMap Prod.fst)
  let (_,groups,lookups) := vs.foldl (fun (cache,gs,n) (code,c,v) =>
    let (next,w,calls) := match cache[code]? with
      | some w => (cache,w,0)
      | none =>
        let url := dict[code]?.join
        let w := url.elim 0 (build.getD · 0)
        (cache.insert code w,w,url.isSome.toNat)
    let gs := if w == 0 then gs else gs.insert c (plus (gs.getD c zero) (weighted w v))
    (next,gs,n+calls)) (({} : Std.HashMap Nat Nat),({} : Std.HashMap String Totals),0)
  (groups,lookups)

def CodeFanout.Goal : Prop := ∀ dict ranks vs country,
  let (gs,n) := translate dict ranks vs
  let selected := vs.filter fun r => r.2.1 == country
  gs.getD country zero = summarize (selected.flatMap fun (code,_,v) =>
    let key := dict[code]?.join
    (ranks.filter (fun (s,rank) => s.isSome && s == key && rank > 40)).map fun _ => v) ∧
    n = ((vs.map Prod.fst).eraseDups.countP fun i => dict[i]?.join.isSome)

/-- B3. Apply filtered customer multiplicities before accumulating sales by
(store-id,item-id,year). Resolve region/category labels only for occupied cells;
both dimension lookups can have multiple rows or repeated projected labels. -/
abbrev Sale := Nat × Nat × Nat × Int × Option Int -- store, item, customer, year, revenue
abbrev Dim := List (Nat × String)
def cells (sales : List Sale) (eligible : List Nat) :=
  let customers := histogram eligible
  sales.foldl (fun (m : Std.HashMap (Nat × Nat × Int) Totals) (s,i,c,y,v) =>
    let w := customers.getD c 0
    let k := (s,i,y)
    if 2011 ≤ y && y ≤ 2016 && w > 0 then m.insert k (plus (m.getD k zero) (weighted w v))
    else m) {}

def lookupLabel (d : Dim) (label : String) := histogram ((d.filter (fun e => e.2 == label)).map Prod.fst)

def LateLabels.Goal : Prop := ∀ sales eligible (stores items : Dim) region category year,
  let sw := lookupLabel stores region
  let iw := lookupLabel items category
  ((cells sales eligible).toList.foldl (fun acc ((s,i,y),a) =>
    let w := sw.getD s 0 * iw.getD i 0
    if y == year then plus acc ⟨a.cnt*w,a.nn*w,a.sum*w⟩ else acc) zero) =
    summarize (sales.flatMap fun (s,i,c,y,v) =>
      if y == year && 2011 ≤ y && y ≤ 2016 then
        (eligible.filter (· == c)).flatMap fun _ =>
        (stores.filter (· == (s,region))).flatMap fun _ =>
        (items.filter (· == (i,category))).map fun _ => v
      else [])

/-- B4. Short-circuit US before requesting a dictionary LIKE evaluation. Each distinct
non-NULL word demanded by the remaining rows is tested once, including false results. -/
abbrev Search := Option String × Option String × Option Int -- country, search word, revenue
def searchAgg (rows : List Search) :=
  let (_,st,calls) := rows.foldl (fun (cache,total_,n) (country,word,v) =>
    if country == some "US" then (cache,plus total_ (weighted 1 v),n) else
      match word with
      | none => (cache,total_,n)
      | some w => match cache[w]? with
        | some ok => (cache,if ok then plus total_ (weighted 1 v) else total_,n)
        | none =>
          let ok := w.contains "data"
          (cache.insert w ok,if ok then plus total_ (weighted 1 v) else total_,n+1))
    (({} : Std.HashMap String Bool),zero,0)
  (st,calls)

def ResidualWords.Goal : Prop := ∀ rows,
  let kept := rows.filter fun (c,w,_) => c == some "US" || w.any (·.contains "data")
  searchAgg rows = (summarize (kept.map (fun r => r.2.2)),
    (((rows.filter (fun r => r.1 != some "US")).filterMap (fun r => r.2.1)).eraseDups).length)

/-- B5. A price synopsis for each item id returns the qualifying fanout directly
for uniform buckets. Store the non-NULL count, not COUNT(*), for an all-above bucket. -/
structure Prices where
  low : Option Int
  high : Option Int
  nonNull : Nat
def prices (xs : List (Option Int)) : Prices :=
  let vs := xs.filterMap id
  ⟨vs.min?,vs.max?,vs.length⟩
def priceCount (p : Prices) (xs : List (Option Int)) (cut : Int) :=
  if p.high.all (· ≤ cut) then (0,false)
  else if p.low.any (cut < ·) then (p.nonNull,false)
  else (xs.countP (fun x => x.any (cut < ·)),true)

def PriceSynopsis.Goal : Prop := ∀ xs cut,
  let (n,scanned) := priceCount (prices xs) xs cut
  n = xs.countP (fun x => x.any (cut < ·)) ∧
    (scanned = true ↔ (∃ x ∈ xs, x.any (· ≤ cut) = true) ∧ (∃ x ∈ xs, x.any (cut < ·) = true))

#guard encode (some "page-01") != encode (some "page-1") && decode (encode (some "page-0")) == some "page-0"
#guard (
  let (gs,n) := translate #[some "p",none,some "p"] [(some "p",41),(some "p",90),(none,99),(some "p",40)]
    [(0,"US",none),(0,"GB",some 1),(1,"US",some 8),(2,"US",some 2)]
  gs.getD "US" zero == ⟨4,2,4⟩ && gs.getD "GB" zero == ⟨2,2,2⟩ && n == 2)
#guard (cells [(2,3,1,2015,none),(2,3,1,2015,some 0),(2,3,1,2017,some 5)] [1,1]).getD
  (2,3,2015) zero == ⟨4,2,0⟩
#guard searchAgg [(some "US",some "uncached",none),(none,some "metadata",some 3),
  (some "GB",some "metadata",some 2),(none,none,some 10)] == (⟨3,2,5⟩,1)
#guard priceCount (prices [some 101,none,some 102]) [] 100 == (2,false) &&
  priceCount (prices [some 100,some 101]) [some 100,some 101] 100 == (1,true)

end TinylambOpt.ResearchGoals.AnalyticsWorkloads.DictionaryJoinsAndLateLabels
