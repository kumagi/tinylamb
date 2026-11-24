import Std

namespace TinylambOpt.ResearchGoals.CloudBI.PublicBI

/-- Nullable dimensions are group keys. A comparison with a non-NULL literal
qualifies exactly its some value. Measures use widened fixed-point coefficients. -/
abbrev Key := Option String × Option String × Option Int
structure Row where
  key : Key
  values : List (Option Int)
  deriving BEq, DecidableEq

abbrev Ext := Option Int × Option Int -- MAX, MIN, as projected by Q108
def extrema (vs : List (Option Int)) : Ext :=
  let xs := vs.filterMap id
  (xs.max?,xs.min?)

def cell (rs : List Row) (col : Nat) := extrema (rs.map (fun r => (r.values[col]?).join))
def attrs (rs : List Row) (cols : List Nat) := cols.map (cell rs)

/-- After equality restrictions fix all three keys, one aggregate state suffices for
Q108. Preserve an absent group and an existing group with entirely NULL measures. -/
def ConstantGroup.Goal : Prop := ∀ (rows : List Row) (l p : String) (y : Int) cols,
  let k : Key := (some l,some p,some y)
  let selected := rows.filter (·.key == k)
  let grouped := (selected.map Row.key).eraseDups.map (fun g =>
    (g, attrs (selected.filter (·.key == g)) cols))
  grouped = (if selected.isEmpty then [] else [(k,attrs selected cols)])

/-- Partial Q108 hits in many blocks: merge per-column extrema once per occupied
block while retaining group presence separately from the validity of all measures. -/
def mergeExt (x y : Ext) : Ext := ((([x.1,y.1]).filterMap id).max?, (([x.2,y.2]).filterMap id).min?)
def PartialExtrema.Goal : Prop := ∀ (blocks : List (List Row)) (k : Key) (c : Nat),
  let parts := blocks.map (fun b => b.filter (·.key == k))
  let retained := parts.filter (fun b => !b.isEmpty)
  let allHits := blocks.flatten.filter (·.key == k)
  (retained.isEmpty,retained.foldl (fun a b => mergeExt a (cell b c)) (none,none)) =
    (allHits.isEmpty,cell allHits c)

/-- Use only occupied dictionary codes when enumerating a sorted dimension axis.
Chunk dictionaries can have duplicate labels and unused entries. Decode a NULL
code as the SQL grouping NULL. The order is a single shared model order. -/
structure Chunk where
  dict : Array (Option String)
  codes : List Nat

def labels (b : Chunk) := b.codes.map (fun i => b.dict[i]?.join)
def axes (keys : List (Option String)) := keys.eraseDups.mergeSort (fun a b => compare a b != .gt)
def OccupiedAxis.Goal : Prop := ∀ (blocks : List Chunk),
  axes (blocks.flatMap (fun b => b.codes.eraseDups.map (fun i => b.dict[i]?.join))) =
    axes (blocks.flatMap labels)

/-- Fold a suffix CASE after grouping by the finer dimensions. The sum of the
fine-group counts is the bag multiplicity of the output label, including ELSE on NULL. -/
def outfield (p : Option String) := p.any (·.endsWith " Fielder")
abbrev Dim := Option String × Option String

def GroupBeforeSuffix.Goal : Prop := ∀ (rows : List Dim) league flag,
  let fine := rows.foldl (fun (m : Std.HashMap Dim Nat) k => m.insert k (m.getD k 0 + 1)) {}
  let result := fine.toList.foldl (fun a ((l,p),w) =>
    if l == league && outfield p == flag then a+w else a) 0
  result = rows.countP (fun (l,p) => l == league && outfield p == flag) ∧ fine.size ≤ rows.length

/-- Field presence and code equality are independent across chunk dictionaries.
For an index on (league,position,year), remap each literal to *all* local aliases;
intersect the three code tests before loading the four batting measures. -/
abbrev Codes := Nat × Nat × Nat
def decode (ls ps : Array (Option String)) (ys : Array (Option Int)) (c : Codes) : Key :=
  (ls[c.1]?.join,ps[c.2.1]?.join,ys[c.2.2]?.join)
def hits {α : Type} [BEq α] (d : Array (Option α)) (v : α) :=
  (List.range d.size).filter (fun i => d[i]?.join == some v)
def TripleBitmap.Goal : Prop := ∀ ls ps ys (codes : List Codes) league pos yr,
  let a := hits ls league
  let b := hits ps pos
  let c := hits ys yr
  codes.filter (fun (i,j,k) => a.contains i && b.contains j && c.contains k) =
    codes.filter (fun t => decode ls ps ys t == (some league,some pos,some yr))

/-- Attribute display (Tableau ATTR): emit a single non-NULL value when both
extremes agree; distinguish a mixed group from an entirely NULL group. -/
inductive Attr where
  | absent | single (v : Int) | multiple
  deriving BEq, DecidableEq
def display (e : Ext) : Attr := match e with
  | (some hi,some lo) => if hi == lo then .single hi else .multiple
  | _ => .absent
def AttributeWitness.Goal : Prop := ∀ (xs : List (Option Int)),
  display (extrema xs) = match (xs.filterMap id).eraseDups with
    | [] => .absent | [x] => .single x | _ => .multiple

/-- Translate exact fixed-point coefficients from local block scales to a common
scale only after computing MIN/MAX. Positive multipliers preserve order for signed
values. An all-NULL block contributes no extremum. -/
def LiftedDecimals.Goal : Prop := ∀ (blocks : List (Int × List (Option Int))),
  (∀ b ∈ blocks, 0 < b.1) →
  blocks.foldl (fun state (m,xs) =>
    let (hi,lo) := extrema xs
    mergeExt state (hi.map (·*m),lo.map (·*m))) (none,none) =
    extrema (blocks.flatMap (fun (m,xs) => xs.map (fun v => v.map (·*m))))

/-- GROUP BY axis membership can be answered by a histogram after removing a
slice of the input. Emit a disappearing dimension value only after the last
row was removed. Different league/position combinations can share a label. -/
def AxisRetraction.Goal : Prop := ∀ (keep drop : List Dim) (label : Option String),
  let old := (keep ++ drop).map Prod.snd
  let deleted := drop.map Prod.snd
  ((axes old).contains label && (old.count label == deleted.count label)) =
    ((axes old).contains label && !(axes (keep.map Prod.snd)).contains label)

/-- Precompute a single classification for each referenced position code and count
the calls. Distinct codes that decode to the same spelling or NULL remain safe. -/
def classify (c : Chunk) :=
  c.codes.foldl (fun (cache,out,calls) code =>
    match cache[code]? with
    | some b => (cache,b::out,calls)
    | none =>
      let b := outfield c.dict[code]?.join
      (cache.insert code b,b::out,calls+1))
    (({} : Std.HashMap Nat Bool),([] : List Bool),0)
def ClassificationCache.Goal : Prop := ∀ c,
  let (_,out,n) := classify c
  out.reverse = (labels c).map outfield ∧ n = c.codes.eraseDups.length

/-- For a three-dimensional slice, stratify by year and physical block. Tighten
the bound on matching league/position pairs using the two marginal counts among
only that year's rows. No independence assumption on these skewed dimensions. -/
def SliceBounds.Goal : Prop := ∀ (blocks : List (List Row)) (l p : String) (y : Int),
  let marg := fun (b : List Row) =>
    let ys := b.filter (fun r => r.key.2.2 == some y)
    let a := ys.countP (fun r => r.key.1 == some l)
    let c := ys.countP (fun r => r.key.2.1 == some p)
    (a+c-ys.length,min a c)
  let n := blocks.flatten.countP (fun r => r.key == (some l,some p,some y))
  ((blocks.map marg).map Prod.fst).sum ≤ n ∧ n ≤ ((blocks.map marg).map Prod.snd).sum

/-- A projected measure can bypass decoding of a block whose selected dictionary
codes all reference one non-NULL value. Unselected codes, including more extreme
values, must not affect the result. All NULL gives an empty extremum. -/
def CodeExtrema.Goal : Prop := ∀ (dict : Array (Option Int)) (selected : List Nat),
  let occupied := selected.eraseDups.filterMap (fun i => dict[i]?.join)
  let summary := match occupied with
    | [] => (none,none)
    | x::rest => if rest.all (· == x) then (some x,some x)
                 else (occupied.max?,occupied.min?)
  summary = extrema (selected.map (fun i => dict[i]?.join))

#guard extrema [none,some (-3),none,some 7] == (some 7,some (-3))
#guard display (extrema [none,some 0,some 0]) == .single 0
#guard hits #[some "PCL",none,some "PCL"] "PCL" == [0,2]
#guard (classify ⟨#[some "Center Fielder", none],[0,1,0,0]⟩).2 == ([true,true,false,true],2)

end TinylambOpt.ResearchGoals.CloudBI.PublicBI
