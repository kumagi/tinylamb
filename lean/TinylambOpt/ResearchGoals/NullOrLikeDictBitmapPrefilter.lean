import Std

namespace TinylambOpt.ResearchGoals.NullOrLikeDictBitmapPrefilter

inductive Tri where
  | tTrue
  | tFalse
  | tUnknown
  deriving DecidableEq

def triOr (a b : Tri) : Tri :=
  match a, b with
  | .tTrue, _ => .tTrue
  | _, .tTrue => .tTrue
  | .tUnknown, _ => .tUnknown
  | _, .tUnknown => .tUnknown
  | .tFalse, .tFalse => .tFalse

def triAnd (a b : Tri) : Tri :=
  match a, b with
  | .tFalse, _ => .tFalse
  | _, .tFalse => .tFalse
  | .tUnknown, _ => .tUnknown
  | _, .tUnknown => .tUnknown
  | .tTrue, .tTrue => .tTrue

def triNot : Tri → Tri
  | .tTrue => .tFalse
  | .tFalse => .tTrue
  | .tUnknown => .tUnknown

def triAccept : Tri → Bool
  | .tTrue => true
  | .tFalse => false
  | .tUnknown => false

def evalIsNull (v : Option String) : Tri :=
  if v.isNone then .tTrue else .tFalse

def evalMatch (matchStr : String → Bool) (v : Option String) : Tri :=
  match v with
  | none => .tUnknown
  | some s => if matchStr s then .tTrue else .tFalse

def refAccepts (dict : List String) (codes : List (Option Nat))
    (matchStr : String → Bool) (extra : Nat → Tri) : List Bool :=
  codes.zipIdx.map fun (oc, idx) =>
    let v := oc.bind (fun c => dict[c]?)
    let pred := triAnd (triOr (evalIsNull v) (evalMatch matchStr v)) (extra idx)
    triAccept pred

/-- Dictionary-precomputed bitmap prefilter:
    evaluates `matchStr` once per distinct dictionary entry, then evaluates rows
    via `(oc == none || mask[c]) && (extra idx == tTrue)`. -/
def optAccepts (dict : List String) (codes : List (Option Nat))
    (matchStr : String → Bool) (extra : Nat → Tri) : List Bool :=
  let mask := dict.map matchStr
  codes.zipIdx.map fun (oc, idx) =>
    let lhs :=
      match oc with
      | none => true
      | some c => mask.getD c false
    lhs && triAccept (extra idx)

/-- Counterexample: replacing `IS NULL` by `NOT (col = x)` yields `UNKNOWN`,
    not `TRUE`, on NULL values, even inside `OR matchStr(col)`. -/
def buggyNotEqOrAccept (v : Option String) (matchStr : String → Bool) : Bool :=
  let eqTri : Tri := match v with | none => .tUnknown | some s => if s == "x" then .tTrue else .tFalse
  triAccept (triOr (triNot eqTri) (evalMatch matchStr v))

/-- Dictionary-level bitmap prefilter for `(col IS NULL OR LIKE ...)` predicates
    (STATS-CEB C4 / JOB J4 / JOB J5): evaluating `matchStr` once per dictionary
    entry rather than per row and folding `NULL` to `TRUE` matches SQL 3-valued
    logic whenever all non-NULL codes are valid indices `< dict.length`. -/
def Goal : Prop :=
  ∀ (dict : List String) (codes : List (Option Nat))
    (matchStr : String → Bool) (extra : Nat → Tri),
    (∀ c : Nat, some c ∈ codes → c < dict.length) →
    optAccepts dict codes matchStr extra = refAccepts dict codes matchStr extra

example :
    let dict := ["sql", "postgres", "tag-sql-2", "python"]
    let codes : List (Option Nat) := [none, some 0, some 2, some 1, some 3, none]
    let matchStr : String → Bool := fun s => s == "sql" || s == "tag-sql-2"
    let extra : Nat → Tri := fun i => if i == 5 then .tUnknown else if i % 2 == 0 then .tTrue else .tFalse
    optAccepts dict codes matchStr extra = refAccepts dict codes matchStr extra ∧
    optAccepts dict codes matchStr extra = [true, false, true, false, false, false] := by
  decide

example :
    let matchStr : String → Bool := fun s => s == "sql"
    triAccept (triOr (evalIsNull none) (evalMatch matchStr none)) = true ∧
    triAccept (triAnd (evalIsNull none) (evalMatch matchStr none)) = false ∧
    buggyNotEqOrAccept none matchStr = false := by
  decide

end TinylambOpt.ResearchGoals.NullOrLikeDictBitmapPrefilter
