import TinylambOpt.ResearchGoals.AnalyticsWorkloads.Common

namespace TinylambOpt.ResearchGoals.AnalyticsWorkloads.Job

/-- J1. Cache the product of filtered company-name and company-type frequencies
for each company/type pair occurring in the bridge. A bit per dimension key would
lose multiplicity on a non-unique build. The last component is the qualified title year. -/
abbrev Bridge := Nat × Nat × Int

def pairCache (rows : List Bridge) (companies types : List Nat) :=
  let c := histogram companies
  let t := histogram types
  let keys := (rows.map (fun (a,b,_) => (a,b))).eraseDups
  keys.foldl (fun (m : Std.HashMap (Nat × Nat) Nat) (a,b) =>
    m.insert (a,b) (c.getD a 0 * t.getD b 0)) {}

def CompanyPair.Goal : Prop := ∀ rows companies types,
  let cache := pairCache rows companies types
  rows.foldl (fun acc (a,b,y) => plus acc (weighted (cache.getD (a,b) 0) (some y))) zero =
    summarize (rows.flatMap fun (a,b,y) => (companies.filter (· == a)).flatMap fun _ =>
      (types.filter (· == b)).map fun _ => some y)

/-- J2. Fixed-width ASCII d.d ratings admit a two-digit comparison. All other strings
use the original binary comparison; '10.0' must not pass '7.0' via a numeric cast. -/
def rating (s : String) : Option Nat :=
  match s.toList with
  | [a,'.',b] => if a.isDigit && b.isDigit then some ((a.toNat-48)*10+b.toNat-48) else none
  | _ => none

def atLeast (bound v : String) :=
  match rating bound, rating v with
  | some a,some b => decide (a ≤ b)
  | _,_ => decide (bound ≤ v)

def RatingOrder.Goal : Prop := ∀ (ratings : List (Option String)) bound,
  ratings.filter (fun v => v.any (atLeast bound)) =
    ratings.filter (fun v => v.any (fun s => decide (bound ≤ s)))

/-- J3. Discharge GROUP BY gender after proving the only passing label is F.
Group presence is distinct from a NULL AVG. Multiplicities from the joins are retained. -/
def singletonGroup (ys : List (Option Int)) : List (String × Totals) :=
  let st := summarize ys
  if st.cnt == 0 then [] else [("F",st)]

def FixedGender.Goal : Prop := ∀ (rows : List (String × Option Int)),
  let kept := rows.filter fun r => r.1 == "F"
  singletonGroup (kept.map Prod.snd) =
    ((kept.map Prod.fst).eraseDups.map fun g =>
      (g,summarize ((kept.filter (fun r => r.1 == g)).map Prod.snd)))

/-- J4. Encode the projected keyword label, not its id. First coalesce repeated
(movie,label) associations, then probe the selected movie-info weight once per pair. -/
def labelPairs (bridge : List (Nat × Nat)) (dict : List (Nat × String)) :=
  bridge.flatMap fun (movie,k) => (dict.filter (fun e => e.1 == k)).map fun e => (movie,e.2)

def labelCount (pairs : List (Nat × String)) (info : List Nat) (label : String) :=
  let weights := histogram info
  ((histogram pairs).toList.map fun ((movie,name),n) =>
    if name == label then n * weights.getD movie 0 else 0).sum

def LabelQuotient.Goal : Prop := ∀ bridge dict info label,
  labelCount (labelPairs bridge dict) info label =
    (bridge.flatMap fun (movie,k) => (info.filter (· == movie)).flatMap fun _ =>
      (dict.filter (fun e => e.1 == k)).map Prod.snd).count label

/-- J5. A dictionary entry certified to equal 'aka-' followed by canonical decimal
digits can use digit-length ranges; arbitrary spellings keep literal prefix semantics. -/
abbrev DecimalCode := Nat × Nat -- suffix value, power of ten at the leading position

def certified (s : String) : Option DecimalCode → Prop
  | none => True
  | some (n,w) => s = "aka-" ++ toString n ∧ w = 10 ^ ((toString n).length-1)

def aka (s : String) : Option DecimalCode → Bool
  | some (n,w) => w ≤ n && n < 2*w
  | none => s.startsWith "aka-1"

def DecimalPrefix.Goal : Prop := ∀ s code, certified s code → aka s code = s.startsWith "aka-1"

/-- J1/J2. Bounds for a pair of correlated filters inside a synopsis cell, weighted
by the *same* nonnegative per-title fanout. These apply even under perfect correlation. -/
abbrev Mass := Bool × Bool × Nat
def mass (rows : List Mass) (p : Bool → Bool → Bool) :=
  (rows.map fun (a,b,w) => if p a b then w else 0).sum

def CorrelatedFanout.Goal : Prop := ∀ (cells : List (List Mass)),
  let a := fun rs => mass rs (fun x _ => x)
  let b := fun rs => mass rs (fun _ y => y)
  let both := mass cells.flatten (· && ·)
  (cells.map fun rs => a rs + b rs - mass rs (fun _ _ => true)).sum ≤ both ∧
    both ≤ (cells.map fun rs => min (a rs) (b rs)).sum

#guard (pairCache [(1,2,1999),(1,2,2000)] [1,1] [2,2,2]).getD (1,2) 0 == 6
#guard !atLeast "7.0" "10.0" && atLeast "7.0" "7.0" && !atLeast "7.0" "6.9"
#guard (singletonGroup []).isEmpty && (singletonGroup [none]).map (fun x => x.2.cnt) == [1]
#guard labelCount (labelPairs [(1,7),(1,8)] [(7,"a"),(7,"a"),(8,"a")]) [1,1] "a" == 6
#guard aka "aka-10" (some (10,10)) && !aka "aka-21" (some (21,10)) &&
  !aka "aka-01" none && aka "aka-1x" none
#guard mass [(true,false,100),(true,true,2),(false,true,7)] (· && ·) == 2

end TinylambOpt.ResearchGoals.AnalyticsWorkloads.Job
