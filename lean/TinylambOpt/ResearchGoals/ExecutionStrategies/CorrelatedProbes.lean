import Std

namespace TinylambOpt.ResearchGoals.CorrelatedProbes

/-! Specialized per-correlation-key kernels. Count multiplicities as bags, identify
rows by occurrence where needed, and leave the outer stream in its original order. -/
abbrev Cell := Option Int

/-- Relational division: all required tags occur. Repeated requirements and duplicate
supplied tags contribute once; SQL equality cannot witness a required NULL. -/
def Division.Goal : Prop := ∀ (need supplied : List Cell),
  (need.all fun n => supplied.any fun h => h.isSome && h == n) =
    (!need.contains none &&
      (((supplied.filterMap id).eraseDups.filter (fun h => need.contains (some h))).length ==
       (need.filterMap id).eraseDups.length))

/-- For a scalar subquery with OFFSET d and no LIMIT, store at most d+2 rows per key.
The d+2nd row signals a cardinality error even when it contains the same value. -/
inductive Scalar where
  | value (v : Cell)
  | many
  deriving BEq, DecidableEq

def scalar : List Cell → Scalar
  | [] => .value none
  | [x] => .value x
  | _ => .many

def capped (rows : List (Nat × Cell)) (bound : Nat) :=
  rows.foldl (fun (m : Std.HashMap Nat (Array Cell)) (k,v) =>
    let a := m.getD k #[]
    if a.size < bound then m.insert k (a.push v) else m) {}

def OffsetScalar.Goal : Prop := ∀ (rows : List (Nat × Cell)) d k,
  scalar (((capped rows (d+2)).getD k #[]).toList.drop d) =
    scalar (((rows.filter (fun r => r.1 == k)).map Prod.snd).drop d)

/-- Keep the two smallest values *by occurrence*. Removing the current row can
invalidate only the first occurrence; duplicates of that minimum must survive. -/
def excluding (xs : List Cell) (i : Nat) := (xs.take i ++ xs.drop (i+1)).filterMap id

def firstTwo (xs : List Cell) :=
  xs.zipIdx.foldl (fun best (cell,i) => match cell with
    | none => best
    | some v =>
      let entry := (v,i)
      match best with
      | [] => [entry]
      | a::rest => if v < a.1 then [entry,a] else
        match rest with
        | [] => [a,entry]
        | b::_ => if v < b.1 then [a,entry] else best) []

def ExcludingMinimum.Goal : Prop := ∀ xs i,
  ((firstTwo xs).find? fun e => e.2 != i).map Prod.fst = (excluding xs i).min?

/-- The cardinality guard of a correlated COUNT(DISTINCT v) >= k permits stopping
as soon as k distinct non-NULL values have been witnessed. -/
def enough (k : Nat) (seen : Std.HashSet Int) : List Cell → Bool
  | [] => seen.size ≥ k
  | v::vs => if seen.size ≥ k then true else
      enough k (v.elim seen (fun x => seen.insert x)) vs

def CountDistinctStop.Goal : Prop := ∀ (xs : List Cell) k,
  enough k {} xs = decide (k ≤ (xs.filterMap id).eraseDups.length)

/-- For "<> ANY" and "= ALL", two different non-NULL witnesses decide every
non-NULL probe. A constant-sized summary also covers all-NULL and empty right bags. -/
def anyNe (x : Cell) (ys : List Cell) : Option Bool :=
  if ys.any (fun y => x.isSome && y.isSome && x != y) then some true
  else if !ys.isEmpty && (x.isNone || ys.contains none) then none else some false

def witnesses (ys : List Cell) : List Cell :=
  let (nonNull,hasNull) := ys.foldl (fun (vs,seenNull) v => match v with
    | none => (vs,true)
    | some n => if vs.length < 2 && !vs.contains n then (n::vs,seenNull) else (vs,seenNull))
    (([] : List Int),false)
  nonNull.map some ++ (if hasNull then [none] else [])

def NeWitnesses.Goal : Prop := ∀ (ys : List Cell) x,
  (witnesses ys).length ≤ 3 ∧ anyNe x (witnesses ys) = anyNe x ys

/-- Given candidate build keys with arbitrary overlap, concatenate disjoint probe
branches by *outer row id*. DISTINCT by payload would merge different outer rows. -/
def OrExists.Goal : Prop := ∀ (xs : List (Nat × Nat)) (a b : List Nat),
  (xs.zipIdx.filter fun (r,_) => a.contains r.1 || b.contains r.2) =
    List.merge (xs.zipIdx.filter (fun (r,_) => a.contains r.1))
      (xs.zipIdx.filter (fun (r,_) => !a.contains r.1 && b.contains r.2)) (fun u v => u.2 ≤ v.2)

/-- COUNT(*) over two independent optional children of one parent factorizes even
when only some right-side pairs satisfy an aggregate FILTER. Evaluate both counts
on the padded sides, since the predicate may accept NULL-extension. -/
def pad {α : Type} (xs : List α) : List (Option α) :=
  if xs.isEmpty then [none] else xs.map some

def OptionalSiblings.Goal : Prop := ∀ (a b : List Int) (p q : Cell → Bool),
  (((pad a).flatMap (fun x => (pad b).map fun y => (x,y))).countP
    (fun (x,y) => p x && q y)) = (pad a).countP p * (pad b).countP q

/-- Correlated LIKE-prefix EXISTS: remove any required prefix already covered by
a shorter prefix. The empty prefix covers every non-NULL input, including empty. -/
def minimal (ps : List (List UInt8)) :=
  ps.eraseDups.filter fun p => !(ps.any fun q => q.length < p.length && q.isPrefixOf p)

def PrefixAntichain.Goal : Prop := ∀ (ps : List (List UInt8)) (xs : List (Option (List UInt8))),
  (xs.filter fun x => x.any fun s => ps.any (fun p => p.isPrefixOf s)) =
  (xs.filter fun x => x.any fun s => (minimal ps).any (fun p => p.isPrefixOf s))

#guard !([(none : Cell)].all fun x => [(none : Cell)].any fun y => y.isSome && x == y)
#guard scalar (((capped [(1,some 5),(1,none),(1,none),(1,some 8)] 3).getD 1 #[]).toList.drop 1)
  == .many
#guard firstTwo [some 2,none,some 2,some 4] == [(2,0),(2,2)]
#guard enough 2 {} [some 1,none,some 1] == false
#guard anyNe none [] == some false && anyNe (some 1) [some 1,none] == none
#guard ((List.merge [(2,0),(2,2)] [(3,1)] (fun a b => a.2 ≤ b.2)).map Prod.fst) == [2,3,2]
#guard (pad ([] : List Int)).countP Option.isNone == 1
#guard minimal [[1,2],[1],[]] == [[]]

end TinylambOpt.ResearchGoals.CorrelatedProbes
