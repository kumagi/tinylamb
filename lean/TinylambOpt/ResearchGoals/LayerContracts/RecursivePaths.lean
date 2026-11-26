import Std

namespace TinylambOpt.ResearchGoals.LayerContracts.RecursivePaths

/-! A recursive query traverses a finite labeled edge bag, using UNION
semantics on the observable terminal vertices. The full work-table key is
(vertex, automaton state). A projected key can suppress a later accepting path. -/
structure Edge where
  src : Nat
  dst : Nat
  label : Nat
  deriving BEq
abbrev Node := Nat × Nat
structure Model where
  states : Nat
  next : Nat → Nat → Option Nat
  accept : Nat → Bool

def advance (m : Model) (q : Nat) (labels : List Nat) := labels.foldlM m.next q
def expand (m : Model) (g : List Edge) (wave : List Node) :=
  wave.flatMap fun (v,q) => g.filterMap fun e =>
    if e.src == v then (m.next q e.label).map (e.dst,·) else none
def slow (m : Model) (g : List Edge) (start : Node) : Nat → List Node
  | 0 => [start]
  | n+1 => let prev := slow m g start n
           (prev ++ expand m g prev).eraseDups

def insertNew (key : Node → Node) (acc : List Node) (x : Node) :=
  if acc.any (fun y => key x == key y) then acc else acc ++ [x]
def walk (m : Model) (g : List Edge) (key : Node → Node) : Nat → List Node → List Node → List Node
  | 0,seen,_ => seen
  | n+1,seen,wave =>
    let allSeen := (expand m g wave).foldl (insertNew key) seen
    if allSeen.length == seen.length then seen else
      walk m g key n allSeen (allSeen.drop seen.length)
def query (m : Model) (g : List Edge) (v n : Nat) (key : Node → Node) :=
  ((walk m g key n [(v,0)] [(v,0)]).filter (fun p => m.accept p.2)).map Prod.fst
def vertices (g : List Edge) (v : Nat) := (v :: g.flatMap (fun e => [e.src,e.dst])).eraseDups

/-- Expand only new full-state work-table rows. -/
def deltaOnly (m : Model) : Prop := ∀ g v n x, x ∈ query m g v n id ↔ ∃ q, (x,q) ∈ slow m g (v,0) n ∧ m.accept q

/-- Can DISTINCT use only the vertex before testing the accepting state? -/
def vertexOnly (m : Model) : Prop := ∀ g v n x, x ∈ query m g v n (fun p => (p.1,0)) ↔ x ∈ query m g v n id

/-- Finite graph times automaton size is enough rounds for all terminal vertices. -/
def productDepth (m : Model) : Prop := ∀ g v n x, let bound := (vertices g v).length * m.states
  x ∈ query m g v (bound+n) id ↔ x ∈ query m g v bound id

/-- Aggressive recursion cap: one round per graph vertex even with path constraints. -/
def plainDepth (m : Model) : Prop := ∀ g v n x, x ∈ query m g v n id → x ∈ query m g v (vertices g v).length id

/-- Bound keys in the visited bitmap by the claimed state count. -/
def stateDomain (m : Model) : Prop := ∀ g v n w q, (w,q) ∈ slow m g (v,0) n → q < m.states

/-- Dropping duplicate edges before starting a recursive UNION preserves reached vertices. -/
def edgeDedup (m : Model) : Prop := ∀ g v n x, x ∈ query m g.eraseDups v n id ↔ x ∈ query m g v n id

/-- Stopping on first physical encounter with a vertex is enough to decide acceptance. -/
def seedAcceptance (m : Model) : Prop := ∀ labels a b, advance m 0 labels = some a → advance m a b ≠ none →
  (advance m a b).any m.accept → m.accept a

/-- Can independently traversed edge shards be unioned without a cross-shard work queue? -/
def shardUnion (m : Model) : Prop := ∀ a b v n x, x ∈ query m (a++b) v n id ↔ x ∈ query m a v n id ∨ x ∈ query m b v n id

namespace EvenHops

/-- Retain only even-length walks. -/
def model : Model :=
  ⟨2, fun q _ => some (1-q), fun q => q==0⟩

def DeltaOnly.Goal : Prop := deltaOnly model
def VertexOnly.Goal : Prop := vertexOnly model
def ProductDepth.Goal : Prop := productDepth model
def PlainDepth.Goal : Prop := plainDepth model
def StateDomain.Goal : Prop := stateDomain model
def EdgeDedup.Goal : Prop := edgeDedup model
def SeedAcceptance.Goal : Prop := seedAcceptance model
def ShardUnion.Goal : Prop := shardUnion model

def Parity.Goal : Prop := ∀ ls, advance model 0 ls = some (ls.length%2)

def TwoSteps.Goal : Prop := ∀ q a b, q<2 → advance model q [a,b] = some q

end EvenHops

namespace AtLeastOneRed

/-- Reach a vertex along a path containing label one. -/
def model : Model :=
  ⟨2, fun q x => some (if x==1 then 1 else q), fun q => q==1⟩

def DeltaOnly.Goal : Prop := deltaOnly model
def VertexOnly.Goal : Prop := vertexOnly model
def ProductDepth.Goal : Prop := productDepth model
def PlainDepth.Goal : Prop := plainDepth model
def StateDomain.Goal : Prop := stateDomain model
def EdgeDedup.Goal : Prop := edgeDedup model
def SeedAcceptance.Goal : Prop := seedAcceptance model
def ShardUnion.Goal : Prop := shardUnion model

def RedWitness.Goal : Prop := ∀ ls, (advance model 0 ls).any model.accept = ls.contains 1

def Absorbing.Goal : Prop := ∀ ls, advance model 1 ls = some 1

end AtLeastOneRed

namespace Alternating

/-- Require alternating red and blue edges starting at zero. -/
def model : Model :=
  ⟨2, fun q x => if q==x then some (1-q) else none, fun _ => true⟩

def DeltaOnly.Goal : Prop := deltaOnly model
def VertexOnly.Goal : Prop := vertexOnly model
def ProductDepth.Goal : Prop := productDepth model
def PlainDepth.Goal : Prop := plainDepth model
def StateDomain.Goal : Prop := stateDomain model
def EdgeDedup.Goal : Prop := edgeDedup model
def SeedAcceptance.Goal : Prop := seedAcceptance model
def ShardUnion.Goal : Prop := shardUnion model

def RepeatedColor.Goal : Prop := ∀ q x, q<2 → x<2 → advance model q [x,x] = none

def Pairs.Goal : Prop := ∀ n, advance model 0 ((List.replicate n [0,1]).flatten) = some 0

end Alternating

namespace SingleRed

/-- Exactly one edge labeled one; reject second occurrence. -/
def model : Model :=
  ⟨2, fun q x => if x != 1 then some q else if q==0 then some 1 else none, fun q => q==1⟩

def DeltaOnly.Goal : Prop := deltaOnly model
def VertexOnly.Goal : Prop := vertexOnly model
def ProductDepth.Goal : Prop := productDepth model
def PlainDepth.Goal : Prop := plainDepth model
def StateDomain.Goal : Prop := stateDomain model
def EdgeDedup.Goal : Prop := edgeDedup model
def SeedAcceptance.Goal : Prop := seedAcceptance model
def ShardUnion.Goal : Prop := shardUnion model

def Count.Goal : Prop := ∀ ls, (advance model 0 ls).any model.accept = (ls.count 1 == 1)

def PruneSecond.Goal : Prop := ∀ ls, 1 < ls.count 1 → advance model 0 ls = none

end SingleRed

namespace TollBudget

/-- Path sum of nonnegative tolls must fit a three-unit budget. -/
def model : Model :=
  ⟨4, fun q x => if q+x ≤ 3 then some (q+x) else none, fun _ => true⟩

def DeltaOnly.Goal : Prop := deltaOnly model
def VertexOnly.Goal : Prop := vertexOnly model
def ProductDepth.Goal : Prop := productDepth model
def PlainDepth.Goal : Prop := plainDepth model
def StateDomain.Goal : Prop := stateDomain model
def EdgeDedup.Goal : Prop := edgeDedup model
def SeedAcceptance.Goal : Prop := seedAcceptance model
def ShardUnion.Goal : Prop := shardUnion model

def Budget.Goal : Prop := ∀ ls, (advance model 0 ls).isSome = decide (ls.sum ≤ 3)

def ExactCost.Goal : Prop := ∀ ls q, advance model 0 ls = some q → q=ls.sum

end TollBudget

namespace OrderedClass

/-- Nondecreasing labels from classes 0, 1 and 2. -/
def model : Model :=
  ⟨3, fun q x => if q ≤ x && x<3 then some x else none, fun _ => true⟩

def DeltaOnly.Goal : Prop := deltaOnly model
def VertexOnly.Goal : Prop := vertexOnly model
def ProductDepth.Goal : Prop := productDepth model
def PlainDepth.Goal : Prop := plainDepth model
def StateDomain.Goal : Prop := stateDomain model
def EdgeDedup.Goal : Prop := edgeDedup model
def SeedAcceptance.Goal : Prop := seedAcceptance model
def ShardUnion.Goal : Prop := shardUnion model

def DescendingPair.Goal : Prop := ∀ a b, b<a → advance model 0 [a,b] = none

def KeepLast.Goal : Prop := ∀ ls x q, advance model 0 (ls++[x]) = some q → q=x

end OrderedClass

namespace LastBlue

/-- Accept only paths ending in an edge labeled two. -/
def model : Model :=
  ⟨2, fun _ x => some (if x==2 then 1 else 0), fun q => q==1⟩

def DeltaOnly.Goal : Prop := deltaOnly model
def VertexOnly.Goal : Prop := vertexOnly model
def ProductDepth.Goal : Prop := productDepth model
def PlainDepth.Goal : Prop := plainDepth model
def StateDomain.Goal : Prop := stateDomain model
def EdgeDedup.Goal : Prop := edgeDedup model
def SeedAcceptance.Goal : Prop := seedAcceptance model
def ShardUnion.Goal : Prop := shardUnion model

def LastTag.Goal : Prop := ∀ ls, (advance model 0 ls).any model.accept = (ls.getLast? == some 2)

def Reset.Goal : Prop := ∀ q ls, ls ≠ [] → advance model q ls = advance model 0 ls

end LastBlue

namespace PatternAB

/-- Reachability with adjacent edge labels 1 then 2 somewhere in the path. -/
def model : Model :=
  ⟨3, fun q x => some (if q==2 || (q==1 && x==2) then 2 else if x==1 then 1 else 0), fun q => q==2⟩

def DeltaOnly.Goal : Prop := deltaOnly model
def VertexOnly.Goal : Prop := vertexOnly model
def ProductDepth.Goal : Prop := productDepth model
def PlainDepth.Goal : Prop := plainDepth model
def StateDomain.Goal : Prop := stateDomain model
def EdgeDedup.Goal : Prop := edgeDedup model
def SeedAcceptance.Goal : Prop := seedAcceptance model
def ShardUnion.Goal : Prop := shardUnion model

def Witness.Goal : Prop := ∀ ls, (advance model 0 ls).any model.accept = ((ls.zip (ls.drop 1)).contains (1,2))

def SuffixStable.Goal : Prop := ∀ ls, advance model 2 ls = some 2

end PatternAB

namespace ModuloCost

/-- Require total edge cost divisible by three. -/
def model : Model :=
  ⟨3, fun q x => some ((q+x)%3), fun q => q==0⟩

def DeltaOnly.Goal : Prop := deltaOnly model
def VertexOnly.Goal : Prop := vertexOnly model
def ProductDepth.Goal : Prop := productDepth model
def PlainDepth.Goal : Prop := plainDepth model
def StateDomain.Goal : Prop := stateDomain model
def EdgeDedup.Goal : Prop := edgeDedup model
def SeedAcceptance.Goal : Prop := seedAcceptance model
def ShardUnion.Goal : Prop := shardUnion model

def Residue.Goal : Prop := ∀ ls, advance model 0 ls = some (ls.sum%3)

def Reorder.Goal : Prop := ∀ (a b : List Nat), a.Perm b → advance model 0 a = advance model 0 b

end ModuloCost

namespace BothTypes

/-- Visit at least one edge of each of two designated classes. -/
def model : Model :=
  ⟨4, fun q x => some (q ||| (if x==1 then 1 else if x==2 then 2 else 0)), fun q => q==3⟩

def DeltaOnly.Goal : Prop := deltaOnly model
def VertexOnly.Goal : Prop := vertexOnly model
def ProductDepth.Goal : Prop := productDepth model
def PlainDepth.Goal : Prop := plainDepth model
def StateDomain.Goal : Prop := stateDomain model
def EdgeDedup.Goal : Prop := edgeDedup model
def SeedAcceptance.Goal : Prop := seedAcceptance model
def ShardUnion.Goal : Prop := shardUnion model

def Coverage.Goal : Prop := ∀ ls, (advance model 0 ls).any model.accept = (ls.contains 1 && ls.contains 2)

def RepeatLabel.Goal : Prop := ∀ q x, advance model q [x,x] = advance model q [x]

end BothTypes

private def loopEdge : List Edge := [⟨0,0,1⟩]
#guard (query AtLeastOneRed.model loopEdge 0 1 id).contains 0
#guard !(query AtLeastOneRed.model loopEdge 0 1 (fun p => (p.1,0))).contains 0

end TinylambOpt.ResearchGoals.LayerContracts.RecursivePaths
