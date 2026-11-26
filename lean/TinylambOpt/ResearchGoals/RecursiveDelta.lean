import Std

/-!
These claims compare recursive evaluation with a delta-based iteration. A recursive SQL
query can process new frontier rows while preserving the modeled fixpoint.

Modeled claims

- `Goal` — `∀ edges seeds, let s := execute edges seeds (∀ v, v ∈ s.seen ↔ Reach edges
  seeds v) ∧ s.seen.Nodup ∧ s.frontier = [] ∧ s.expanded.Perm s.seen`
-/

namespace TinylambOpt.ResearchGoals.RecursiveDelta

abbrev Edge := Nat × Nat

-- UNION DISTINCT reachability, including each seed by a zero-length path.
inductive Reach (edges : List Edge) (seeds : List Nat) : Nat → Prop where
  | seed {v} : v ∈ seeds → Reach edges seeds v
  | step {u v} : Reach edges seeds u → (u,v) ∈ edges → Reach edges seeds v

def index (edges : List Edge) : Std.HashMap Nat (List Nat) :=
  edges.foldl (fun m (u,v) => m.insert u (v :: m.getD u [])) {}

structure State where
  seen : List Nat
  frontier : List Nat
  expanded : List Nat := []

def round (adj : Std.HashMap Nat (List Nat)) (s : State) : State :=
  let next := ((s.frontier.flatMap (adj.getD · [])).filter (fun v => !s.seen.contains v)).eraseDups
  ⟨s.seen ++ next, next, s.expanded ++ s.frontier⟩

def walk (adj : Std.HashMap Nat (List Nat)) : Nat → State → State
  | 0, s => s
  | n+1, s => if s.frontier.isEmpty then s else walk adj n (round adj s)

def execute (edges : List Edge) (seeds : List Nat) :=
  let roots := seeds.eraseDups
  walk (index edges) (edges.length+1) ⟨roots, roots, []⟩

def Goal : Prop := ∀ edges seeds,
  let s := execute edges seeds
  (∀ v, v ∈ s.seen ↔ Reach edges seeds v) ∧ s.seen.Nodup ∧
  s.frontier = [] ∧ s.expanded.Perm s.seen

private def sample := execute [(0,1),(0,1),(1,0),(1,2),(2,2)] [0,0,9]
example : sample.seen.mergeSort = [0,1,2,9] := by cbv
example : sample.expanded.mergeSort = [0,1,2,9] := by cbv

end TinylambOpt.ResearchGoals.RecursiveDelta
