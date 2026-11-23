import Std

namespace TinylambOpt.ResearchGoals.BoundedHeapTopNMerge

abbrev Row := Int × Nat

def rowLt (a b : Row) : Bool :=
  a.1 < b.1 || (a.1 == b.1 && a.2 < b.2)

def insertRow (r : Row) : List Row → List Row
  | [] => [r]
  | x :: xs => if rowLt r x then r :: x :: xs else x :: insertRow r xs

def sortRows : List Row → List Row
  | [] => []
  | r :: rs => insertRow r (sortRows rs)

def refTopN (off lim : Nat) (chunks : List (List Row)) : List Row :=
  ((sortRows chunks.flatten).drop off).take lim

def parTopN (off lim : Nat) (chunks : List (List Row)) : List Row :=
  let workerKeep := off + lim
  let localKept := chunks.map fun c => (sortRows c).take workerKeep
  ((sortRows localKept.flatten).drop off).take lim

/-- Two-stage worker-local Top-(offset + limit) merge (`executor/topn.cpp`,
    `executor/detail/relational_sort_limit.cpp`): pruning each worker to
    `offset + limit` rows and taking `(sorted.drop offset).take limit` on the
    merged union equals full global sorting across arbitrary chunk splits. -/
def Goal : Prop :=
  ∀ (off lim : Nat) (chunks : List (List Row)),
    parTopN off lim chunks = refTopN off lim chunks

/-- Keeping only `limit` rows per worker drops valid survivors when `offset > 0`. -/
def badWorkerLimitOnly (off lim : Nat) (chunks : List (List Row)) : List Row :=
  let localKept := chunks.map fun c => (sortRows c).take lim
  ((sortRows localKept.flatten).drop off).take lim

private def cA : List Row := [(1, 0), (2, 0), (10, 0)]
private def cB : List Row := [(5, 0), (6, 0)]

example :
    parTopN 1 2 [cA, cB] = [(2, 0), (5, 0)] ∧
    refTopN 1 2 [cA, cB] = [(2, 0), (5, 0)] := by
  decide

example :
    badWorkerLimitOnly 1 1 [cA, cB] = [(5, 0)] ∧
    refTopN 1 1 [cA, cB] = [(2, 0)] := by
  decide

end TinylambOpt.ResearchGoals.BoundedHeapTopNMerge
