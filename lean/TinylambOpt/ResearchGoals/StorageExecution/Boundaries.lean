import Std

namespace TinylambOpt.ResearchGoals.StorageExecution

/-- Minimum segmentation cost. Edge i j prices the nonempty interval [i,j).
At iteration d the array stores suffix costs for n, n-1, …, n-d. -/
def boundaryCost (n : Nat) (edge : Nat → Nat → Nat) : Nat := Id.run do
  let mut suffix := #[0]
  for done in List.range n do
    let start := n - done - 1
    let options := (List.range (done+1)).map fun d =>
      edge start (start+d+1) + suffix[done-d]?.getD 0
    suffix := suffix.push (options.min?.getD 0)
  return suffix.back?.getD 0

#guard boundaryCost 4 (fun a b => 4+(b-a)^2) == 16

end TinylambOpt.ResearchGoals.StorageExecution
