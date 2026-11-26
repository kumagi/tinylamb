import TinylambOpt.ResearchGoals.RestartRaces

/-!
This fixture checks concrete commit, replay, and concurrency schedules. These examples
expose the ordering requirements for SQL transactions across restart.
-/
open TinylambOpt.ResearchGoals.RestartRaces

namespace RestartRaceChecks

-- A strict local winner: one miss against two. The other order would
-- already have cached both negative titles requested by the second morsel.
private def morsels : List (List JobJ3.Edge) := [[(0,0),(0,1)],[(1,0),(2,1)]]
#guard JobJ3.ProbeSchedule.greedy (fun _ => false) (fun _ => false) ⟨[],[]⟩ morsels == 3
#guard JobJ3.ProbeSchedule.cheapest (fun _ => false) (fun _ => false) ⟨[],[]⟩ morsels == 2

private def swapRows : List Catalog.UniqueHandoff.Row := [⟨0,some 1,some 2⟩,⟨1,some 2,some 1⟩]
private def keysOK (done : List Nat) :=
  let keys := swapRows.filterMap fun r => if done.contains r.owner then r.newKey else r.oldKey
  decide keys.Nodup
#guard keysOK [] && keysOK [0,1] && !keysOK [0] && !keysOK [1]

-- Check the rename lower bound against shortest paths computed with only
-- the primitive move, without using its cycle decomposition.
private def levels (n : Nat) :=
  let slots := List.range (n+1)
  let moves := slots.flatMap (fun i => slots.map (i,·))
  let start := (List.range n).map some ++ [none]
  let next := fun (states : List (List (Option Nat))) =>
    (states.flatMap (fun s => moves.filterMap (Catalog.RenameMoves.move s))).eraseDups
  (List.range (2*n)).scanl (fun states _ => next states) [start]

private def permutations : Nat → List Nat → List (List Nat)
  | 0, _ => [[]]
  | n+1, xs => xs.flatMap (fun x => (permutations n (xs.erase x)).map (x::·))

private def verifyMoves (n : Nat) :=
  let reachable := levels n
  (permutations n (List.range n)).all fun p =>
    let finish := p.map some ++ [none]
    reachable.findIdx (·.contains finish) == Catalog.RenameMoves.budget p
#guard (List.range 5).all verifyMoves

private def gens : List Recovery.GenerationGC.Gen := [⟨2,20,[200]⟩,⟨1,10,[100]⟩]
private def cut : Recovery.GenerationGC.Cut := ⟨[100,200],0,30,[1]⟩
#guard (Recovery.GenerationGC.chosen gens cut).map (·.id) == some 1
#guard (Recovery.GenerationGC.chosen gens {cut with manifests := [1,2]}).map (·.id) == some 2
end RestartRaceChecks
