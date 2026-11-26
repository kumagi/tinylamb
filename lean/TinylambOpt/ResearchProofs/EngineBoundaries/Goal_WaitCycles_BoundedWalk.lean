import TinylambOpt.ResearchGoals.EngineBoundaries.WaitCycles

/-!
Concurrency control must account for the modeled wait dependencies of SQL transactions.

Proved

- `BoundedWalk` — A nonempty simple cycle returns to its first waiter in exactly its
  length steps.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.EngineBoundaries.Goal_WaitCycles_BoundedWalk

open TinylambOpt.ResearchGoals.EngineBoundaries.WaitCycles

private theorem walk_chain (g : Graph) : ∀ (xs : List Nat) (last : Nat),
    (xs.zip (xs.drop 1 ++ [last])).all
      (fun p : Nat × Nat => (edge g p.1).any (fun e => e.holder == p.2)) = true →
    xs ≠ [] → walk g xs.length xs.head! = some last := by
  intro xs
  induction xs with
  | nil =>
      intro last h hne
      simp at hne
  | cons x xs ih =>
      intro last h hne
      cases xs with
      | nil =>
          have hEdge : ∃ e, edge g x = some e ∧ e.holder = last := by
            simpa [Option.any_eq_true] using h
          rcases hEdge with ⟨e, he, hel⟩
          change ((edge g x).bind fun e => some e.holder) = some last
          rw [he]
          simp [hel]
      | cons y ys =>
          have hall := List.all_eq_true.mp h
          have hxy : (edge g x).any (fun e => e.holder == y) = true := by
            simpa using hall (x,y) (by simp)
          have htail : ((y :: ys).zip (ys ++ [last])).all
              (fun p : Nat × Nat => (edge g p.1).any (fun e => e.holder == p.2)) = true := by
            apply List.all_eq_true.mpr
            intro p hp
            exact hall p (List.mem_cons_of_mem _ hp)
          have hi := ih last htail (by simp)
          have hEdge : ∃ e, edge g x = some e ∧ e.holder = y := by
            simpa [Option.any_eq_true] using hxy
          rcases hEdge with ⟨e, he, hey⟩
          change ((edge g x).bind (fun e => walk g (ys.length+1) e.holder)) = some last
          rw [he]
          simp only [Option.bind_some]
          rw [hey]
          have hyhead : (y :: ys).head! = y := rfl
          rw [hyhead] at hi
          simpa using hi

theorem sound : BoundedWalk.Goal := by
  intro g a rest hcyc
  rcases hcyc with ⟨hne, hnod, hchain⟩
  have hwalk := walk_chain g (a :: rest) a (by simpa [chain] using hchain) (by simp)
  have hh : (a :: rest).head! = a := rfl
  rw [hh] at hwalk
  simpa using hwalk

end TinylambOpt.ResearchProofs.EngineBoundaries.Goal_WaitCycles_BoundedWalk
