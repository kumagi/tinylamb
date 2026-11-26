import Std

/-!
Concurrency control must account for the modeled wait dependencies of SQL transactions.
-/

namespace TinylambOpt.ResearchGoals.EngineBoundaries.WaitCycles

/-! One active outgoing write-intent wait per transaction. Edges point from
waiter to holder. Transaction numbers order ages for timestamp policies; a
separate ticket identifies a particular wait across cleanup and re-entry. -/
structure Edge where
  waiter : Nat
  holder : Nat
  ticket : Nat
  deriving BEq, DecidableEq
abbrev Graph := List Edge

def edge (g : Graph) (w : Nat) := g.find? (fun e => e.waiter == w)
def chain (g : Graph) (xs : List Nat) :=
  (xs.zip (xs.drop 1 ++ xs.take 1)).all (fun (a,b) => (edge g a).any (fun e => e.holder == b))
def cycle (g : Graph) (xs : List Nat) := xs ≠ [] ∧ xs.Nodup ∧ chain g xs = true
def retire (g : Graph) (v : Nat) := g.filter (fun e => e.waiter != v && e.holder != v)
def install (g : Graph) (e : Edge) := e :: g.filter (fun f => f.waiter != e.waiter)
def clearTicket (g : Graph) (w t : Nat) := g.filter (fun e => !(e.waiter == w && e.ticket == t))
def walk (g : Graph) : Nat → Nat → Option Nat
  | 0, w => some w
  | n+1, w => (edge g w).bind (fun e => walk g n e.holder)

/-- Timestamp-ordered wait-die edges cannot form a closed wait chain. -/
def WaitDie.Goal : Prop := ∀ g xs, (∀ e ∈ g, e.waiter < e.holder) → ¬ cycle g xs

/-- The reverse strict ordering used for allowed wound-wait edges is acyclic too. -/
def WoundWait.Goal : Prop := ∀ g xs, (∀ e ∈ g, e.holder < e.waiter) → ¬ cycle g xs

/-- An independent cycle survives retiring a victim chosen in a different component. -/
def RetireLocal.Goal : Prop := ∀ g xs v, (g.map Edge.waiter).Nodup → cycle g xs →
  (cycle (retire g v) xs ↔ v ∉ xs)

/-- Aggressive: retiring the youngest in one detected cycle clears all graph cycles. -/
def OneVictim.Goal : Prop := ∀ g xs v, cycle g xs → xs.max? = some v →
  ∀ ys, ¬ cycle (retire g v) ys

/-- The victim still waiting on its old holder alone validates a stale detector snapshot. -/
def VictimEdgeOnly.Goal : Prop := ∀ old live xs v, cycle old xs → xs.max? = some v →
  edge live v = edge old v → cycle live xs

/-- Recheck just the outgoing edges of the recorded cycle before using it. -/
def LocalRevalidation.Goal : Prop := ∀ old live xs, cycle old xs →
  (∀ w ∈ xs, (edge old w).map Edge.holder = (edge live w).map Edge.holder) → cycle live xs

/-- A late cleanup cannot cancel a newer wait from the same transaction with a fresh ticket. -/
def StaleCleanup.Goal : Prop := ∀ g e old, old ≠ e.ticket →
  edge (clearTicket (install g e) e.waiter old) e.waiter = some e

/-- A fresh wait must belong to any cycle newly created by installing it. -/
def LocalDetection.Goal : Prop := ∀ g e xs, cycle (install g e) xs → ¬ cycle g xs → e.waiter ∈ xs

/-- In a functional wait graph, rotating the witness preserves its validity. -/
def RotateWitness.Goal : Prop := ∀ g a rest, cycle g (a::rest) ↔ cycle g (rest ++ [a])

/-- A nonempty simple cycle returns to its first waiter in exactly its length steps. -/
def BoundedWalk.Goal : Prop := ∀ g a rest, cycle g (a::rest) →
  walk g (rest.length+1) a = some a

private def twoCycles : Graph := [⟨1,2,1⟩, ⟨2,1,1⟩, ⟨3,4,1⟩, ⟨4,3,1⟩]
#guard chain twoCycles [1,2] && chain (retire twoCycles 2) [3,4]
#guard edge (clearTicket (install [] ⟨1,3,2⟩) 1 1) 1 == some ⟨1,3,2⟩

end TinylambOpt.ResearchGoals.EngineBoundaries.WaitCycles
