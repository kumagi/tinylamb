import Std

namespace TinylambOpt.ResearchGoals.RestartRaces.Concurrency

namespace BatchOCC
/-! A cohort all read the same snapshot before staging their independent
writes. Each key has at most one writer. A write may be a same-value write.
Observable reads here are key contents, including absence via a tombstone. -/
structure Tx where
  id : Nat
  reads : List Nat
  writes : Nat → Option (Option Int)

def applyTx (s : Nat → Option Int) (tx : Tx) (k : Nat) := (tx.writes k).getD (s k)
def valid (base : Nat → Option Int) (s : Nat → Option Int) : List Tx → Prop
  | [] => True
  | t::ts => t.reads.map s = t.reads.map base ∧ valid base (applyTx s t) ts
def edge (base : Nat → Option Int) (reader writer : Tx) :=
  reader.id ≠ writer.id ∧ ∃ k ∈ reader.reads, applyTx base writer k ≠ base k

/-- Can a commit combiner accept *exactly* the serializable snapshot-read
cohorts using anti-dependencies on value-changing writes? Asks for existence
of a serial order, including read/write skew cycles and same-value updates. -/
def Goal : Prop := ∀ base (cohort : List Tx), (cohort.map Tx.id).Nodup →
  (∀ a ∈ cohort, ∀ b ∈ cohort, a.id ≠ b.id →
    ∀ k, (a.writes k).isSome → b.writes k = none) →
  ((∃ order, order.Perm cohort ∧ valid base base order) ↔
    ∃ rank : Nat → Nat, ∀ a ∈ cohort, ∀ b ∈ cohort, edge base a b → rank a.id < rank b.id)
end BatchOCC

namespace CycleInstant
/-! Detector samples immutable wait tickets on separate shards. With generation
checks, one observation certifies its edge continuously over a closed interval.
Outside that interval an edge may be absent. -/
abbrev Window := Nat × Nat
abbrev covers (w : Window) (tick : Nat) := w.1 ≤ tick ∧ tick ≤ w.2

/-- For a candidate cycle already checked for topology, when do non-atomic
edge observations force the existence of a simultaneous cycle? This is an
information bound over all live histories compatible with the observations. -/
def Goal : Prop := ∀ (ws : List Window), ws ≠ [] →
  ((ws.map Prod.fst).max?.getD 0 ≤ (ws.map Prod.snd).min?.getD 0 ↔
    ∀ live : Nat → Nat → Prop, (∀ i w, ws[i]? = some w → ∀ t, covers w t → live i t) →
      ∃ t, ∀ i ∈ List.range ws.length, live i t)

#guard !decide (covers (1,3) 4) && decide (covers (4,5) 4)
end CycleInstant

namespace PackedWrites
/-! Stage constant bit/field assignments in a packed physical word. A lane
read returns its previous value. Missing writes preserve the base. -/
structure Op where
  readSet : List Nat
  put : Nat → Option Bool
def run (o : Op) (s : Nat → Bool) (i : Nat) := (o.put i).getD (s i)
def safe (a b : Op) :=
  (∀ k ∈ a.readSet, b.put k = none) ∧ (∀ k ∈ b.readSet, a.put k = none) ∧
  (∀ k x y, a.put k = some x → b.put k = some y → x = y)

/-- Replace a whole-row write conflict by an exact field certificate when
the storage update is atomic. Both orders must preserve both isolated read
observations for every base and produce the same final packed value. -/
def Goal : Prop := ∀ a b, safe a b ↔ ∀ s,
  a.readSet.map (run b s) = a.readSet.map s ∧ b.readSet.map (run a s) = b.readSet.map s ∧
  ∀ k, run a (run b s) k = run b (run a s) k
end PackedWrites

namespace RangePromotion
/-! A bounded predicate-lock table must cover requested keys by intervals.
An external transaction blocked on several promoted intervals waits once.
The surrogate charges it once per interval. Alternative covers have been
prevalidated for coverage, disjoint intervals and the same slot budget. -/
abbrev Cover := List (Nat × Nat)
def contact (keys : List Nat) (r : Nat × Nat) := keys.any (fun k => r.1 ≤ k && k ≤ r.2)
def hits (c : Cover) (keys : List Nat) := c.countP (contact keys)
def exact (ws : List (List Nat)) (c : Cover) := ws.countP (fun w => hits c w != 0)
def additive (ws : List (List Nat)) (c : Cover) := (ws.map (hits c)).sum

/-- Price range-lock promotion using local collision counts. If one writer
touches at most d intervals per candidate, does minimizing that surrogate
guarantee at most d times the minimum number of distinct blocked writers? -/
def Goal : Prop := ∀ ws (covers : List Cover) chosen d, chosen ∈ covers →
  (∀ c ∈ covers, ∀ w ∈ ws, hits c w ≤ d) →
  (∀ c ∈ covers, additive ws chosen ≤ additive ws c) →
  ∀ c ∈ covers, exact ws chosen ≤ d * exact ws c

#guard exact [[1,9],[3]] [(0,2),(8,9)] == 1 && additive [[1,9],[3]] [(0,2),(8,9)] == 2
end RangePromotion

end TinylambOpt.ResearchGoals.RestartRaces.Concurrency
