import TinylambOpt.ResearchGoals.QueryRewrites.Common

/-! Group state spilling and replay with nullable exact-integer inputs. Partial
records carry an identity and a sum/count pair. Retry identities denote immutable
records, independently of a group's SQL equality key. -/
namespace TinylambOpt.ResearchGoals.ResourcePolicies.AggregateReplay
open TinylambOpt.ResearchGoals.QueryRewrites

abbrev Rows := List (Nat × Option Int)
abbrev State := Int × Nat
def state (vs : List (Option Int)) : State :=
  let nonnull := vs.filterMap id
  (nonnull.sum, nonnull.length)
def combine (x y : State) : State := (x.1+y.1,x.2+y.2)
def answer (s : State) := if s.2 == 0 then none else some s.1

/-- Flushing any ordered set of partitions preserves GROUP BY SUM and its all-NULL result. -/
def SpilledNullableSum.Goal : Prop := ∀ (ps : List Rows) k,
  answer ((ps.map (fun rs => state (forKey rs k))).foldl combine (0,0)) = nsum (forKey ps.flatten k)

/-- A group with no non-NULL inputs still owns a group key and must survive a sparse-state flush. -/
def NonNullOnlySpillGroupPresence.Goal : Prop := ∀ (rs : Rows),
  ((rs.filter (fun r => r.2.isSome)).map Prod.fst).eraseDups = (rs.map Prod.fst).eraseDups

structure Record where
  id : Nat
  key : Nat
  partialState : State
  deriving Repr, DecidableEq

def replay (rs : List Record) (k : Nat) :=
  ((rs.filter (fun r => r.key == k)).map Record.partialState).foldl combine (0,0)
def uniqueRecords (rs : List Record) := rs.eraseDupsBy (fun a b => a.id == b.id)

/-- Deduplicate repeated immutable records across retries before merging into the group hash table. -/
def RetryRecordDeduplication.Goal : Prop := ∀ (rs retry : List Record) k, (rs.map Record.id).Nodup →
  (∀ r ∈ retry, r ∈ rs) → replay (uniqueRecords (rs++retry)) k = replay rs k

/-- Deduplicating by aggregate value instead of record identity is sufficient for retried partials. -/
def ValueDeduplicatedPartials.Goal : Prop := ∀ (rs : List Record) k, (rs.map Record.id).Nodup →
  replay (rs.eraseDupsBy (fun a b => a.key == b.key && a.partialState == b.partialState)) k = replay rs k

/-- Splitting a partial into two records can preserve every group answer when identities are tracked independently. -/
def RepartitionPartialConservation.Goal : Prop := ∀ a b tail k, a.key = b.key →
  replay (a::b::tail) k = replay ({a with partialState := combine a.partialState b.partialState}::tail) k

/-- A zero sum is sufficient to drop a group partial without affecting SQL SUM for any subsequent batch. -/
def ZeroSumPartialElision.Goal : Prop := ∀ r tail k, r.partialState.1 = 0 →
  answer (replay (r::tail) k) = answer (replay tail k)

def distinctCount (vs : List (Option Int)) := (vs.filterMap id).eraseDups.length

/-- Final COUNT DISTINCT can sum local counts only across disjoint non-NULL value domains. -/
def DisjointDistinctSpillDomains.Goal : Prop := ∀ a b, (∀ x : Int, some x ∈ a → ¬ some x ∈ b) →
  distinctCount (a++b) = distinctCount a + distinctCount b

/-- Hash routing by value allows local distinct-state reclamation after a partition has been finalized. -/
def DistinctPartitionFinalization.Goal : Prop := ∀ vs (bucketOf : Int → Nat) n,
  (∀ x, some x ∈ vs → bucketOf x < n) →
  ((List.range n).map (fun b => distinctCount (vs.filter (fun v => v.any (fun x => bucketOf x == b))))).sum =
    distinctCount vs

/-- Take local top-k with duplicates before spilling, then run a final ordered merge at the same limit. -/
def BoundedRunCandidates.Goal : Prop := ∀ k (runs : List (List Nat)),
  best k (runs.flatMap (best k)) = best k runs.flatten

/-- Local top-k rows suffice even if DISTINCT is delayed until after all runs have been truncated. -/
def LateDistinctAfterLocalLimit.Goal : Prop := ∀ k (runs : List (List Nat)),
  best k (runs.flatMap (best k)).eraseDups = best k runs.flatten.eraseDups

#guard answer (state [none,some 4,some (-4)]) == some 0

end TinylambOpt.ResearchGoals.ResourcePolicies.AggregateReplay
