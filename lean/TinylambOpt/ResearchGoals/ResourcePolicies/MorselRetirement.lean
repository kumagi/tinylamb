import Std

/-! Parallel morsels over an immutable input snapshot. Workers can finish out of
order. A coordinator retires only a contiguous prefix; retry and cancellation
must distinguish an empty completed morsel from a missing result. -/
namespace TinylambOpt.ResearchGoals.ResourcePolicies.MorselRetirement

abbrev Report := Nat × List Nat
def lookup (reports : List Report) (i : Nat) := (reports.find? (fun r => r.1 == i)).map Prod.snd
def readyIds (reports : List Report) (start count : Nat) :=
  ((List.range count).map (start+·)).takeWhile (fun i => (lookup reports i).isSome)
def emit (reports : List Report) (start count : Nat) :=
  (readyIds reports start count).flatMap (fun i => (lookup reports i).getD [])
def consistent (rs : List Report) := ∀ a ∈ rs, ∀ b ∈ rs, a.1 = b.1 → a.2 = b.2

/-- Reordering completion messages cannot change the ordered output if repeated ids report the same snapshot rows. -/
def DuplicateConsistentRetirement.Goal : Prop := ∀ a b start count, consistent a → a.Perm b →
  emit a start count = emit b start count

/-- A completed empty filter result advances retirement without fabricating rows. -/
def EmptyMorselRetirement.Goal : Prop := ∀ rs start n, lookup rs start = some [] →
  emit rs start (n+1) = emit rs (start+1) n

/-- Publishing a partial prefix now and resuming at the retirement frontier composes as later results arrive. -/
def FrontierResumeOrderedOutput.Goal : Prop := ∀ old new start count, consistent (old++new) →
  let consumed := (readyIds old start count).length
  emit old start count ++ emit (old++new) (start+consumed) (count-consumed) = emit (old++new) start count

/-- Discarding reports below the retired frontier releases memory without changing future output. -/
def RetiredReportReclamation.Goal : Prop := ∀ rs start count,
  emit (rs.filter (fun r => start ≤ r.1)) start count = emit rs start count

/-- LIMIT can cancel later morsels as soon as a contiguous completed prefix already contains enough output rows. -/
def LimitCancellationFrontier.Goal : Prop := ∀ rs start count k future, consistent (rs++future) →
  k ≤ (emit rs start count).length →
  (emit (rs++future) start count).take k = (emit rs start count).take k

/-- A row count from arbitrary finished morsels is enough to cancel the remaining work in an ordered LIMIT. -/
def OutOfOrderLimitCredit.Goal : Prop := ∀ rs start count k future, consistent (rs++future) →
  (∀ r ∈ rs, start ≤ r.1 ∧ r.1 < start+count) →
  k ≤ (rs.flatMap Prod.snd).length →
  (emit (rs++future) start count).take k = (emit rs start count).take k

/-- A dispatch window bounds retained rows when each unretired morsel reports once and has a known output cap. -/
def DispatchWindowOutputCap.Goal : Prop := ∀ (rs : List Report) start window rows,
  (rs.map Prod.fst).Nodup → (∀ r ∈ rs, start ≤ r.1 ∧ r.1 < start+window ∧ r.2.length ≤ rows) →
  (rs.flatMap Prod.snd).length ≤ window*rows

/-- Limiting worker count also caps the reorder queue if workers may immediately claim a new morsel behind a stalled head. -/
def WorkerCountBoundsReorderQueue.Goal : Prop := ∀ (schedule : List (Nat × Nat)) workers,
  (∀ e ∈ schedule, e.1 < workers ∧ 0 < e.2) → (schedule.map Prod.snd).Nodup →
  schedule.length ≤ workers

/-- Two adjacent output windows can be drained independently once the entire first window has reported. -/
def CompletePrefixSplit.Goal : Prop := ∀ rs start a b, (readyIds rs start a).length = a →
  emit rs start (a+b) = emit rs start a ++ emit rs (start+a) b

#guard readyIds [(2,[7]),(0,[])] 0 3 == [0]

end TinylambOpt.ResearchGoals.ResourcePolicies.MorselRetirement
