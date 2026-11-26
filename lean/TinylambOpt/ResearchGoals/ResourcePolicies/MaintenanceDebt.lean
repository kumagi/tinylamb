import Std

/-! Maintenance work arriving before each service opportunity. Work units can be
WAL bytes awaiting flush, obsolete extent bytes awaiting reclamation, or compaction
bytes. Service credits expire if unused. The second model tracks coalesced requests. -/
namespace TinylambOpt.ResearchGoals.ResourcePolicies.MaintenanceDebt

structure Tick where
  added : Nat
  capacity : Nat
  deriving Repr, DecidableEq

def step (owed : Nat) (t : Tick) := owed + t.added - t.capacity
def endDebt (owed : Nat) (ts : List Tick) := ts.foldl step owed
def high (owed : Nat) (ts : List Tick) := ((ts.scanl step owed).max?).getD owed
def excess (ts : List Tick) := (ts.map Tick.added).sum - (ts.map Tick.capacity).sum
def suffixExcess (ts : List Tick) :=
  (((List.range (ts.length+1)).map (fun k => excess (ts.drop k))).max?).getD 0

/-- The last busy period, computable from suffix excesses, determines the debt once the queue started empty. -/
def BusyPeriodDebtEnvelope.Goal : Prop := ∀ ts, endDebt 0 ts = suffixExcess ts

/-- Per-window excess bounds guarantee a high-water cap for every prefix, despite unused early credits. -/
def SlidingWindowBacklogCap.Goal : Prop := ∀ ts cap,
  (∀ i n, excess ((ts.drop i).take n) ≤ cap) → high 0 ts ≤ cap

/-- A sustainable average write rate would suffice for a finite peak equal to one service quantum. -/
def AverageThroughputCapsBacklog.Goal : Prop := ∀ ts quantum, (∀ t ∈ ts, t.capacity = quantum) →
  (ts.map Tick.added).sum ≤ ts.length*quantum → high 0 ts ≤ quantum

/-- Test a repeated trace only twice to bound all later cycles if its total service covers its arrivals. -/
def PeriodicSteadyHighWater.Goal : Prop := ∀ ts cycles, (ts.map Tick.added).sum ≤ (ts.map Tick.capacity).sum →
  high 0 ((List.replicate cycles ts).flatten) ≤ high 0 (ts++ts)

/-- Debt from a stopped writer cannot amplify under replaying the same future load. -/
def InitialBacklogSensitivity.Goal : Prop := ∀ ts a b, a ≤ b →
  endDebt b ts - endDebt a ts ≤ b-a

/-- Combining two service ticks is lossless if the first one does not idle. -/
def NonIdleServiceBatch.Goal : Prop := ∀ (a b : Tick) start,
  a.capacity ≤ start+a.added → endDebt start [a,b] = step start ⟨a.added+b.added,a.capacity+b.capacity⟩

/-- Simultaneous flush and compaction activity can be provisioned using the larger of their independent peaks. -/
def SharedMaintenancePeak.Goal : Prop := ∀ (a b : List Tick), a.length = b.length →
  let combined := a.zipWith (fun x y => Tick.mk (x.added+y.added) (x.capacity+y.capacity)) b
  high 0 combined ≤ max (high 0 a) (high 0 b)

/-- Serving the oldest pending object once per id can absorb a burst of repeat invalidations. -/
def queue (old requests : List Nat) := (old++requests).eraseDups

def CoalescedRequestOrder.Goal : Prop := ∀ old a b, queue (queue old a) b = queue old (a++b)

/-- Drain a prefix of already coalesced work. A key remains pending exactly when it has
an unserviced old entry or a fresh invalidation after the cut. -/
def DrainCutKeepsNewInvalidation.Goal : Prop := ∀ (old fresh : List Nat) k n, old.Nodup →
  (k ∈ queue (old.drop n) fresh ↔ (k ∈ old ∧ ¬ k ∈ old.take n) ∨ k ∈ fresh)

/-- Dropping a finished id from a combined queue is safe even when the same id was invalidated after the cut. -/
def RemoveAfterCoalescing.Goal : Prop := ∀ old fresh k, k ∈ old →
  (queue old fresh).erase k = queue (old.erase k) fresh

/-- Keep the dirty object with its full generation rather than only its slot during a bounded drain. -/
def ExactGenerationDrain.Goal : Prop := ∀ (pending : List (Nat × Nat)) slot oldGen newGen, oldGen ≠ newGen →
  ((pending.filter (fun t => t != (slot,oldGen))).count (slot,newGen)) = pending.count (slot,newGen)

#guard endDebt 0 [⟨0,8⟩,⟨8,0⟩] == 8

#guard high 0 [⟨12,4⟩,⟨0,4⟩,⟨0,4⟩] == 8

end TinylambOpt.ResearchGoals.ResourcePolicies.MaintenanceDebt
