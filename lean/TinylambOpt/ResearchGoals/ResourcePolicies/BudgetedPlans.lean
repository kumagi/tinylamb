import Std

/-! A memo candidate produces the same logical rows as its competitors. Its
profile records work, startup, maximum memory, and memory still held on return.
A blocking parent keeps the left result live throughout the right child. -/
namespace TinylambOpt.ResearchGoals.ResourcePolicies.BudgetedPlans

structure Profile where
  work : Nat
  startup : Nat
  peak : Nat
  held : Nat
  deriving DecidableEq, Repr

def dominates (a b : Profile) : Prop :=
  a.work ≤ b.work ∧ a.startup ≤ b.startup ∧ a.peak ≤ b.peak ∧ a.held ≤ b.held
def join (a b : Profile) (scratch : Nat) : Profile :=
  ⟨a.work+b.work, a.work+b.startup,
   max a.peak (max (a.held+b.peak) (a.held+b.held+scratch)), a.held+b.held⟩

/-- Pruning a dominated build child is safe inside a blocking parent when live output is part of its signature. -/
def RetainedOutputMemoDominance.Goal : Prop := ∀ a b probe scratch, dominates a b →
  dominates (join a probe scratch) (join b probe scratch)

/-- Peak and work alone are sufficient to discard a candidate before embedding it as a join build. -/
def IsolatedPeakMemoPruning.Goal : Prop := ∀ a b probe scratch,
  a.peak ≤ b.peak → a.work ≤ b.work → a.held ≤ a.peak → b.held ≤ b.peak →
  (join a probe scratch).peak ≤ (join b probe scratch).peak

/-- Peak memory for the opposite build order is determined by excess peak above retained output. -/
def BuildSideRetentionOrder.Goal : Prop := ∀ a b s, a.held ≤ a.peak → b.held ≤ b.peak →
  b.peak-b.held ≤ a.peak-a.held → (join a b s).peak ≤ (join b a s).peak

def cheapest (plans : List Profile) (cap : Nat) :=
  ((plans.filter (fun p => p.peak ≤ cap)).map Profile.work).min?

/-- Keeping a cover of the feasible cost frontier preserves optimum work at every memory limit. -/
def BudgetFrontierCover.Goal : Prop := ∀ (all kept : List Profile),
  (∀ p ∈ kept, p ∈ all) → (∀ p ∈ all, ∃ q ∈ kept, dominates q p) →
  ∀ cap, cheapest kept cap = cheapest all cap

/-- A plan minimizing one weighted work-plus-memory score remains optimal at all budgets that admit it. -/
def ScalarWinnerBudgetReuse.Goal : Prop := ∀ (ps : List Profile) winner price cap, winner ∈ ps →
  (∀ p ∈ ps, winner.work+price*winner.peak ≤ p.work+price*p.peak) →
  winner.peak ≤ cap → cheapest ps cap = some winner.work

def latency (p : Profile) (rows requested : Nat) :=
  if rows = 0 then p.startup else p.startup + ((p.work-p.startup) * min rows requested)/rows

/-- Two calibrated latency endpoints certify dominance for a LIMIT probe at every intermediate demand. -/
def RowGoalEndpointDominance.Goal : Prop := ∀ a b n k, 0 < n →
  a.startup ≤ a.work → b.startup ≤ b.work → a.startup ≤ b.startup → a.work ≤ b.work →
  latency a n k ≤ latency b n k

structure Mode where
  setup : Nat
  perRow : Nat
  deriving Repr
def bill (m : Mode) (n : Nat) := m.setup+m.perRow*n

/-- If row-count error is bounded, the work penalty of choosing the estimated winner is bounded by slope difference. -/
def CardinalityErrorRegret.Goal : Prop := ∀ a b estimate actual,
  bill a estimate ≤ bill b estimate → bill a actual - bill b actual ≤
    (max a.perRow b.perRow - min a.perRow b.perRow) * (max estimate actual - min estimate actual)

/-- If both endpoints prefer a physical mode, the whole cardinality interval can share one memo entry. -/
def IntervalPlanStability.Goal : Prop := ∀ a b lo hi n, lo ≤ n → n ≤ hi →
  bill a lo ≤ bill b lo → bill a hi ≤ bill b hi → bill a n ≤ bill b n

/-- After materialization, switching kernels repays its conversion cost on the remaining rows, independent of sunk setup. -/
def ReplanRemainingWorkCut.Goal : Prop := ∀ (sunk conversion remaining oldRate newRate : Nat), newRate ≤ oldRate →
  (sunk+conversion+remaining*newRate ≤ sunk+remaining*oldRate ↔ conversion ≤ remaining*(oldRate-newRate))

/-- Local cheapest choices under independent budgets can still exceed a shared grant during overlapping evaluation. -/
def IndependentChildCaps.Goal : Prop := ∀ a b cap, a.held ≤ a.peak → b.held ≤ b.peak →
  a.peak ≤ cap → b.peak ≤ cap → (join a b 0).peak ≤ cap

#guard cheapest [⟨5,2,100,5⟩,⟨20,1,4,2⟩] 9 == some 20

#guard (join ⟨10,1,8,8⟩ ⟨4,2,8,0⟩ 0).peak == 16

end TinylambOpt.ResearchGoals.ResourcePolicies.BudgetedPlans
