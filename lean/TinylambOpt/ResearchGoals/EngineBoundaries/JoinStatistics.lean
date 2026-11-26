import TinylambOpt.ResearchGoals.ExecutionStrategies.SearchBounds

namespace TinylambOpt.ResearchGoals.EngineBoundaries.JoinStatistics

open TinylambOpt.ResearchGoals.SearchBounds (fanout)

/-! Exact top-key counts from a single snapshot, followed by a residual
frequency cap. All keys below have already passed SQL's non-NULL equality
gate. Sampled MCV estimates require certified bounds before pruning. -/
abbrev Keys := List Nat

def tailKeys (hot xs : Keys) := xs.filter (fun x => !hot.contains x)
def matched (hot a b : Keys) := (hot.eraseDups.map (fun x => a.count x * b.count x)).sum
def cap (xs : Keys) := (xs.map xs.count).max?.getD 0
def residualBound (a b : Keys) := min (a.length * cap b) (b.length * cap a)
def upper (hot a b : Keys) :=
  matched hot a b + residualBound (tailKeys hot a) (tailKeys hot b)
def keySupport (a b : Keys) := a.eraseDups.filter (b.contains ·)
def outerSize (a b : Keys) := (a.map (fun k => max 1 (b.count k))).sum

/-- Extracting heavy keys exactly leaves only a join between the two residuals. -/
def SplitHeavy.Goal : Prop := ∀ hot a b,
  fanout a b = matched hot a b + fanout (tailKeys hot a) (tailKeys hot b)

/-- The tighter of two directional residual caps remains safe under arbitrary key correlation. -/
def TailCap.Goal : Prop := ∀ hot a b, fanout a b ≤ upper hot a b

/-- Tracking an additional exact hot key should never loosen this two-sided bound. -/
def PromoteKey.Goal : Prop := ∀ hot a b k, upper (k::hot) a b ≤ upper hot a b

/-- A scalar min/max cap is sharp when every residual left row hits a right key of maximal frequency. -/
def TightCap.Goal : Prop := ∀ a b, (∀ k ∈ a, b.count k = cap b) →
  fanout a b = residualBound a b

/-- Common residual support gives a lower bound even when all residual frequencies are unknown. -/
def CertifiedFloor.Goal : Prop := ∀ hot a b, matched hot a b +
  (keySupport (tailKeys hot a) (tailKeys hot b)).length ≤ fanout a b

/-- A proposed NDV-based estimate used as an upper bound for an inner join. -/
def UniformUpper.Goal : Prop := ∀ a b, (fanout a b) *
  max a.eraseDups.length b.eraseDups.length ≤ a.length * b.length

/-- An equality join's NULL-safe variant adds precisely the NULL-by-NULL rectangle. -/
def NullSafeIncrement.Goal : Prop := ∀ (a b : List (Option Nat)),
  (a.map b.count).sum = fanout (a.filterMap id) (b.filterMap id) + a.count none * b.count none

/-- For a LEFT JOIN, add unmatched left rows once, including duplicate preserved keys. -/
def OuterCorrection.Goal : Prop := ∀ a b,
  outerSize a b = fanout a b + (a.filter (fun k => !b.contains k)).length

/-- Semijoin pruning can drop residuals when the *certified* upper bound is zero. -/
def EmptyCertificate.Goal : Prop := ∀ hot a b, upper hot a b = 0 →
  (a.filter (b.contains ·)) = []

/-- Per-shard heavy-key estimates can be summed if the build-side histogram is shared. -/
def ShardBudget.Goal : Prop := ∀ a c b hot,
  fanout (a++c) b ≤ upper hot a b + upper hot c b

private def skew : Keys := [0,0,1]
#guard fanout skew skew == 5
#guard fanout skew skew * skew.eraseDups.length > skew.length * skew.length
#guard upper [0] skew skew == 5

end TinylambOpt.ResearchGoals.EngineBoundaries.JoinStatistics
