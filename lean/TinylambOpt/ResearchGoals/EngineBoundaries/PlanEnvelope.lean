import Std

/-!
A cached or specialized plan needs the modeled validity conditions before it can serve a
SQL invocation.
-/

namespace TinylambOpt.ResearchGoals.EngineBoundaries.PlanEnvelope

/-! Deterministic startup, per-row, and excess-memory costs in integer cost
units. Each candidate supplies the same required physical properties. Bounds
here are on the cost model, not a statement about elapsed execution time. -/
structure Plan where
  startup : Nat
  perRow : Nat
  workingSet : Nat
  spillRate : Nat
  deriving BEq, DecidableEq

def cost (p : Plan) (rows grant : Nat) :=
  p.startup + p.perRow * rows + (p.workingSet - grant) * p.spillRate

def cheaper (p q : Plan) (n g : Nat) := decide (cost p n g ≤ cost q n g)
def best (plans : List Plan) (n g : Nat) := (plans.map (fun p => cost p n g)).min?
def clamps (lo hi : Nat) (xs : List Nat) := xs.map (fun x => min hi (max lo x))

/-- Both cardinality endpoints certify an entire interval at a fixed grant. -/
def CardinalityEnds.Goal : Prop := ∀ p q lo hi n g, lo ≤ n → n ≤ hi →
  cheaper p q lo g → cheaper p q hi g → cheaper p q n g

/-- Four corners alone certify a rectangle even across spill transitions. -/
def CornersOnly.Goal : Prop := ∀ p q l h a b n g, l ≤ n → n ≤ h → a ≤ g → g ≤ b →
  [l,h].all (fun r => [a,b].all (cheaper p q r)) → cheaper p q n g

/-- Include both working-set breakpoints, clamped to the grant interval. -/
def SpillBreakpoints.Goal : Prop := ∀ p q l h a b n g, l ≤ n → n ≤ h → a ≤ g → g ≤ b →
  [l,h].all (fun r => (clamps a b [a,b,p.workingSet,q.workingSet]).all (cheaper p q r)) →
  cheaper p q n g

/-- Remove a dominated candidate from a memo group without affecting the optimum. -/
def ParetoPrune.Goal : Prop := ∀ p q rest n g, cheaper p q n g →
  best (p::q::rest) n g = best (p::rest) n g

/-- A crossing between a high-startup and a high-per-row plan is one-directional. -/
def SingleCrossing.Goal : Prop := ∀ p q n m g, p.perRow ≤ q.perRow → n ≤ m →
  cheaper p q n g → cheaper p q m g

/-- At a fixed grant, the optimal cost over a finite memo has decreasing marginal cost. -/
def MarginalEnvelope.Goal : Prop := ∀ ps n g a b c, best ps n g = some a →
  best ps (n+1) g = some b → best ps (n+2) g = some c → a+c ≤ 2*b

/-- Aggressive memo reuse: the LIMIT 1 winner remains a winner at every larger row count. -/
def ReuseFirstRow.Goal : Prop := ∀ ps p g n, p ∈ ps → 0 < n →
  best ps 1 g = some (cost p 1 g) → best ps n g = some (cost p n g)

/-- A larger grant cannot raise the best candidate cost for a fixed group. -/
def GrantMonotonic.Goal : Prop := ∀ ps n g h a b, g ≤ h →
  best ps n g = some a → best ps n h = some b → b ≤ a

/-- A lower bound at the smallest row count and largest grant prunes a whole rectangle. -/
def RectangleCutoff.Goal : Prop := ∀ p n lo hi g cutoff, lo ≤ n → g ≤ hi →
  cutoff < cost p lo hi → cutoff < cost p n g

/-- Winner mismatch under cardinality error is bounded by the two plans' slope difference. -/
def ReestimateRegret.Goal : Prop := ∀ p q n m g, cheaper p q n g →
  cost p m g - cost q m g ≤ (max n m - min n m) * (max p.perRow q.perRow - min p.perRow q.perRow)

private def lowStart : Plan := ⟨0, 8, 0, 0⟩
private def lowSlope : Plan := ⟨12, 1, 0, 0⟩
#guard cheaper lowStart lowSlope 1 0 && !(cheaper lowStart lowSlope 8 0)
-- Both grant endpoints favor p; grant 4 favors q.
#guard ([0,8].all (cheaper ⟨0,0,8,1⟩ ⟨2,0,4,2⟩ 1))
#guard !(cheaper ⟨0,0,8,1⟩ ⟨2,0,4,2⟩ 1 4)

end TinylambOpt.ResearchGoals.EngineBoundaries.PlanEnvelope
