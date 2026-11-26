import Std

/-! Selection ordering under a row-dependent evaluation cost. Boolean selectors
represent WHERE acceptance for pure predicates. A separate outcome model tracks
the first observable error, which conjunction of acceptance masks cannot express. -/
namespace TinylambOpt.ResearchGoals.ResourcePolicies.PredicateScheduling

structure Test where
  keep : Nat → Bool
  cost : Nat → Nat
def survivors (ts : List Test) (rows : List Nat) :=
  rows.filter (fun r => ts.all (fun t => t.keep r))
def work : List Test → List Nat → Nat
  | [], _ => 0
  | t::ts, rs => (rs.map t.cost).sum + work ts (rs.filter t.keep)
def saved (t following : Test) (rs : List Nat) :=
  ((rs.filter (fun r => !(t.keep r))).map following.cost).sum

/-- The exact advantage of swapping adjacent predicates depends on avoided work on rejected rows. -/
def AvoidedEvaluationInterchange.Goal : Prop := ∀ a b rest rs,
  work (a::b::rest) rs ≤ work (b::a::rest) rs ↔ saved b a rs ≤ saved a b rs

/-- Standalone row counts suffice to order tests when both constant per-row costs are known. -/
def ConstantCostRejectRatio.Goal : Prop := ∀ a b ca cb rs,
  (∀ r ∈ rs, a.cost r = ca ∧ b.cost r = cb) →
  (work [a,b] rs ≤ work [b,a] rs ↔
   ((rs.filter (fun r => !(b.keep r))).length)*ca ≤ ((rs.filter (fun r => !(a.keep r))).length)*cb)

/-- An order learned on all input rows remains valid downstream of any earlier selection. -/
def GlobalSelectivityOrderTransfer.Goal : Prop := ∀ a b rs (guardRow : Nat → Bool),
  work [a,b] rs ≤ work [b,a] rs → work [a,b] (rs.filter guardRow) ≤ work [b,a] (rs.filter guardRow)

/-- Repricing disjoint morsels separately gives the complete work estimate for a fixed conjunction order. -/
def MorselEvaluationCharge.Goal : Prop := ∀ ts a b, work ts (a++b) = work ts a + work ts b

/-- Per-morsel reordering is semantically safe with pure tests, including bags and input order. -/
def MorselSpecificFilterOrder.Goal : Prop := ∀ (ts as bs : List Test) a b, as.Perm ts → bs.Perm ts →
  survivors as a ++ survivors bs b = survivors ts (a++b)

/-- Sampling only a prefix can certify that changing order will repay profiling cost across the full input. -/
def PrefixProfilePaysSwitch.Goal : Prop := ∀ a b rs n charge,
  work [b,a] (rs.take n)+charge < work [a,b] (rs.take n) → work [b,a] rs + charge < work [a,b] rs

inductive Outcome where
  | pass | reject | fault (code : Nat)
  deriving Repr, DecidableEq
def evaluate : List (Nat → Outcome) → Nat → Outcome
  | [], _ => .pass
  | f::rest, r => match f r with
    | .pass => evaluate rest r
    | other => other

/-- Reorder fallible tests if every simultaneous non-pass outcome on reachable rows is identical. -/
def FallibleFilterCommutation.Goal : Prop := ∀ (a b : Nat → Outcome) rest (rows : List Nat),
  (∀ r ∈ rows, a r ≠ .pass → b r ≠ .pass → a r = b r) →
  rows.map (evaluate (a::b::rest)) = rows.map (evaluate (b::a::rest))

/-- A cheap reject can be evaluated ahead of a fallible cast whenever the winning row set is unchanged. -/
def AcceptanceMaskPreservesErrors.Goal : Prop := ∀ a b (rs : List Nat),
  rs.filter (fun r => evaluate [a,b] r == .pass) = rs.filter (fun r => evaluate [b,a] r == .pass) →
  rs.map (evaluate [a,b]) = rs.map (evaluate [b,a])

/-- Memoize a dictionary predicate per equality class only when the observable outcome factors through that class. -/
def DictionaryOutcomeMemoization.Goal : Prop := ∀ (key : Nat → Nat) (test : Nat → Outcome) (rows : List Nat),
  (∀ x ∈ rows, ∀ y ∈ rows, key x = key y → test x = test y) →
  rows.map (fun x => (rows.find? (fun y => key y == key x)).map test) = (rows.map test).map some

end TinylambOpt.ResearchGoals.ResourcePolicies.PredicateScheduling
