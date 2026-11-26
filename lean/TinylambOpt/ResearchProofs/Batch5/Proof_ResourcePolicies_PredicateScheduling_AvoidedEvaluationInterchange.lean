import TinylambOpt.ResearchGoals.ResourcePolicies.PredicateScheduling

namespace TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.PredicateScheduling.Goal_AvoidedEvaluationInterchange

open TinylambOpt.ResearchGoals.ResourcePolicies.PredicateScheduling


/-! Swapping adjacent filters is beneficial exactly when the rejected rows of
the new second filter carry more avoided cost than those of the new first. -/

namespace Scratch.AvoidedEvaluationInterchange

open TinylambOpt.ResearchGoals.ResourcePolicies.PredicateScheduling

theorem sum_partition : ∀ (f : Nat → Nat) (p : Nat → Bool) (rs : List Nat),
    (rs.map f).sum = ((rs.filter p).map f).sum + ((rs.filter (fun r => !p r)).map f).sum := by
  intro f p rs
  induction rs with
  | nil => simp
  | cons r rs ih =>
      by_cases hp : p r = true
      · simp [hp, ih]
        omega
      · have hpfalse : p r = false := by cases h : p r <;> simp_all
        simp [hpfalse, ih]
        omega

theorem filter_filter_comm : ∀ (p q : Nat → Bool) (rs : List Nat),
    (rs.filter p).filter q = (rs.filter q).filter p := by
  intro p q rs
  induction rs with
  | nil => simp
  | cons r rs ih =>
      by_cases hp : p r = true <;> by_cases hq : q r = true <;> simp [hp, hq, ih]

theorem swap_cost_balance : ∀ (a b : Test) (rest : List Test) (rs : List Nat),
    work (a :: b :: rest) rs + saved a b rs = work (b :: a :: rest) rs + saved b a rs := by
  intro a b rest rs
  unfold saved
  simp only [work]
  rw [sum_partition a.cost b.keep rs]
  rw [sum_partition b.cost a.keep rs]
  rw [filter_filter_comm b.keep a.keep rs]
  omega

theorem sound : TinylambOpt.ResearchGoals.ResourcePolicies.PredicateScheduling.AvoidedEvaluationInterchange.Goal := by
  intro a b rest rs
  have hbal := swap_cost_balance a b rest rs
  constructor <;> intro hle <;> omega

end Scratch.AvoidedEvaluationInterchange
end TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.PredicateScheduling.Goal_AvoidedEvaluationInterchange
