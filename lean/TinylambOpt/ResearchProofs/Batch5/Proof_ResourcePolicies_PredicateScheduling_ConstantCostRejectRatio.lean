import TinylambOpt.ResearchGoals.ResourcePolicies.PredicateScheduling

namespace TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.PredicateScheduling.Goal_ConstantCostRejectRatio

open TinylambOpt.ResearchGoals.ResourcePolicies.PredicateScheduling


/-! With constant per-row costs, reordering is decided solely by how many rows
each filter rejects. The proof reuses the exact avoided-work balance. -/

namespace Scratch.ConstantCostRejectRatio

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

theorem work_add_saved : ∀ (a b : Test) (rs : List Nat),
    work [a, b] rs + saved a b rs = (rs.map a.cost).sum + (rs.map b.cost).sum := by
  intro a b rs
  unfold saved
  simp only [work]
  rw [sum_partition b.cost a.keep rs]
  omega

theorem map_cost_eq_const : ∀ (f : Nat → Nat) (c : Nat) (rs : List Nat),
    (∀ r ∈ rs, f r = c) → (rs.map f).sum = rs.length * c := by
  intro f c rs
  induction rs with
  | nil => simp
  | cons r rs ih =>
      intro h
      have hr : f r = c := h r (by simp)
      have htail : ∀ x ∈ rs, f x = c := by
        intro x hx
        exact h x (by simp [hx])
      simp [hr, ih htail, Nat.succ_mul]
      omega

theorem sound : TinylambOpt.ResearchGoals.ResourcePolicies.PredicateScheduling.ConstantCostRejectRatio.Goal := by
  intro a b ca cb rs hcost
  have hA : (rs.map a.cost).sum = rs.length * ca :=
    map_cost_eq_const a.cost ca rs (fun r hr => (hcost r hr).1)
  have hB : (rs.map b.cost).sum = rs.length * cb :=
    map_cost_eq_const b.cost cb rs (fun r hr => (hcost r hr).2)
  have hsaveAB : saved a b rs = ((rs.filter (fun r => !a.keep r)).length) * cb := by
    unfold saved
    apply map_cost_eq_const
    intro r hr
    exact (hcost r (List.mem_filter.mp hr).1).2
  have hsaveBA : saved b a rs = ((rs.filter (fun r => !b.keep r)).length) * ca := by
    unfold saved
    apply map_cost_eq_const
    intro r hr
    exact (hcost r (List.mem_filter.mp hr).1).1
  have hbal1 := work_add_saved a b rs
  have hbal2 := work_add_saved b a rs
  rw [hA, hB, hsaveAB] at hbal1
  rw [hB, hA, hsaveBA] at hbal2
  constructor <;> intro hle <;> omega

end Scratch.ConstantCostRejectRatio
end TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.PredicateScheduling.Goal_ConstantCostRejectRatio
