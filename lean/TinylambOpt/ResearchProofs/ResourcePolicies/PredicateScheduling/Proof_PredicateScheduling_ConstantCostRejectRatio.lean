import TinylambOpt.ResearchGoals.ResourcePolicies.PredicateScheduling
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace Nov26.Proof_PredicateScheduling_ConstantCostRejectRatio
open TinylambOpt.ResearchGoals.ResourcePolicies.PredicateScheduling
open TinylambOpt.ResearchGoals.ResourcePolicies.PredicateScheduling.ConstantCostRejectRatio

private theorem sum_map_const_of_forall (f : Nat → Nat) (c : Nat) (l : List Nat)
    (h : ∀ r ∈ l, f r = c) : (l.map f).sum = c * l.length := by
  induction l with
  | nil => simp
  | cons x xs ih =>
    simp only [List.map_cons, List.sum_cons, List.length_cons]
    have hx : f x = c := h x (by simp)
    have hxs : (xs.map f).sum = c * xs.length := ih (fun r hr => h r (by simp [hr]))
    rw [hx, hxs, Nat.mul_add, Nat.mul_one]; omega

theorem sound : Goal := by
  intro a b ca cb rs hcost
  have hca : ∀ r ∈ rs, a.cost r = ca := fun r hr => (hcost r hr).1
  have hcb : ∀ r ∈ rs, b.cost r = cb := fun r hr => (hcost r hr).2
  let ka := (rs.filter a.keep).length
  let kna := (rs.filter (fun r => !a.keep r)).length
  let kb := (rs.filter b.keep).length
  let knb := (rs.filter (fun r => !b.keep r)).length
  have hlen_not : ∀ (p : Nat → Bool) (l : List Nat),
      (l.filter p).length + (l.filter (fun r => !p r)).length = l.length := by
    intro p l; induction l with
    | nil => simp
    | cons x xs ih => by_cases hp : p x <;> simp [List.filter, hp, ih] <;> omega
  have ha_split : ka + kna = rs.length := by simpa [ka, kna] using (hlen_not a.keep rs)
  have hb_split : kb + knb = rs.length := by simpa [kb, knb] using (hlen_not b.keep rs)
  have hsumA := sum_map_const_of_forall a.cost ca rs hca
  have hsumB := sum_map_const_of_forall b.cost cb rs hcb
  have hsumBa := sum_map_const_of_forall b.cost cb (rs.filter a.keep)
    (fun r hr => hcb r (List.mem_filter.mp hr).1)
  have hsumAb := sum_map_const_of_forall a.cost ca (rs.filter b.keep)
    (fun r hr => hca r (List.mem_filter.mp hr).1)
  have key : ca * rs.length + cb * ka ≤ cb * rs.length + ca * kb ↔ knb * ca ≤ kna * cb := by
    have hk : ka + kna = kb + knb := by omega
    have d1 : ca * (ka + kna) = ca * ka + ca * kna := Nat.mul_add _ _ _
    have d2 : cb * (ka + kna) = cb * ka + cb * kna := Nat.mul_add _ _ _
    have d3 : ca * (kb + knb) = ca * kb + ca * knb := Nat.mul_add _ _ _
    have d4 : cb * (kb + knb) = cb * kb + cb * knb := Nat.mul_add _ _ _
    rw [hk] at d1 d2
    have e1 : ca * ka + ca * kna = ca * kb + ca * knb := by rw [← d1, d3]
    have e2 : cb * ka + cb * kna = cb * kb + cb * knb := by rw [← d2, d4]
    rw [← ha_split]
    -- distribute
    rw [Nat.mul_add ca ka kna, Nat.mul_add cb ka kna]
    -- now linear in product atoms; generalize to make omega happy
    let x1 := ca * ka
    let x2 := ca * kna
    let x3 := cb * ka
    let x4 := cb * kna
    let x5 := ca * kb
    let x6 := ca * knb
    let x7 := cb * kb
    let x8 := cb * knb
    have e1' : x1 + x2 = x5 + x6 := by simpa [x1,x2,x5,x6] using e1
    have e2' : x3 + x4 = x7 + x8 := by simpa [x3,x4,x7,x8] using e2
    have r1 : knb * ca = x6 := by simp [x6, Nat.mul_comm]
    have r2 : kna * cb = x4 := by simp [x4, Nat.mul_comm]
    rw [r1, r2]
    constructor <;> intro h <;> omega
  simp only [work, Nat.add_zero, hsumA, hsumB, hsumBa, hsumAb]
  exact key
end Nov26.Proof_PredicateScheduling_ConstantCostRejectRatio
