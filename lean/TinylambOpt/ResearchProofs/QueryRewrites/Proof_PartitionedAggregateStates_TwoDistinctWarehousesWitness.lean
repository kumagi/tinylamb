import TinylambOpt.ResearchProofs.QueryRewrites.KeyGrouping
import TinylambOpt.ResearchGoals.QueryRewrites.PartitionedAggregateStates

namespace TinylambOpt.ResearchProofs.QueryRewrites.PartitionedAggregateStates.Goal_TwoDistinctWarehousesWitness

open TinylambOpt.ResearchGoals.QueryRewrites.PartitionedAggregateStates


/-!
Two distinct non-NULL warehouse witnesses suffice: among two distinct values at
most one can equal the probe key, so a qualifying witness survives in the first
two entries. The rewritten existence probe therefore agrees with scanning all
warehouses, including when the probe key itself is absent.
-/

set_option maxHeartbeats 0
set_option linter.unusedSimpArgs false

open TinylambOpt.ResearchProofs.QueryRewrites.KeyGrouping
open TinylambOpt.ResearchGoals.QueryRewrites.PartitionedAggregateStates

/-- For a nodup list, any key-different witness appears within the first two entries. -/
theorem nodup_take_two_any_ne {d : List Nat} (hnd : d.Nodup) (k : Nat) :
    (d.take 2).any (fun a => a != k) = d.any (fun a => a != k) := by
  cases d with
  | nil => simp
  | cons a d =>
      cases d with
      | nil => simp
      | cons b d =>
          simp only [List.take, List.any_cons, List.any_nil, Bool.or_false]
          by_cases ha : (a == k) = true
          · have haeq : a = k := beq_iff_eq.mp ha
            have hbne_nat : b ≠ a := by
              intro h
              exact (List.nodup_cons.mp hnd).1 (by simp [h])
            have hbne : (b != k) = true := by
              simp [show b ≠ k by
                intro hbk
                exact hbne_nat (hbk.trans haeq.symm)]
            have hane : (a != k) = false := by simp [haeq]
            simp [hane, hbne]
          · have hneq : a ≠ k := by
              intro h
              apply ha
              simp [h]
            have hane : (a != k) = true := by simp [hneq]
            simp [hane]

/-- Existence of a non-NULL, key-different warehouse is existence in the distinct list. -/
theorem any_warehouses_eq_distinct_any (warehouses : List (Option Nat)) (k : Nat) :
    warehouses.any (fun x => x.any (fun y => y != k)) =
      (warehouses.filterMap id).eraseDups.any (fun a => a != k) := by
  let d := (warehouses.filterMap id).eraseDups
  change warehouses.any (fun x => x.any (fun y => y != k)) = d.any (fun a => a != k)
  have hiff : (d.any (fun a => a != k) = true) ↔
      (warehouses.any (fun x => x.any (fun y => y != k)) = true) := by
    rw [List.any_eq_true, List.any_eq_true]
    constructor
    · rintro ⟨a, ha, hane⟩
      have ha' : a ∈ warehouses.filterMap id := (List.mem_eraseDups).mp ha
      rcases (List.mem_filterMap.mp ha') with ⟨x, hx, hxeq⟩
      cases x with
      | none => simp at hxeq
      | some y =>
          have hya : y = a := by simpa using hxeq
          subst y
          refine ⟨some a, hx, ?_⟩
          simpa using hane
    · rintro ⟨x, hx, hxany⟩
      cases x with
      | none => simp at hxany
      | some a =>
          have hane : (a != k) = true := by simpa using hxany
          refine ⟨a, ?_, hane⟩
          apply List.mem_eraseDups.mpr
          apply List.mem_filterMap.mpr
          exact ⟨some a, hx, rfl⟩
  by_cases hp : d.any (fun a => a != k) = true
  · have hq : warehouses.any (fun x => x.any (fun y => y != k)) = true := hiff.mp hp
    simp [hp, hq]
  · have hqfalse : warehouses.any (fun x => x.any (fun y => y != k)) = false := by
      cases hq : warehouses.any (fun x => x.any (fun y => y != k)) <;> simp_all
    simp [hp, hqfalse]

theorem sound : TwoDistinctWarehousesWitness.Goal := by
  intro warehouses k
  dsimp
  rw [any_warehouses_eq_distinct_any]
  exact nodup_take_two_any_ne (nodup_eraseDups (warehouses.filterMap id)) k
end TinylambOpt.ResearchProofs.QueryRewrites.PartitionedAggregateStates.Goal_TwoDistinctWarehousesWitness
