import TinylambOpt.ResearchGoals.ResourcePolicies.GrantAdmission

namespace TinylambOpt.ResearchProofs.ResourcePolicies.GrantAdmission

open TinylambOpt.ResearchGoals.ResourcePolicies.GrantAdmission

private theorem sum_map_le_sum_map_const {l : List Nat} {c : Nat} (h : ∀ x ∈ l, x ≤ c) :
    l.sum ≤ l.length * c := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      have hx : x ≤ c := h x List.mem_cons_self
      have hxs : ∀ y ∈ xs, y ≤ c := fun y hy => h y (List.mem_cons_of_mem x hy)
      simp only [List.sum_cons, List.length_cons, Nat.succ_mul]
      have := ih hxs
      omega

theorem emergencyFloorAcrossClients : EmergencyFloorAcrossClients.Goal := by
  intro buffers floor maxClients hlen hall
  calc buffers.sum
      ≤ buffers.length * floor := sum_map_le_sum_map_const hall
    _ ≤ maxClients * floor := Nat.mul_le_mul_right floor hlen
