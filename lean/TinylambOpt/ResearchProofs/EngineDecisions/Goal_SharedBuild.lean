import TinylambOpt.ResearchGoals.EngineDecisions.Optimizer

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.EngineDecisions.Goal_SharedBuild

open TinylambOpt.ResearchGoals.EngineDecisions.Optimizer.SharedBuild

private theorem eraseDups_filter_not_contains_self (xs : List Nat) :
    (xs.eraseDups.filter (fun k => !xs.contains k)) = [] := by
  apply List.filter_eq_nil_iff.mpr
  intro k hk
  have hmem : k ∈ xs := List.mem_of_mem_eraseDups hk
  have hc : xs.contains k = true := List.contains_iff_mem.mpr hmem
  simp [hc]

private theorem extra_self (price : Nat → Nat) (xs : List Nat) : extra price xs xs = 0 := by
  unfold extra
  simp [eraseDups_filter_not_contains_self]

private theorem sum_map_filter_le_add (price : Nat → Nat) (available b : List Nat) (xs : List Nat) :
    ((xs.filter (fun k => !available.contains k)).map price).sum ≤
    ((xs.filter (fun k => !b.contains k)).map price).sum +
    ((b.eraseDups.filter (fun k => !available.contains k)).map price).sum := by
  induction xs with
  | nil => simp
  | cons k ks ih =>
      by_cases ha : available.contains k = true
      · simp [ha]
        omega
      · have haf : available.contains k = false := by cases h : available.contains k <;> simp_all
        by_cases hb : b.contains k = true
        · -- k in b, not in available: contributes to right side's second term
          have hmem : k ∈ b.eraseDups := List.mem_eraseDups.mpr (List.contains_iff_mem.mp hb)
          have hmemf : k ∈ b.eraseDups.filter (fun k => !available.contains k) := by
            simp [List.mem_filter, hmem, haf]
          -- sum of b.eraseDups.filter(!avail) >= price k + sum of same without k
          have hle : price k + ((b.eraseDups.filter (fun k => !available.contains k)).map price).sum ≤
              ((b.eraseDups.filter (fun k => !available.contains k)).map price).sum + price k := by omega
          -- better: split off k
          simp [haf, hb]
          have ih2 := ih
          -- ((ks.filter(!avail)).map price).sum <= ...
          -- goal: price k + ((ks.filter(!avail)).map price).sum <= ((ks.filter(!b)).map price).sum + ((b.eraseDups.filter(!avail)).map price).sum
          -- we know ((b.eraseDups.filter(!avail)).map price).sum includes price k
          have hsplit : ((b.eraseDups.filter (fun k => !available.contains k)).map price).sum =
              price k + (((b.eraseDups.filter (fun k => !available.contains k)).erase k).map price).sum := by
            rw [← List.sum_map_erase (f := price) hmemf]
            simp
          rw [hsplit]
          omega
        · have hbf : b.contains k = false := by cases h : b.contains k <;> simp_all
          simp [haf, hbf]
          omega

theorem sound : Goal := by
  intro price a b
  constructor
  · intro h
    have := h b.builds
    simp [cost, extra_self] at this
    exact this
  · intro h available
    unfold cost extra at *
    have hex := sum_map_filter_le_add price available b.builds a.builds.eraseDups
    omega

end TinylambOpt.ResearchProofs.EngineDecisions.Goal_SharedBuild
