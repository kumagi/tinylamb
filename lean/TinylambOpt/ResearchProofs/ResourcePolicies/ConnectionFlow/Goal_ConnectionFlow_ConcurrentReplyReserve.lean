import TinylambOpt.ResearchGoals.ResourcePolicies.ConnectionFlow

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false
set_option linter.unusedSimpArgs false
set_option linter.deprecated false

namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_ConnectionFlow_ConcurrentReplyReserve
open TinylambOpt.ResearchGoals.ResourcePolicies.ConnectionFlow

/-- Private helper: every entry of a list being `≤ k` bounds its sum by
`length * k`. -/
private theorem sum_le {k : Nat} : ∀ (loads : List Nat),
    (∀ n ∈ loads, n ≤ k) → loads.sum ≤ loads.length * k := by
  intro loads
  induction loads with
  | nil => intro _; simp
  | cons x xs ih =>
    intro h
    simp only [List.sum_cons, List.length_cons]
    rw [Nat.add_mul, Nat.one_mul]
    have hx : x ≤ k := h x (by simp)
    have hrest := ih fun n hn => h n (by simp [hn])
    omega

/-- Summing per-connection post-admission slack (each `≤ high + maxFrame`) plus a
reserved control frame bounds all pending output by `connections *
(high + maxFrame + control)`. -/
theorem sound : ConcurrentReplyReserve.Goal := by
  intro loads hi maxFrame control h
  have hsum : (loads.map (fun n => n + control)).sum = loads.sum + loads.length * control := by
    induction loads with
    | nil => simp
    | cons x xs ih =>
      simp only [List.map_cons, List.sum_cons, List.length_cons]
      rw [Nat.add_mul, Nat.one_mul]
      rw [ih fun n hn => h n (List.mem_cons_of_mem x hn)]
      omega
  have hb : loads.sum ≤ loads.length * hi + loads.length * maxFrame := by
    have := sum_le loads (k := hi + maxFrame) h
    rwa [← Nat.mul_add]
  rw [hsum, Nat.mul_add, Nat.mul_add]
  omega

end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_ConnectionFlow_ConcurrentReplyReserve
