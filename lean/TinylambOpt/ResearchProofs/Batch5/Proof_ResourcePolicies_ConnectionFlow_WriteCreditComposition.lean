import TinylambOpt.ResearchGoals.ResourcePolicies.ConnectionFlow

namespace TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.ConnectionFlow.Goal_WriteCreditComposition

open TinylambOpt.ResearchGoals.ResourcePolicies.ConnectionFlow


/-! Composition of socket write credits: two successive partial-send callbacks
with acknowledged byte counts `a` and `b` leave the same unwritten frame suffix
as one callback for `a + b`. This is a structural property of `remaining`, not
an assumption about the socket. -/
set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace WccProof

open TinylambOpt.ResearchGoals.ResourcePolicies.ConnectionFlow

/-- Structural induction on the pending frames; arithmetic on the two cuts is
handled by Presburger reasoning. -/
theorem remaining_compose : ∀ (fs : Frames) (a b : Nat),
    remaining b (remaining a fs) = remaining (a + b) fs
  | [], _, _ => by simp [remaining]
  | f :: fs, a, b => by
      by_cases ha : a < f.length
      · rw [remaining, ite_eq_left_iff.mpr (fun hn => (hn ha).elim)]
        by_cases hb : b < (f.drop a).length
        · have hb' : b < f.length - a := by simpa only [List.length_drop] using hb
          have hsum : a + b < f.length := by omega
          rw [remaining, ite_eq_left_iff.mpr (fun hn => (hn hb).elim), remaining, ite_eq_left_iff.mpr (fun hn => (hn hsum).elim)]
          congr 1
          rw [List.drop_drop]
        · have hb' : f.length - a ≤ b := by
            simpa only [List.length_drop, Nat.not_lt] using hb
          have hsum : f.length ≤ a + b := by omega
          rw [remaining, ite_eq_right_iff.mpr (fun hp => (hb hp).elim), remaining, ite_eq_right_iff.mpr (fun hp => by omega)]
          have heq : b - (f.length - a) = a + b - f.length := by omega
          simp only [List.length_drop]
          rw [heq]
      · rw [remaining, ite_eq_right_iff.mpr (fun hp => (ha hp).elim), remaining_compose]
        rw [remaining, ite_eq_right_iff.mpr (fun hp => by omega)]
        congr 1
        omega

end WccProof

open TinylambOpt.ResearchGoals.ResourcePolicies.ConnectionFlow

theorem sound : TinylambOpt.ResearchGoals.ResourcePolicies.ConnectionFlow.WriteCreditComposition.Goal :=
  WccProof.remaining_compose
end TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.ConnectionFlow.Goal_WriteCreditComposition
