import TinylambOpt.ResearchGoals.ResourcePolicies.WaitQueue

namespace TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.WaitQueue.Goal_MaximalCompatiblePrefix

open TinylambOpt.ResearchGoals.ResourcePolicies.WaitQueue


/-! A compatible prefix of a queue is contained in the maximal turn: one
exclusive writer, or the leading run of readers. -/

namespace Scratch.MaximalCompatiblePrefix

open TinylambOpt.ResearchGoals.ResourcePolicies.WaitQueue

theorem take_all_false_of_head_writer : ∀ (t : Ticket) (ts : List Ticket) k,
    t.writer = true → 0 < k → ((t :: ts).take k).all (fun x => !x.writer) = false := by
  intro t ts k htw hk
  cases k with
  | zero => omega
  | succ k' => simp [htw]

theorem all_take_le_takeWhile : ∀ (ts : List Ticket) k,
    k ≤ ts.length → (ts.take k).all (fun x => !x.writer) = true →
    k ≤ (ts.takeWhile (fun x => !x.writer)).length := by
  intro ts
  induction ts with
  | nil =>
      intro k hk h
      simp at hk ⊢
      omega
  | cons t ts ih =>
      intro k hk h
      cases k with
      | zero => simp
      | succ k' =>
          by_cases htw : t.writer = true
          · simp [htw] at h
          · have htf : t.writer = false := by cases h' : t.writer <;> simp_all
            have hk' : k' ≤ ts.length := by
              simp at hk
              omega
            have hall : (ts.take k').all (fun x => !x.writer) = true := by
              simpa [htf] using h
            have ires := ih k' hk' hall
            simp [htf, ires]

theorem sound : TinylambOpt.ResearchGoals.ResourcePolicies.WaitQueue.MaximalCompatiblePrefix.Goal := by
  intro q k hk0 hklen hcompat
  cases q with
  | nil =>
      simp at hklen
      omega
  | cons t ts =>
      by_cases htw : t.writer = true
      · have hk1 : k = 1 := by
          rcases hcompat with hall | hk1
          · have hbad := take_all_false_of_head_writer t ts k htw hk0
            rw [hbad] at hall
            contradiction
          · exact hk1
        subst k
        simp [turn, htw]
      · have htf : t.writer = false := by cases h' : t.writer <;> simp_all
        rcases hcompat with hall | hk1
        · cases k with
          | zero => omega
          | succ k' =>
              have hk' : k' ≤ ts.length := by
                simp at hklen
                omega
              have htsall : (ts.take k').all (fun x => !x.writer) = true := by
                simpa [htf] using hall
              have hle := all_take_le_takeWhile ts k' hk' htsall
              simp [turn, htw]
              omega
        · subst k
          simp [turn, htw]

end Scratch.MaximalCompatiblePrefix
end TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.WaitQueue.Goal_MaximalCompatiblePrefix
