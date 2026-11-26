import TinylambOpt.ResearchGoals.ResourcePolicies.WaitQueue

/-!
`MaximalCompatiblePrefix`: a shared read lease admits the whole initial reader
cohort, and an exclusive write lease admits exactly the head. SQL execution can
grant the largest prefix compatible with one lease without rechecking each ticket.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.ResourcePolicies.Goal_WaitQueue_MaximalCompatiblePrefix

open TinylambOpt.ResearchGoals.ResourcePolicies.WaitQueue

private theorem takeWhile_long (p : Ticket → Bool) :
    ∀ (xs : List Ticket) (k : Nat), k ≤ xs.length → (xs.take k).all p = true →
      k ≤ (xs.takeWhile p).length := by
  intro xs
  induction xs with
  | nil =>
      intro k hlen hall
      simp at hlen
      omega
  | cons x xs ih =>
      intro k hlen hall
      by_cases hx : p x
      · cases k with
        | zero => simp
        | succ k' =>
            have hallAll : (List.take k' xs).all p = true := by
              rw [List.all_eq_true]
              intro y hy
              exact (List.all_eq_true.mp hall) y (by simp [hy, hx])
            simp [hx]
            have hle : k' ≤ xs.length := by simp at hlen; omega
            have hres := ih k' hle hallAll
            omega
      · cases k with
        | zero => simp
        | succ k' =>
            have hxbad : p x = true := (List.all_eq_true.mp hall) x (by simp)
            simp [hx] at hxbad

theorem sound : MaximalCompatiblePrefix.Goal := by
  intro q k hkpos hklen hcond
  cases q with
  | nil => simp at hklen; omega
  | cons t ts =>
      by_cases htw : t.writer
      · simp [turn, htw]
        rcases hcond with hall | hk1
        · have htmem : t ∈ (t :: ts).take k := by
            cases k with
            | zero => omega
            | succ k' => simp
          have hread : (!t.writer) = true := (List.all_eq_true.mp hall) t htmem
          simp [htw] at hread
        · omega
      · simp [turn, htw]
        rcases hcond with hall | hk1
        · cases k with
          | zero => omega
          | succ k' =>
              have hallAll : (List.take k' ts).all (fun x : Ticket => !x.writer) = true := by
                rw [List.all_eq_true]
                intro y hy
                exact (List.all_eq_true.mp hall) y (by simp [hy])
              have hle : k' ≤ ts.length := by simp at hklen; omega
              have hres := takeWhile_long (fun x : Ticket => !x.writer) ts k' hle hallAll
              omega
        · omega

end TinylambOpt.ResearchProofs.ResourcePolicies.Goal_WaitQueue_MaximalCompatiblePrefix
