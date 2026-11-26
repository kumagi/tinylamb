import TinylambOpt.ResearchGoals.November26.Click

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false


namespace TinylambOpt.ResearchProofs.November26.Click.Goal_Q32
open TinylambOpt.ResearchGoals.November26.Click

theorem length_sub_filter_ne (ips : List Nat) (first : Nat) :
    ips.length - (ips.filter (fun x => x != first)).length = ips.count first := by
  induction ips with
  | nil => simp
  | cons x xs ih =>
      by_cases hx : x == first
      · have hxeq : x = first := beq_iff_eq.mp hx
        simp [List.count_cons, hxeq]
        have hle := List.length_filter_le (fun y => y != first) xs
        omega
      · have hxne : x ≠ first := by
          intro h
          apply hx
          simp [h]
        simp [hxne, ih]

theorem count_filter_ne (ips : List Nat) (first query : Nat) (hq : ¬ query == first) :
    (ips.filter (fun x => x != first)).count query = ips.count query := by
  have hqne : query ≠ first := by
    intro h
    apply hq
    simp [h]
  induction ips with
  | nil => simp
  | cons x xs ih =>
      by_cases hx : x == first
      · have hxeq : x = first := beq_iff_eq.mp hx
        have hneq : first ≠ query := fun h => hqne h.symm
        simp [List.count_cons, hxeq, hneq, ih]
      · have hxne : x ≠ first := by
          intro h
          apply hx
          simp [h]
        by_cases hxq : x == query <;> simp [List.count_cons, hxne, hxq, ih]

theorem sound : TinylambOpt.ResearchGoals.November26.Click.Q32.Goal := by
  intro ips first query
  dsimp
  by_cases hq : query == first
  · have hlen := length_sub_filter_ne ips first
    have hqeq : query = first := beq_iff_eq.mp hq
    simp [hqeq, hlen]
  · have hc := count_filter_ne ips first query hq
    have hqne : query ≠ first := by
      intro h
      apply hq
      simp [h]
    simp [hqne, hc]
end TinylambOpt.ResearchProofs.November26.Click.Goal_Q32
