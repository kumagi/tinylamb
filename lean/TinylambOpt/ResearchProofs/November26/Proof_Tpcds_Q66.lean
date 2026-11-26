import TinylambOpt.ResearchGoals.November26.Tpcds

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unnecessarySimpa false

namespace TinylambOpt.ResearchProofs.November26.Tpcds.Goal_Q66
open TinylambOpt.ResearchGoals.November26

theorem key_eq_iff (attrs : Nat → String) (id month w m : Nat) :
    (((id, attrs id, month) : Nat × String × Nat) == (w, attrs w, m)) =
    (((id, month) : Nat × Nat) == (w, m)) := by
  by_cases hid : id == w
  · have hidEq : id = w := beq_iff_eq.mp hid
    subst id
    simp [BEq.beq]
  · have hidne : id ≠ w := by
      intro h
      apply hid
      simp [h]
    simp [BEq.beq, hidne]

theorem sound : TinylambOpt.ResearchGoals.November26.Tpcds.Q66.Goal := by
  intro facts attrs w m
  dsimp
  unfold forKey
  induction facts with
  | nil => rfl
  | cons f fs ih =>
    rcases f with ⟨⟨id, month⟩, v⟩
    simp only [List.map_cons, List.filter_cons]
    rw [key_eq_iff attrs id month w m]
    by_cases hsame : id = w ∧ month = m
    · simp [hsame, ih]
    · simp [hsame, ih]
end TinylambOpt.ResearchProofs.November26.Tpcds.Goal_Q66
