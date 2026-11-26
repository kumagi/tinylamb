import TinylambOpt.ResearchGoals.November26.Click

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false


namespace TinylambOpt.ResearchProofs.November26.Click.Goal_Q40
open TinylambOpt.ResearchGoals.November26.Click

def key (r : Nat × Nat × String) := (r.1, r.2.1, if r.1 == 0 && r.2.1 == 0 then r.2.2 else "")

theorem key_eq_iff (r : Nat × Nat × String) (s a : Nat) (src : String) :
    (key r == (s, a, src)) = true ↔
      (r.1 = s ∧ r.2.1 = a ∧ (if r.1 = 0 ∧ r.2.1 = 0 then r.2.2 = src else src = "")) := by
  rw [beq_iff_eq]
  dsimp [key]
  by_cases h0 : r.1 = 0 ∧ r.2.1 = 0
  · simp [h0]
  · simp [h0]

theorem count_key_zero (rows : List (Nat × Nat × String)) (src : String) :
    (rows.map key).count (0, 0, src) = rows.count (0, 0, src) := by
  rw [List.count_eq_countP, List.countP_map, List.count_eq_countP]
  apply List.countP_congr
  intro r hr
  change (key r == (0, 0, src)) = true ↔ (r == (0, 0, src)) = true
  rw [key_eq_iff]
  constructor
  · intro h
    rcases h with ⟨h1, h2, h3⟩
    rcases r with ⟨x, y, z⟩
    simp_all
  · intro h
    have heq : r = (0, 0, src) := beq_iff_eq.mp h
    subst r
    simp [key]

theorem count_key_pair (rows : List (Nat × Nat × String)) (s a : Nat)
    (hnot : ¬(s = 0 ∧ a = 0)) :
    (rows.map key).count (s, a, "") = (rows.map (fun r => (r.1, r.2.1))).count (s, a) := by
  rw [List.count_eq_countP, List.countP_map, List.count_eq_countP, List.countP_map]
  apply List.countP_congr
  intro r hr
  change (key r == (s, a, "")) = true ↔ ((r.1, r.2.1) == (s, a)) = true
  rw [key_eq_iff]
  constructor
  · intro h
    exact beq_iff_eq.mpr (Prod.ext h.1 h.2.1)
  · intro h
    have hp : (r.1, r.2.1) = (s, a) := beq_iff_eq.mp h
    refine ⟨congrArg Prod.fst hp, congrArg Prod.snd hp, ?_⟩
    by_cases h0 : r.1 = 0 ∧ r.2.1 = 0
    · exfalso
      apply hnot
      constructor
      · exact (congrArg Prod.fst hp).symm.trans h0.1
      · exact (congrArg Prod.snd hp).symm.trans h0.2
    · simp [h0]

theorem count_key_none (rows : List (Nat × Nat × String)) (s a : Nat) (src : String)
    (hnot : ¬(s = 0 ∧ a = 0)) (hsrc : src ≠ "") :
    (rows.map key).count (s, a, src) = 0 := by
  rw [List.count_eq_countP, List.countP_map, List.countP_eq_zero]
  intro r hr
  change ¬ (key r == (s, a, src)) = true
  rw [key_eq_iff]
  intro h
  rcases h with ⟨h1, h2, h3⟩
  by_cases h0 : r.1 = 0 ∧ r.2.1 = 0
  · exfalso
    apply hnot
    constructor
    · exact h1.symm.trans h0.1
    · exact h2.symm.trans h0.2
  · simp [h0] at h3
    exact hsrc h3

theorem sound : TinylambOpt.ResearchGoals.November26.Click.Q40.Goal := by
  intro rows s a src key
  change (rows.map key).count (s, a, src) =
    (if s == 0 && a == 0 then rows.count (0, 0, src)
     else if src == "" then (rows.map (fun r => (r.1, r.2.1))).count (s, a) else 0)
  change (rows.map TinylambOpt.ResearchProofs.November26.Click.Goal_Q40.key).count (s, a, src) =
    (if s == 0 && a == 0 then rows.count (0, 0, src)
     else if src == "" then (rows.map (fun r => (r.1, r.2.1))).count (s, a) else 0)
  by_cases h0 : s == 0 && a == 0
  · have h0p : s = 0 ∧ a = 0 := by
      simpa only [Bool.and_eq_true, beq_iff_eq] using h0
    rcases h0p with ⟨rfl, rfl⟩
    simp [count_key_zero]
  · have hnot : ¬(s = 0 ∧ a = 0) := by
      intro h
      apply h0
      simp [h.1, h.2]
    simp [hnot]
    by_cases hsrc : src == ""
    · have hsrcp : src = "" := beq_iff_eq.mp hsrc
      subst src
      exact count_key_pair rows s a hnot
    · have hsrcne : src ≠ "" := by intro h; apply hsrc; simp [h]
      rw [if_neg hsrcne]
      exact count_key_none rows s a src hnot hsrcne
end TinylambOpt.ResearchProofs.November26.Click.Goal_Q40
