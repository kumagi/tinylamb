import TinylambOpt.ResearchGoals.ClickstreamLayout.Execution

namespace TinylambOpt.ResearchProofs.Batch5.ClickstreamLayout.Execution.Goal_ClockRange

open TinylambOpt.ResearchGoals.ClickstreamLayout.Execution


/-!
These models describe encoded clickstream columns and the predicates and aggregates that
can consume them.

Proved

- `ClockRange` — For a nonempty selection interval, deciding from the base alone is
  possible exactly when every admissible residual gives the same answer. Escapes
  still get the same WHERE result as a full timestamp decode.
-/
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unusedSimpArgs false

namespace Scratch.ClickstreamLayout.ClockRange

open TinylambOpt.ResearchGoals.ClickstreamLayout.Execution
open ClockRange

private theorem orBool {p q : Prop} [Decidable p] [Decidable q] (h : p ∨ q) : (decide p || decide q) = true := by
  rw [Bool.or_eq_true_iff]
  rcases h with hp | hq
  · exact .inl (by rw [decide_eq_true_eq]; exact hp)
  · exact .inr (by rw [decide_eq_true_eq]; exact hq)

private theorem andBool {p q : Prop} [Decidable p] [Decidable q] (h : p ∧ q) : (decide p && decide q) = true := by
  rw [Bool.and_eq_true]
  exact ⟨by rw [decide_eq_true_eq]; exact h.1, by rw [decide_eq_true_eq]; exact h.2⟩

private theorem orOfOrBool {p q : Prop} [Decidable p] [Decidable q] (h : (decide p || decide q) = true) : p ∨ q := by
  rcases Bool.or_eq_true_iff.mp h with hp | hq
  · exact .inl (of_decide_eq_true hp)
  · exact .inr (of_decide_eq_true hq)

private theorem notOr {p q : Prop} [Decidable p] [Decidable q] (h : ¬ (decide p || decide q) = true) : ¬ p ∧ ¬ q := by
  rcases Decidable.em p with hp | hp
  · exact absurd (orBool (Or.inl hp)) h
  · rcases Decidable.em q with hq | hq
    · exact absurd (orBool (Or.inr hq)) h
    · exact ⟨hp, hq⟩

private theorem notAnd {p q : Prop} [Decidable p] [Decidable q] (h : ¬ (decide p && decide q) = true) : ¬ p ∨ ¬ q := by
  rcases Decidable.em p with hp | hp
  · right
    intro hq
    exact h (andBool ⟨hp, hq⟩)
  · left; exact hp

private theorem inside_true (l h v : Int) (h1 : l ≤ v) (h2 : v < h) :
    inside l h v = true := by
  unfold ClockRange.inside
  rw [Bool.and_eq_true]
  refine ⟨?_, ?_⟩ <;> rw [decide_eq_true_eq] <;> assumption

private theorem inside_false (l h v : Int) (h1 : ¬ (l ≤ v ∧ v < h)) :
    inside l h v = false := by
  rcases Decidable.em (l ≤ v) with a | a
  · rcases Decidable.em (v < h) with b | b
    · exact absurd ⟨a, b⟩ h1
    · unfold ClockRange.inside
      rw [decide_eq_false b, Bool.and_false]
  · unfold ClockRange.inside
    rw [decide_eq_false a, Bool.false_and]

private theorem probe_delta_eq (l h a b base d : Int) :
    probe l h a b base (Slot.delta d) =
      (if base + b < l || h ≤ base + a then (false, (0 : Nat))
       else if l ≤ base + a && base + b < h then (true, (0 : Nat))
       else (inside l h (base + d), (1 : Nat))) := rfl

private theorem plain_delta (base d : Int) : plain base (Slot.delta d) = some (base + d) := rfl

theorem sound : TinylambOpt.ResearchGoals.ClickstreamLayout.Execution.ClockRange.Goal := by
  intro l h a b base hl hab
  constructor
  · intro s hs
    cases s with
    | missing => rfl
    | absolute v => rfl
    | delta d =>
      have hd : a ≤ d ∧ d ≤ b := hs d rfl
      rw [probe_delta_eq, plain_delta, Option.any_some]
      by_cases h1 : (base + b < l || h ≤ base + a) = true
      · rw [if_pos h1]
        refine (inside_false l h (base + d) ?_).symm
        rcases orOfOrBool (p := base + b < l) (q := h ≤ base + a) h1 with c1 | c1
        · omega
        · omega
      · by_cases h2 : (l ≤ base + a && base + b < h) = true
        · rw [if_neg h1, if_pos h2]
          have hlo : l ≤ base + a := by
            by_cases lo : l ≤ base + a
            · exact lo
            · exfalso
              have hb : (l ≤ base + a && base + b < h) = false := by
                rw [decide_eq_false lo, Bool.false_and]
              rw [hb] at h2
              exact absurd h2 (by decide)
          have hhi : base + b < h := by
            by_cases hi : base + b < h
            · exact hi
            · exfalso
              have hb : (l ≤ base + a && base + b < h) = false := by
                rw [decide_eq_false hi, Bool.and_false]
              rw [hb] at h2
              exact absurd h2 (by decide)
          show true = inside l h (base + d)
          exact (inside_true l h (base + d) (by omega) (by omega)).symm
        · rw [if_neg h1, if_neg h2]
  · constructor
    · intro hc
      by_cases h1 : (base + b < l || h ≤ base + a) = true
      · intro d dad dbd
        have hnot : ∀ x : Int, a ≤ x → x ≤ b → inside l h (base + x) = false := by
          intro x hx1 hx2
          refine inside_false l h (base + x) ?_
          rcases orOfOrBool (p := base + b < l) (q := h ≤ base + a) h1 with c1 | c1
          · omega
          · omega
        rw [hnot d dad dbd, hnot a (by omega) (by omega)]
      · rw [probe_delta_eq, if_neg h1] at hc
        by_cases h2 : (l ≤ base + a && base + b < h) = true
        · intro d dad dbd
          have hlo : l ≤ base + a := by
            by_cases lo : l ≤ base + a
            · exact lo
            · exfalso
              have hb : (l ≤ base + a && base + b < h) = false := by
                rw [decide_eq_false lo, Bool.false_and]
              rw [hb] at h2
              exact absurd h2 (by decide)
          have hhi : base + b < h := by
            by_cases hi : base + b < h
            · exact hi
            · exfalso
              have hb : (l ≤ base + a && base + b < h) = false := by
                rw [decide_eq_false hi, Bool.and_false]
              rw [hb] at h2
              exact absurd h2 (by decide)
          rw [inside_true l h (base + d) (by omega) (by omega),
            inside_true l h (base + a) hlo (by omega)]
        · rw [if_neg h2] at hc
          simp at hc
    · intro hall
      by_cases h1 : (base + b < l || h ≤ base + a) = true
      · rw [probe_delta_eq, if_pos h1]
      · rw [probe_delta_eq, if_neg h1]
        by_cases h2 : (l ≤ base + a && base + b < h) = true
        · rw [if_pos h2]
        · exfalso
          have ⟨n1, n2⟩ : ¬ (base + b < l) ∧ ¬ (h ≤ base + a) :=
            notOr (p := base + b < l) (q := h ≤ base + a) h1
          rcases notAnd (p := l ≤ base + a) (q := base + b < h) h2 with q1 | q2
          · -- base + a < l; witness d = l - base
            have hEq := hall (l - base) (by omega) (by omega)
            have hsum : base + (l - base) = l := by omega
            rw [hsum] at hEq
            rw [inside_true l h l (by omega) hl,
              inside_false l h (base + a) (by omega)] at hEq
            simp at hEq
          · -- base + b ≥ h
            by_cases lo : l ≤ base + a
            · -- witness d = b
              have hEq := hall b hab (by omega)
              rw [inside_false l h (base + b) (by omega),
                inside_true l h (base + a) lo (by omega)] at hEq
              simp at hEq
            · -- witness d = l - base
              have hEq := hall (l - base) (by omega) (by omega)
              rw [show base + (l - base) = l from by omega] at hEq
              rw [inside_true l h l (by omega) hl,
                inside_false l h (base + a) (by omega)] at hEq
              simp at hEq

end Scratch.ClickstreamLayout.ClockRange
end TinylambOpt.ResearchProofs.Batch5.ClickstreamLayout.Execution.Goal_ClockRange
