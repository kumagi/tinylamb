import TinylambOpt.ResearchGoals.ClickBenchLayout.Execution

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false

namespace TinylambOpt.ResearchProofs.ClickBenchLayout.Goal_CB_ClockRange

open TinylambOpt.ResearchGoals.ClickBenchLayout.Execution
open ClockRange

private theorem d_true {p : Prop} [Decidable p] (hp : p) : decide p = true := decide_eq_true hp
private theorem d_false {p : Prop} [Decidable p] (hp : ¬p) : decide p = false := decide_eq_false hp

private theorem insT (l h v : Int) (h1 : l ≤ v) (h2 : v < h) : inside l h v = true := by
  show (decide (l ≤ v) && decide (v < h)) = true
  rw [d_true h1, d_true h2]
  exact rfl

private theorem insF (l h v : Int) (h1 : ¬(l ≤ v)) : inside l h v = false := by
  show (decide (l ≤ v) && decide (v < h)) = false
  rw [d_false h1, Bool.false_and]

private theorem insF' (l h v : Int) (h2 : ¬(v < h)) : inside l h v = false := by
  show (decide (l ≤ v) && decide (v < h)) = false
  rw [d_false h2, Bool.and_false]

private theorem hsplit (l h a b base d : Int) :
    probe l h a b base (.delta d) =
      (if (decide (base + b < l) || decide (h ≤ base + a)) = true then (false, 0)
       else if (decide (l ≤ base + a) && decide (base + b < h)) = true then (true, 0)
       else (inside l h (base + d), 1)) := rfl

private theorem g1T (l h a b base : Int)
    (hc : (decide (base + b < l) || decide (h ≤ base + a)) = true) :
    base + b < l ∨ h ≤ base + a := by
  rw [Bool.or_eq_true] at hc
  rcases hc with e1 | e2
  · exact Or.inl (of_decide_eq_true e1)
  · exact Or.inr (of_decide_eq_true e2)

private theorem g1N (l h a b base : Int)
    (hc : ¬((decide (base + b < l) || decide (h ≤ base + a)) = true)) :
    ¬(base + b < l) ∧ ¬(h ≤ base + a) :=
  ⟨fun hh => hc (by rw [d_true hh, Bool.true_or]),
   fun hh => hc (by rw [d_true hh, Bool.or_true])⟩

private theorem g2N (l h a b base : Int)
    (hc : ¬((decide (l ≤ base + a) && decide (base + b < h)) = true)) :
    ¬(l ≤ base + a) ∨ ¬(base + b < h) := by
  by_cases hl : l ≤ base + a
  · right
    intro hh
    apply hc
    rw [d_true hl, Bool.true_and, d_true hh]
  · left; exact hl

private theorem g2T (l h a b base : Int)
    (hc : (decide (l ≤ base + a) && decide (base + b < h)) = true) :
    l ≤ base + a ∧ base + b < h := by
  rw [Bool.and_eq_true] at hc
  rcases hc with ⟨e1, e2⟩
  exact ⟨of_decide_eq_true e1, of_decide_eq_true e2⟩

private theorem partA (l h a b base : Int) (hlt : l < h) (hle : a ≤ b) :
    ∀ s, (∀ d, s = .delta d → a ≤ d ∧ d ≤ b) →
      (probe l h a b base s).1 = (plain base s).any (inside l h) := by
  intro s hs
  cases s with
  | missing => rfl
  | absolute v => rfl
  | delta d =>
    obtain ⟨had, hbd⟩ := hs d rfl
    rw [hsplit]
    by_cases hc1 : (decide (base + b < l) || decide (h ≤ base + a)) = true
    · rw [if_pos hc1]
      show false = inside l h (base + d)
      cases g1T l h a b base hc1 with
      | inl hb => rw [insF l h (base + d) (by omega)]
      | inr ha => rw [insF' l h (base + d) (by omega)]
    · by_cases hc2 : (decide (l ≤ base + a) && decide (base + b < h)) = true
      · rw [if_neg hc1, if_pos hc2]
        show true = inside l h (base + d)
        obtain ⟨e1, e2⟩ := g2T l h a b base hc2
        rw [insT l h (base + d) (by omega) (by omega)]
      · rw [if_neg hc1, if_neg hc2]
        rfl

private theorem partB (l h a b base : Int) (hlt : l < h) (hle : a ≤ b) :
    ((probe l h a b base (.delta a)).2 = 0 ↔
      ∀ d, a ≤ d → d ≤ b → inside l h (base+d) = inside l h (base+a)) := by
  constructor
  · intro hc d dad hdb
    rw [hsplit] at hc
    by_cases hc1 : (decide (base + b < l) || decide (h ≤ base + a)) = true
    · show inside l h (base + d) = inside l h (base + a)
      cases g1T l h a b base hc1 with
      | inl hb => rw [insF l h (base + d) (by omega), insF l h (base + a) (by omega)]
      | inr ha => rw [insF' l h (base + d) (by omega), insF' l h (base + a) (by omega)]
    · by_cases hc2 : (decide (l ≤ base + a) && decide (base + b < h)) = true
      · show inside l h (base + d) = inside l h (base + a)
        obtain ⟨e1, e2⟩ := g2T l h a b base hc2
        rw [insT l h (base + d) (by omega) (by omega),
          insT l h (base + a) e1 (by omega)]
      · rw [if_neg hc1, if_neg hc2] at hc
        exact absurd hc (Nat.succ_ne_zero 0)
  · intro hcon
    rw [hsplit]
    by_cases hc1 : (decide (base + b < l) || decide (h ≤ base + a)) = true
    · rw [if_pos hc1]
    · by_cases hc2 : (decide (l ≤ base + a) && decide (base + b < h)) = true
      · rw [if_neg hc1, if_pos hc2]
      · rw [if_neg hc1, if_neg hc2]
        have g1f := g1N l h a b base hc1
        by_cases hl : l ≤ base + a
        · -- inside (base+a) = true ; inside (base+b) = false
          have ha2 : base + a < h := by
            exact by omega
          have hA : inside l h (base + a) = true := insT l h (base + a) hl ha2
          obtain e1 | e2 := g2N l h a b base hc2
          · exact absurd hl e1
          · have hB : inside l h (base + b) = false := insF' l h (base + b) e2
            have hcon2 := hcon b hle (by omega)
            rw [hB, hA] at hcon2
            exact absurd hcon2 (by decide)
        · -- base+a < l
          have hfa : inside l h (base + a) = false := insF l h (base + a) hl
          have hT : inside l h (base + max a (l - base)) = true :=
            insT l h (base + max a (l - base)) (by omega) (by omega)
          have hcon2 := hcon (max a (l - base)) (by omega) (by omega)
          rw [hT, hfa] at hcon2
          exact absurd hcon2 (by decide)

theorem sound : Goal :=
  fun l h a b base hlt hle => ⟨partA l h a b base hlt hle, partB l h a b base hlt hle⟩

end TinylambOpt.ResearchProofs.ClickBenchLayout.Goal_CB_ClockRange
