import TinylambOpt.ResearchGoals.ClickstreamLayout.Execution

/-!
These models describe encoded clickstream columns and the predicates and aggregates that
can consume them.

Proved

- `SparseMark` — A two-column rectangular predicate over a lex-clustered part may skip
  a granule exactly when no possible key fits its marks and both SQL ranges.
-/
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.ClickstreamLayout.Exec_SparseMark

open TinylambOpt.ResearchGoals.ClickstreamLayout.Execution
open SparseMark

private theorem le_trans (a b c : Key) (h1 : le a b) (h2 : le b c) : le a c := by
  rcases a with ⟨a1, a2⟩
  rcases b with ⟨b1, b2⟩
  rcases c with ⟨c1, c2⟩
  unfold SparseMark.le at h1 h2 ⊢
  rcases h1 with x | ⟨x1, x2⟩
  · rcases h2 with y | ⟨y1, y2⟩
    · exact .inl (by omega)
    · exact .inl (by omega)
  · rcases h2 with y | ⟨y1, y2⟩
    · exact .inl (by omega)
    · exact .inr ⟨by omega, by omega⟩

private theorem least_low (first : Key) (xl yl yh : Nat) (e : first.1 < xl) :
    least first xl yl yh = (xl, yl) := by
  unfold SparseMark.least
  rw [if_pos e]

private theorem least_mid (first : Key) (xl yl yh : Nat)
    (e1 : ¬ (first.1 < xl)) (e2 : first.2 ≤ yh) :
    least first xl yl yh = (first.1, max yl first.2) := by
  unfold SparseMark.least
  rw [if_neg e1, if_pos e2]

private theorem least_high (first : Key) (xl yl yh : Nat)
    (e1 : ¬ (first.1 < xl)) (e2 : ¬ (first.2 ≤ yh)) :
    least first xl yl yh = (first.1 + 1, yl) := by
  unfold SparseMark.least
  rw [if_neg e1, if_neg e2]

theorem sound : Goal := by
  intro first last xl xh yl yh hxl hyh
  constructor
  · intro h
    obtain ⟨hc1, hc2⟩ := h
    rcases Decidable.em (first.1 < xl) with e1 | e1
    · rw [least_low first xl yl yh e1] at hc1 hc2
      refine ⟨(xl, yl), ?_, hc2, by omega, by simpa using hc1, by omega, by omega⟩
      unfold SparseMark.le
      exact .inl e1
    · rcases Decidable.em (first.2 ≤ yh) with e2 | e2
      · rw [least_mid first xl yl yh e1 e2] at hc1 hc2
        refine ⟨(first.1, max yl first.2), ?_, hc2, by
          exact Nat.le_of_not_lt e1, by simpa using hc1, by omega, by omega⟩
        unfold SparseMark.le
        exact .inr ⟨rfl, by omega⟩
      · rw [least_high first xl yl yh e1 e2] at hc1 hc2
        refine ⟨(first.1 + 1, yl), ?_, hc2, by omega, by simpa using hc1,
          by omega, by omega⟩
        unfold SparseMark.le
        exact .inl (by omega)
  · intro h
    obtain ⟨p, hp0, hp1, hp2, hp3, hp4, hp5⟩ := h
    rcases p with ⟨p1, p2⟩
    rcases Decidable.em (first.1 < xl) with e1 | e1
    · rw [least_low first xl yl yh e1]
      have hle : le (xl, yl) (p1, p2) := by
        unfold SparseMark.le
        rcases Nat.lt_or_ge xl p1 with lt | ge
        · exact .inl lt
        · exact .inr (by omega)
      exact ⟨by omega, le_trans (xl, yl) (p1, p2) last hle hp1⟩
    · rcases Decidable.em (first.2 ≤ yh) with e2 | e2
      · rw [least_mid first xl yl yh e1 e2]
        have hle : le (first.1, max yl first.2) (p1, p2) := by
          unfold SparseMark.le at hp0 ⊢
          rcases hp0 with x | ⟨x1, x2⟩
          · exact .inl (by omega)
          · exact .inr (by omega)
        refine ⟨by
          unfold SparseMark.le at hp0
          rcases hp0 with x | ⟨x1, x2⟩
          · omega
          · omega,
          le_trans (first.1, max yl first.2) (p1, p2) last hle hp1⟩
      · rw [least_high first xl yl yh e1 e2]
        have hf1 : first.1 < p1 := by
          unfold SparseMark.le at hp0
          rcases hp0 with x | ⟨x1, x2⟩
          · exact x
          · omega
        have hle : le (first.1 + 1, yl) (p1, p2) := by
          unfold SparseMark.le
          rcases Nat.lt_or_ge (first.1 + 1) p1 with lt | ge
          · exact .inl lt
          · exact .inr (by omega)
        exact ⟨by omega, le_trans (first.1 + 1, yl) (p1, p2) last hle hp1⟩

end TinylambOpt.ResearchProofs.ClickstreamLayout.Exec_SparseMark
