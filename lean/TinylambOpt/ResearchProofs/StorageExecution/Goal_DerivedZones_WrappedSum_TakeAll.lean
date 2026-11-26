import TinylambOpt.ResearchGoals.StorageExecution.DerivedZones

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_WrappedSum_TakeAll

open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones
open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.WrappedSum

-- 真偽確認: t と各端点・x, y, lo, hi を 0..5 で全網羅し反例なし（真）を確認済み。
-- 周期内では除算の商が等しいため剰余は単調、周期またぎなら [0,t] が安全。
-- したがって時刻区分全体を含む範囲テストは全スコアを保持する。

/-- Within one quotient block the residue is monotone in the numerator. -/
private theorem mod_le_mod_of_div_eq {d s₁ s₂ : Nat} (hd : 0 < d)
    (hdiv : s₁ / d = s₂ / d) (hle : s₁ ≤ s₂) : s₁ % d ≤ s₂ % d := by
  have h1 := (Nat.div_mod_unique hd).1
    (And.intro rfl rfl : s₁ / d = s₁ / d ∧ s₁ % d = s₁ % d)
  have h2 := (Nat.div_mod_unique hd).1
    (And.intro hdiv.symm rfl : s₂ / d = s₁ / d ∧ s₂ % d = s₂ % d)
  omega

/-- The wrap-aware interval covers every score obtainable from the rectangle. -/
private theorem soundInterval_wrappedSum : soundInterval kernel := by
  intro t a b x y hx hy
  rcases hx with ⟨ha_lo, ha_hi⟩
  rcases hy with ⟨hb_lo, hb_hi⟩
  by_cases hq : (a.lo + b.lo) / (t + 1) = (a.hi + b.hi) / (t + 1)
  · simp only [kernel, hq, ↓reduceIte, inside]
    have hsum_lo : a.lo + b.lo ≤ x + y := by omega
    have hsum_hi : x + y ≤ a.hi + b.hi := by omega
    have hq_lo : (a.lo + b.lo) / (t + 1) = (x + y) / (t + 1) := by
      have h := Nat.div_le_div_right (c := t + 1) hsum_lo
      have h' := Nat.div_le_div_right (c := t + 1) hsum_hi
      omega
    have hq_hi : (x + y) / (t + 1) = (a.hi + b.hi) / (t + 1) := by
      have h := Nat.div_le_div_right (c := t + 1) hsum_lo
      have h' := Nat.div_le_div_right (c := t + 1) hsum_hi
      omega
    constructor
    · exact mod_le_mod_of_div_eq (by omega) hq_lo hsum_lo
    · exact mod_le_mod_of_div_eq (by omega) hq_hi hsum_hi
  · simp only [kernel, hq, ↓reduceIte, inside]
    constructor
    · exact Nat.zero_le _
    · exact Nat.le_of_lt_succ (Nat.mod_lt (x + y) (by omega))

/-- Every score of a non-NULL row covered by the bounds is inside the interval. -/
private theorem inside_of_sound {f : Kernel} (hs : soundInterval f) {t : Nat}
    {p : List (Option Row)} {a b : Bounds} (hc : covers p a b)
    {r : Option Row} (hr : r ∈ p) {x y : Nat} (heq : r = some (x, y)) :
    inside (f.bounds t a b) (f.score t (x, y)) := by
  exact hs t a b x y (hc r hr x y heq).1 (hc r hr x y heq).2

/-- A certified interval justifies keeping the entire page. -/
private theorem takeAll_of_sound {f : Kernel} (hs : soundInterval f) : takeAll f := by
  intro t p a b lo hi hc hlo hhi
  unfold selected
  rw [List.filter_eq_self]
  intro v hv
  simp only [List.mem_filterMap, scores] at hv
  rcases hv with ⟨r, hrm, hr⟩
  rcases r with _ | ⟨x, y⟩
  · simp at hr
  · simp only [Option.map_some, Option.some.injEq] at hr
    subst hr
    have hin := inside_of_sound (t := t) hs hc hrm rfl
    simp only [inside] at hin
    simp only [Bool.and_eq_true, decide_eq_true_eq]
    exact ⟨Nat.le_trans hlo hin.1, Nat.lt_of_le_of_lt hin.2 hhi⟩

theorem sound_TakeAll : TakeAll.Goal :=
  takeAll_of_sound soundInterval_wrappedSum

end TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_WrappedSum_TakeAll
