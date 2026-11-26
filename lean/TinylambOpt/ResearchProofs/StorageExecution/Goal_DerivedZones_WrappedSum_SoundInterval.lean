import TinylambOpt.ResearchGoals.StorageExecution.DerivedZones

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_WrappedSum_SoundInterval

open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones
open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.WrappedSum

-- 真偽確認: t と各端点・x, y を 0..5 で全網羅し反例なし（真）を確認済み。
-- 周期内では除算の商が等しいため剰余は単調、周期またぎなら [0,t] が安全。

private theorem mod_le_mod_of_div_eq {d s₁ s₂ : Nat} (hd : 0 < d)
    (hdiv : s₁ / d = s₂ / d) (hle : s₁ ≤ s₂) : s₁ % d ≤ s₂ % d := by
  have h1 := (Nat.div_mod_unique hd).1
    (And.intro rfl rfl : s₁ / d = s₁ / d ∧ s₁ % d = s₁ % d)
  have h2 := (Nat.div_mod_unique hd).1
    (And.intro hdiv.symm rfl : s₂ / d = s₁ / d ∧ s₂ % d = s₂ % d)
  omega

theorem sound_SoundInterval : WrappedSum.SoundInterval.Goal := by
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

end TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_WrappedSum_SoundInterval
