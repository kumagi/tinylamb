import TinylambOpt.ResearchGoals.StorageExecution.DerivedZones

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_WrappedSum_SemijoinFilter

open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones
open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.WrappedSum

-- 真偽確認: 小さい入力で、スコアが保証区間内に入ることと
-- セミジョイン絞り込みが等しいことを計算で確認する（いずれも真）。
#eval let r := WrappedSum.kernel.bounds 3 ⟨1, 5⟩ ⟨2, 6⟩
      let v := WrappedSum.kernel.score 3 (5, 6)
      (r.lo ≤ v && v ≤ r.hi, r, v)
#eval let ks := [0, 2, 4, 7, 9]
      let p : List (Option Row) := [some (1, 2), none, some (5, 6), some (0, 0)]
      let r := WrappedSum.kernel.bounds 3 ⟨0, 5⟩ ⟨0, 6⟩
      let s := scores WrappedSum.kernel 3 p
      (decide (s.filter ks.contains = s.filter ((ks.filter (fun k => r.lo ≤ k && k ≤ r.hi)).contains)), s, r)

private theorem mod_le_mod_of_div_eq {d s₁ s₂ : Nat} (hd : 0 < d)
    (hdiv : s₁ / d = s₂ / d) (hle : s₁ ≤ s₂) : s₁ % d ≤ s₂ % d := by
  have h1 := (Nat.div_mod_unique hd).1
    (And.intro rfl rfl : s₁ / d = s₁ / d ∧ s₁ % d = s₁ % d)
  have h2 := (Nat.div_mod_unique hd).1
    (And.intro hdiv.symm rfl : s₂ / d = s₁ / d ∧ s₂ % d = s₂ % d)
  omega

-- WrappedSum の soundInterval（既存証明を再利用）。
private theorem sound_SoundInterval : WrappedSum.SoundInterval.Goal := by
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

/-- ページ内の全スコアが保証区間内にある。 -/
private theorem score_bounds {f : Kernel} (hf : soundInterval f) {t p a b} (hc : covers p a b) :
    ∀ v ∈ scores f t p, inside (f.bounds t a b) v := by
  intro v hv
  obtain ⟨r, hr, hm⟩ : ∃ r ∈ p, r.map (f.score t) = some v := by
    simpa [scores] using hv
  obtain ⟨x, y, hr2, hscore⟩ : ∃ x y, some (x, y) ∈ p ∧ f.score t (x, y) = v := by
    cases r with
    | none => cases hm
    | some xy =>
      cases xy with
      | mk x y => exact ⟨x, y, hr, by simpa using hm⟩
  obtain ⟨hax, hby⟩ := hc (some (x, y)) hr2 x y rfl
  rw [← hscore]
  exact hf t a b x y hax hby

theorem sound_SemijoinFilter : WrappedSum.SemijoinFilter.Goal := by
  intro t p a b ks hc
  dsimp only
  apply List.filter_congr
  intro v hv
  have hb := score_bounds (f := WrappedSum.kernel) sound_SoundInterval hc v hv
  by_cases hmem : v ∈ ks
  · simp [List.contains_eq_mem, hmem, hb.1, hb.2]
  · have hnot : v ∉ ks.filter (fun k => (WrappedSum.kernel.bounds t a b).lo ≤ k &&
        k ≤ (WrappedSum.kernel.bounds t a b).hi) := by
      simp [List.mem_filter, hmem]
    simp [List.contains_eq_mem, hmem, hnot]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_WrappedSum_SemijoinFilter
