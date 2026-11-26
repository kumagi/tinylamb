import TinylambOpt.ResearchGoals.StorageExecution.DerivedZones

/-!
Scan pruning can exploit derived bounds only when every matching row remains covered by
the block summary.

Proved

- `SaturatingLoss.SkipRange` — Drop a whole page for a range test, preserving empty and
  NULL pages.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_SaturatingLoss_SkipRange

open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones
open TinylambOpt.ResearchGoals.StorageExecution.DerivedZones.SaturatingLoss

/-- Saturating subtraction is antitone in the subtrahend and monotone in the
minuend, so a score from the rectangle is at most the certified upper bound. -/
private theorem score_le_hi {t x y : Nat} {a b : Bounds}
    (hx : x ≤ a.hi) (hy : b.lo ≤ y) :
    kernel.score t (x, y) ≤ (kernel.bounds t a b).hi := by
  dsimp [kernel]
  exact Nat.le_trans (Nat.sub_le_sub_left (Nat.add_le_add_right hy t) x)
    (Nat.sub_le_sub_right hx (b.lo + t))

/-- The symmetric monotonicity gives the certified lower bound. -/
private theorem lo_le_score {t x y : Nat} {a b : Bounds}
    (hx : a.lo ≤ x) (hy : y ≤ b.hi) :
    (kernel.bounds t a b).lo ≤ kernel.score t (x, y) := by
  dsimp [kernel]
  exact Nat.le_trans (Nat.sub_le_sub_right hx (b.hi + t))
    (Nat.sub_le_sub_left (Nat.add_le_add_right hy t) x)

theorem sound_SkipRange : SaturatingLoss.SkipRange.Goal := by
  intro t p a b lo hi hcov hall
  unfold selected
  apply List.filter_eq_nil_iff.mpr
  intro v hv
  unfold scores at hv
  rw [List.mem_filterMap] at hv
  rcases hv with ⟨r, hrp, hr⟩
  cases r with
  | none => simp at hr
  | some xy =>
      rcases xy with ⟨x, y⟩
      simp only [Option.map_some] at hr
      have hvscore : v = kernel.score t (x, y) := by simpa [kernel] using hr.symm
      have hc := hcov (some (x, y)) hrp x y rfl
      simp only [inside] at hc
      rcases hc with ⟨⟨haxl, haxh⟩, ⟨hbyl, hbyh⟩⟩
      have hall' : lo ≤ (kernel.bounds t a b).hi →
          (kernel.bounds t a b).lo < hi → hi ≤ lo := by
        simpa [allows] using hall
      subst v
      intro hpass
      simp only [Bool.and_eq_true, decide_eq_true_eq] at hpass
      have hhi : lo ≤ (kernel.bounds t a b).hi :=
        Nat.le_trans hpass.1 (score_le_hi haxh hbyl)
      have hlo : (kernel.bounds t a b).lo < hi :=
        Nat.lt_of_le_of_lt (lo_le_score haxl hbyh) hpass.2
      exact (Nat.not_lt_of_ge (hall' hhi hlo)) (Nat.lt_of_le_of_lt hpass.1 hpass.2)

end TinylambOpt.ResearchProofs.StorageExecution.Goal_DerivedZones_SaturatingLoss_SkipRange
