import TinylambOpt.ResearchGoals.StorageExecution.BitLayouts

/-!
SQL column readers can work with packed bits when lookup and filtering preserve the
represented values and row positions.

Proved

- `SerpentinePlane.SharedPlaneEdge` — `∀ n w b, 0<n → b+1<w → b%2=0 → (layout.bit n w
  (n-1) b/8 = layout.bit n w (n-1) (b+1)/8 ↔ ((b+1)*n)%8≠0)`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_SerpentinePlane_SharedPlaneEdge

open TinylambOpt.ResearchGoals.StorageExecution.BitLayouts
open TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.SerpentinePlane

/-- The last row of an even bit plane sits at `b*n + (n-1)`; the first row
    of the next (odd) plane sits at `(b+1)*n`.  They are `(m-1, m)` for
    `m = (b+1)*n`, which share a byte iff `m` is not byte aligned. -/
theorem sound_SharedPlaneEdge : SerpentinePlane.SharedPlaneEdge.Goal := by
  intro n w b hn hb hbe
  simp only [layout]
  have hodd : (b + 1) % 2 ≠ 0 := by omega
  simp only [hbe, hodd, ↓reduceIte, Nat.sub_self]
  have hsucc : (b + 1) * n = b * n + n := by rw [Nat.succ_mul]
  have hprev : b * n + (n - 1) = (b + 1) * n - 1 := by omega
  rw [hprev]
  have hmpos : 0 < (b + 1) * n := Nat.mul_pos (by omega) hn
  constructor <;> intro h <;> omega

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_SerpentinePlane_SharedPlaneEdge
