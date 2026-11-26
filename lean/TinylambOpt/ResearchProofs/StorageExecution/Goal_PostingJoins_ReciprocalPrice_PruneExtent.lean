import TinylambOpt.ResearchGoals.StorageExecution.PostingJoins

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_PostingJoins_ReciprocalPrice_PruneExtent

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.PostingJoins
open TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.ReciprocalPrice

/-- If the stored reciprocal interval `[0, min u (t/x+1))` misses the extent,
    then no key of that extent can satisfy `x*y ≤ t`. -/
theorem sound_PruneExtent :
    ReciprocalPrice.PruneExtent.Goal := by
  intro u t x ys lo hi hv hys hspan
  rw [List.filter_eq_nil_iff]
  intro y hy
  have hyu : y < u := hv.2 y hy
  have hlo : lo ≤ y := (hys y hy).1
  have hhi : y < hi := (hys y hy).2
  have hupos : 0 < u := by omega
  have hhipos : 0 < hi := by omega
  cases htest : search.test t x y with
  | false => simp
  | true =>
      have hxy : x * y ≤ t := by simpa [search] using htest
      by_cases hx : x = 0
      · subst x
        have hspans : search.spans u t 0 = [(0, u)] := by
          simp [search, TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.optional, hupos]
        have hp := hspan (0, u) (by simp [hspans])
        rcases hp with hp | hp <;> omega
      · have hxpos : 0 < x := Nat.pos_of_ne_zero hx
        let a := t / x + 1
        let b := min u a
        have ha_pos : 0 < a := by simp [a]
        have hb_pos : 0 < b := by
          dsimp [b]
          exact Nat.lt_min.mpr ⟨hupos, ha_pos⟩
        have hb_pos' : 0 < min u (t / x + 1) := by simpa [b, a] using hb_pos
        have hspans : search.spans u t x = [(0, b)] := by
          simp [search, TinylambOpt.ResearchGoals.StorageExecution.PostingJoins.optional,
            b, a, hx, hb_pos', beq_iff_eq]
        have hp := hspan (0, b) (by simp [hspans])
        have hb_le_lo : b ≤ lo := by
          rcases hp with hp | hp
          · exact hp
          · omega
        have hb_lt_u : b < u := by omega
        have ha_le_u : a ≤ u := by
          by_cases h : a ≤ u
          · exact h
          · have hua : u < a := by omega
            have hb_eq_u : b = u := by
              dsimp [b]
              exact Nat.min_eq_left (by omega : u ≤ a)
            omega
        have hb_eq_a : b = a := by
          dsimp [b]
          exact Nat.min_eq_right ha_le_u
        have hya : a ≤ y := by omega
        have hdecomp : x * (t / x) + t % x = t := Nat.div_add_mod t x
        have hmod : t % x < x := Nat.mod_lt t hxpos
        have hgt : t < x * a := by
          dsimp [a]
          rw [Nat.mul_add, Nat.mul_one]
          omega
        have hle : x * a ≤ x * y := Nat.mul_le_mul_left x hya
        omega

end TinylambOpt.ResearchProofs.StorageExecution.Goal_PostingJoins_ReciprocalPrice_PruneExtent
