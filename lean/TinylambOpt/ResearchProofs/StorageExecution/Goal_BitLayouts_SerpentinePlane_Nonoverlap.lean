import TinylambOpt.ResearchGoals.StorageExecution.BitLayouts

/-!
SQL column readers can work with packed bits when lookup and filtering preserve the
represented values and row positions.

Proved

- `SerpentinePlane.Nonoverlap` — A writer may fill distinct bit cells concurrently.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_SerpentinePlane_Nonoverlap

open TinylambOpt.ResearchGoals.StorageExecution.BitLayouts
open TinylambOpt.ResearchGoals.StorageExecution.BitLayouts.SerpentinePlane

private def tpos (n : Nat) (c r : Nat) : Nat := if c % 2 = 0 then r else n - 1 - r

theorem sound_Nonoverlap : SerpentinePlane.Nonoverlap.Goal := by
  intro n w i j a b hi hj ha hb
  constructor
  · intro h
    have hn : 0 < n := by omega
    have ht : ∀ c r, r < n → tpos n c r < n := by
      intro c r hr
      dsimp [tpos]
      split
      · exact hr
      · omega
    have hai : tpos n a i < n := ht a i hi
    have hbj : tpos n b j < n := ht b j hj
    have hab : a = b := by
      by_cases heq : a = b
      · exact heq
      · by_cases hlt_ab : a < b
        · have hlt1 : a * n + tpos n a i < a * n + n := Nat.add_lt_add_left hai (a * n)
          have hs : a + 1 ≤ b := by omega
          have hmul : (a + 1) * n ≤ b * n := Nat.mul_le_mul_right n hs
          have hmul' : a * n + n ≤ b * n := by simpa [Nat.succ_mul] using hmul
          have hle : b * n ≤ b * n + tpos n b j := by omega
          have hlt : a * n + tpos n a i < b * n + tpos n b j :=
            Nat.lt_of_lt_of_le (Nat.lt_of_lt_of_le hlt1 hmul') hle
          have hh : a * n + tpos n a i = b * n + tpos n b j := by
            simpa [SerpentinePlane.layout, tpos] using h
          rw [hh] at hlt
          exact (Nat.lt_irrefl _ hlt).elim
        · have hlt_ba : b < a := by omega
          have hlt1 : b * n + tpos n b j < b * n + n := Nat.add_lt_add_left hbj (b * n)
          have hs : b + 1 ≤ a := by omega
          have hmul : (b + 1) * n ≤ a * n := Nat.mul_le_mul_right n hs
          have hmul' : b * n + n ≤ a * n := by simpa [Nat.succ_mul] using hmul
          have hle : a * n ≤ a * n + tpos n a i := by omega
          have hlt : b * n + tpos n b j < a * n + tpos n a i :=
            Nat.lt_of_lt_of_le (Nat.lt_of_lt_of_le hlt1 hmul') hle
          have hh : b * n + tpos n b j = a * n + tpos n a i := by
            simpa [SerpentinePlane.layout, tpos] using h.symm
          rw [hh] at hlt
          exact (Nat.lt_irrefl _ hlt).elim
    subst b
    have htij : tpos n a i = tpos n a j := by
      have hh : a * n + tpos n a i = a * n + tpos n a j := by
        simpa [SerpentinePlane.layout, tpos] using h
      omega
    have hij : i = j := by
      dsimp [tpos] at htij
      by_cases hpar : a % 2 = 0
      · simpa [hpar] using htij
      · simp [hpar] at htij
        omega
    exact ⟨hij, rfl⟩
  · rintro ⟨rfl, rfl⟩
    rfl

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BitLayouts_SerpentinePlane_Nonoverlap
