import TinylambOpt.ResearchGoals.JitKernels.DecimalChunks
set_option linter.unusedSimpArgs false

/-! Every decimal prefix has magnitude at most the full magnitude: the fold only
adds nonnegative contributions. If the full word fits a width, every earlier
prefix fits too, so delayed checks on prefixes can be omitted. -/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.Goal_DecimalChunks_NarrowDecimalPrefixCertificate

open TinylambOpt.ResearchGoals.JitKernels.DecimalChunks

theorem stepLe (n c : Nat) : n ≤ 10 * n + (c - 48) := by
  have h1 : n ≤ n * 10 := Nat.le_mul_of_pos_right n (by decide : 0 < 10)
  rw [Nat.mul_comm n 10] at h1
  exact Nat.le_trans h1 (Nat.le_add_right (10 * n) (c - 48))

theorem foldlGeInit (n : Nat) (xs : List Nat) :
    n ≤ List.foldl (fun n c => 10 * n + (c - 48)) n xs := by
  induction xs generalizing n with
  | nil => simp
  | cons c xs ih =>
      exact Nat.le_trans (stepLe n c) (ih (n := 10 * n + (c - 48)))

theorem foldlTakeLe (n : Nat) (xs : List Nat) : ∀ k,
    List.foldl (fun n c => 10 * n + (c - 48)) n (xs.take k) ≤
    List.foldl (fun n c => 10 * n + (c - 48)) n xs := by
  induction xs generalizing n with
  | nil => intro k; simp
  | cons c xs ih =>
      intro k
      cases k with
      | zero => exact Nat.le_trans (stepLe n c) (foldlGeInit (10 * n + (c - 48)) xs)
      | succ k =>
          simp only [List.take]
          exact ih (n := 10 * n + (c - 48)) k

theorem sound : NarrowDecimalPrefixCertificate.Goal := by
  intro s w n hd hmag
  have hle : magnitude (s.take n) ≤ magnitude s := by
    simpa [magnitude] using foldlTakeLe 0 s n
  omega

end TinylambOpt.ResearchProofs.JitKernels.Goal_DecimalChunks_NarrowDecimalPrefixCertificate
