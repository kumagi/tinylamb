import TinylambOpt.ResearchGoals.StorageExecution.RankedMasks

/-!
A validity or selection mask plus its prefix rank can map logical row positions to
packed values for SQL scans.

Proved

- `FixedPopcount.ComplementSymmetry` — `∀ n k, k≤n → codeBits model n k = codeBits
  model n (n-k)`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_RankedMasks_FixedPopcount_ComplementSymmetry

open TinylambOpt.ResearchGoals.StorageExecution.RankedMasks
open TinylambOpt.ResearchGoals.StorageExecution.RankedMasks.FixedPopcount
open TinylambOpt.ResearchGoals.Atlas.SynopsisBounds.Samples

/-! The count of accepted bitmaps with exactly `k` hits is the binomial
coefficient `choose n k`. The latter is symmetric under `k ↦ n-k`, so the
number of code bits is symmetric as well. -/

private theorem choose_eq_zero_of_lt (n k : Nat) (h : n < k) : choose n k = 0 := by
  cases n with
  | zero =>
      cases k with
      | zero => omega
      | succ k => rfl
  | succ n =>
      cases k with
      | zero => omega
      | succ k =>
          rw [choose, choose_eq_zero_of_lt n (k + 1) (by omega)]
          simp [choose_eq_zero_of_lt n k (by omega)]

private theorem choose_diag (n : Nat) : choose n n = 1 := by
  induction n with
  | zero => rfl
  | succ n ih =>
      rw [choose, ih, choose_eq_zero_of_lt n (n + 1) (by omega)]

private theorem choose_symm (n k : Nat) (h : k ≤ n) : choose n k = choose n (n - k) := by
  induction n generalizing k with
  | zero =>
      have hk : k = 0 := by omega
      subst k
      simp [choose]
  | succ n ih =>
      cases k with
      | zero =>
          simp [choose]
          rw [choose_diag, choose_eq_zero_of_lt n (n + 1) (by omega)]
      | succ k =>
          by_cases hkeq : k = n
          · subst k
            simp [choose, choose_diag, choose_eq_zero_of_lt]
          · have hkn : k < n := by omega
            have h1 := ih k (by omega)
            have h2 := ih (k + 1) (by omega)
            have harg : n + 1 - (k + 1) = n - k := by omega
            rw [choose]
            rw [harg]
            rw [h1, h2]
            cases hdk : n - k with
            | zero => omega
            | succ m =>
                have hm1 : n - (k + 1) = m := by omega
                rw [choose, hm1]
                omega

private theorem step_false (k : Nat) : model.step k false = some k := by
  simp [model]

private theorem step_true_succ (k : Nat) : model.step (k + 1) true = some k := by
  simp [model]

private theorem step_true_zero : model.step 0 true = none := by
  simp [model]

private theorem count_eq_choose : ∀ n k, count model n k = choose n k
  | 0, k => by cases k <;> simp [count, model, choose]
  | n + 1, 0 => by
      simp [count, step_false, step_true_zero]
      rw [count_eq_choose n 0]
      simp [choose]
  | n + 1, k + 1 => by
      simp [count, step_false, step_true_succ]
      rw [count_eq_choose n k, count_eq_choose n (k + 1), choose]
      omega

theorem sound_ComplementSymmetry : FixedPopcount.ComplementSymmetry.Goal := by
  intro n k hk
  simp only [codeBits]
  rw [count_eq_choose n k, count_eq_choose n (n - k), choose_symm n k hk]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_RankedMasks_FixedPopcount_ComplementSymmetry
