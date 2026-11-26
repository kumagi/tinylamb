import TinylambOpt.ResearchGoals.Atlas.ExecutorResources

/-!
SQL operators may batch or defer work under the modeled memory and resource ownership
conditions.

Proved

- `Pipeline.SteadyState` — `∀ n a b, 0 < n → finish (List.replicate n (a,b)) =
  a+b+(n-1)*max a b`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.Atlas.Goal_Pipeline_SteadyState

open TinylambOpt.ResearchGoals.Atlas.ExecutorResources
open TinylambOpt.ResearchGoals.Atlas.ExecutorResources.Pipeline Pipeline

-- 小さい入力 (0 ≤ n,a,b < 5) の総当たりでは n > 0 のとき常に成り立つ。
-- n = 0 だけは左辺 0 に対して右辺 a+b となり例外だが、ゴールは 0 < n を仮定する。

/-- 任意の初期状態 (s,t) から同じジョブを k 個流したときの第 2 段の完了時刻。 -/
private theorem fold_replicate_snd (a b s t k : Nat) (kpos : 0 < k) :
    ((List.replicate k (a, b)).foldl step (s, t)).2 =
      max (t + k * b) (s + a + (k - 1) * max a b + b) := by
  induction k generalizing s t with
  | zero => omega
  | succ n ih =>
      cases n with
      | zero =>
          simp [List.replicate, step, Nat.max_comm]
      | succ m =>
          rw [List.replicate_succ, List.foldl_cons]
          dsimp only [step]
          have hih := ih (s + a) (max (s + a) t + b) (by omega : 0 < Nat.succ m)
          rw [hih]
          clear hih ih kpos
          simp only [Nat.add_sub_cancel_right, Nat.succ_mul] at *
          by_cases hab : a ≤ b
          · have hmM : m * a ≤ m * b := Nat.mul_le_mul_left m hab
            have hmax : max a b = b := Nat.max_eq_right hab
            simp [hmax]
            by_cases hst : s + a ≤ t
            · have hm : max (s + a) t = t := Nat.max_eq_right hst
              simp [hm]
              clear hm hmax
              simp only [Nat.max_def]
              split <;> (try split) <;> (try split) <;> (all_goals omega)
            · have hm : max (s + a) t = s + a := Nat.max_eq_left (by omega)
              simp [hm]
              clear hm hmax
              simp only [Nat.max_def]
              split <;> (try split) <;> (try split) <;> (all_goals omega)
          · have hba : b ≤ a := by omega
            have hmM : m * b ≤ m * a := Nat.mul_le_mul_left m hba
            have hmax : max a b = a := Nat.max_eq_left hba
            simp [hmax]
            by_cases hst : s + a ≤ t
            · have hm : max (s + a) t = t := Nat.max_eq_right hst
              simp [hm]
              clear hm hmax
              simp only [Nat.max_def]
              split <;> (try split) <;> (try split) <;> (all_goals omega)
            · have hm : max (s + a) t = s + a := Nat.max_eq_left (by omega)
              simp [hm]
              clear hm hmax
              simp only [Nat.max_def]
              split <;> (try split) <;> (try split) <;> (all_goals omega)

theorem sound_Pipeline_SteadyState : SteadyState.Goal := by
  intro n a b hn
  have h := fold_replicate_snd a b 0 0 n hn
  have hbM : b ≤ max a b := Nat.le_max_right a b
  have hnb : n * b ≤ a + b + (n - 1) * max a b := by
    have hn' : n = (n - 1) + 1 := by omega
    rw [hn']
    simp only [Nat.add_mul, Nat.one_mul]
    have hs : n - 1 + 1 - 1 = n - 1 := by omega
    rw [hs]
    have hmul : (n - 1) * b ≤ (n - 1) * max a b := Nat.mul_le_mul_left (n - 1) hbM
    omega
  have hm : max (n * b) (a + (n - 1) * max a b + b) =
      a + b + (n - 1) * max a b := by
    omega
  simpa [finish, Nat.zero_add, hm] using h

end TinylambOpt.ResearchProofs.Atlas.Goal_Pipeline_SteadyState
