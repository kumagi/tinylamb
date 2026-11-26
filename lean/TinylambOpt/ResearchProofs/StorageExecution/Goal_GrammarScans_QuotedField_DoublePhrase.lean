import TinylambOpt.ResearchGoals.StorageExecution.GrammarScans

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_GrammarScans_QuotedField_DoublePhrase

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.GrammarScans
open TinylambOpt.ResearchGoals.StorageExecution.GrammarScans.QuotedField

/-! Small-input truth check: literals (with and without quotes), concatenations,
    and repetitions of both parities. -/
private def samples : List Rope :=
  [ .lit [], .lit [34], .lit [65, 66], .lit [34, 65, 34],
    .cat (.lit [34]) (.lit [65]), .cat (.lit [34, 34]) (.lit [34]),
    .rep 0 (.lit [34]), .rep 1 (.lit [34]), .rep 3 (.lit [65]),
    .cat (.rep 2 (.lit [34])) (.lit [34]) ]

private def check : Bool :=
  samples.all fun x => compile reader (.rep 2 x) == List.range reader.states

#eval check

example : check := by native_decide

/-! Every byte step either preserves the incoming state or toggles 0 <-> 1,
    so a state below 2 stays below 2. -/
private theorem step_lt (s : Bytes) (q : Nat) (hq : q < 2) :
    s.foldl reader.step q < 2 := by
  induction s generalizing q with
  | nil => simpa using hq
  | cons b bs ih =>
      rw [List.foldl_cons]
      apply ih
      change (if b == 34 then 1 - q else q) < 2
      by_cases hb : b == 34
      · simp [hb]
        omega
      · simpa [hb] using hq

private theorem step_step_ne (b q r : Nat) (hq : q < 2) (hr : r < 2) (hqr : q ≠ r) :
    reader.step q b ≠ reader.step r b := by
  change (if b == 34 then 1 - q else q) ≠ (if b == 34 then 1 - r else r)
  by_cases hb : b == 34
  · simp [hb]
    omega
  · simpa [hb] using hqr

/-! The byte-step map is injective on states {0, 1}. -/
private theorem step_fold_inj (s : Bytes) (q r : Nat) (hq : q < 2) (hr : r < 2)
    (hqr : q ≠ r) : s.foldl reader.step q ≠ s.foldl reader.step r := by
  induction s generalizing q r with
  | nil => exact hqr
  | cons b bs ih =>
      rw [List.foldl_cons, List.foldl_cons]
      exact ih _ _ (by simpa using (step_lt [b] q hq)) (by simpa using (step_lt [b] r hr))
        (step_step_ne b q r hq hr hqr)

/-! The only compiled tables are the identity [0,1] and the swap [1,0]. -/
private theorem power_id : ∀ n : Nat, power n [0, 1] = [0, 1] := by
  intro n
  induction n using Nat.strongRecOn with
  | ind n ih =>
      unfold power
      split
      · decide
      · rename_i hn
        have hlt : n / 2 < n := Nat.div_lt_self (by omega) (by decide)
        have hh := ih (n / 2) hlt
        simp [hh, chain, applyT]

private theorem power_swap : ∀ n : Nat,
    power n [1, 0] = if n % 2 == 0 then [0, 1] else [1, 0] := by
  intro n
  induction n using Nat.strongRecOn with
  | ind n ih =>
      unfold power
      split
      · rename_i hn
        subst n
        decide
      · rename_i hn
        have hlt : n / 2 < n := Nat.div_lt_self (by omega) (by decide)
        have hh := ih (n / 2) hlt
        have hsq : chain (power (n / 2) [1, 0]) (power (n / 2) [1, 0]) = [0, 1] := by
          rw [hh]
          by_cases he : (n / 2) % 2 == 0 <;> simp [he, chain, applyT]
        by_cases heven : n % 2 == 0
        · simp [heven, hsq]
        · rw [hh]
          by_cases hehalf : (n / 2) % 2 == 0 <;>
            simp [heven, hehalf, chain, applyT]

private theorem compile_table (x : Rope) :
    compile reader x = [0, 1] ∨ compile reader x = [1, 0] := by
  induction x with
  | lit s =>
      change (List.range 2).map (fun q => s.foldl reader.step q) = [0, 1] ∨
        (List.range 2).map (fun q => s.foldl reader.step q) = [1, 0]
      have h0lt := step_lt s 0 (by decide)
      have h1lt := step_lt s 1 (by decide)
      have hneq : s.foldl reader.step 0 ≠ s.foldl reader.step 1 :=
        step_fold_inj s 0 1 (by decide) (by decide) (by decide)
      have h0c : s.foldl reader.step 0 = 0 ∨ s.foldl reader.step 0 = 1 := by omega
      have h1c : s.foldl reader.step 1 = 0 ∨ s.foldl reader.step 1 = 1 := by omega
      rw [show List.range 2 = [0, 1] by decide]
      simp only [List.map_cons, List.map_nil]
      rcases h0c with h0c | h0c <;> rcases h1c with h1c | h1c
      · exact False.elim (hneq (by omega))
      · left
        simp [h0c, h1c]
      · right
        simp [h0c, h1c]
      · exact False.elim (hneq (by omega))
  | cat a b iha ihb =>
      rw [compile]
      rcases iha with ha | ha <;> rcases ihb with hb | hb <;>
        simp [ha, hb, chain, applyT]
  | rep k a ih =>
      rw [compile]
      rcases ih with ha | ha
      · left
        simpa [ha] using (power_id k)
      · rw [ha, power_swap]
        by_cases hk : k % 2 == 0
        · left
          simp [hk]
        · right
          simp [hk]

theorem sound_DoublePhrase : QuotedField.DoublePhrase.Goal := by
  intro x
  change power 2 (compile reader x) = [0, 1]
  rcases compile_table x with hx | hx
  · rw [hx]
    exact power_id 2
  · rw [hx]
    simpa using (power_swap 2)

end TinylambOpt.ResearchProofs.StorageExecution.Goal_GrammarScans_QuotedField_DoublePhrase
