import TinylambOpt.ResearchGoals.StorageExecution.GrammarScans

/-!
A compressed string scan uses grammar or chunk summaries to evaluate string predicates
without always reconstructing the full value.

Proved

- `ContainsSlash.RepeatedState` — Exponentiation of the table skips arbitrarily long
  repeated strings.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_GrammarScans_ContainsSlash_RepeatedState

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.GrammarScans
open TinylambOpt.ResearchGoals.StorageExecution.GrammarScans.ContainsSlash

/-! Small-input truth check: literals, concatenation, and repetition, for both
    states and several period counts. -/
private def samples : List Rope :=
  [ .lit [], .lit [47], .lit [65, 66], .lit [65, 47, 66],
    .cat (.lit [65]) (.lit [66, 47]), .cat (.lit [47]) (.lit [65]),
    .rep 0 (.lit [47]), .rep 3 (.lit [65]), .rep 2 (.lit [65, 47]),
    .cat (.rep 2 (.lit [65, 66])) (.lit [47]) ]

private def check : Bool :=
  samples.all fun x => (List.range 6).all fun n => (List.range 2).all fun q =>
    applyT (power n (compile reader x)) q ==
      ((List.replicate n (expand x)).flatten).foldl reader.step q

#eval check

example : check := by native_decide

/-! The reader is "sticky": after a slash the state is 1 forever, otherwise
    the incoming state is preserved. -/
private theorem step_fold (s : Bytes) (q : Nat) :
    s.foldl reader.step q = if 47 ∈ s then 1 else q := by
  induction s generalizing q with
  | nil => simp [reader]
  | cons b bs ih =>
      rw [List.foldl_cons, ih]
      by_cases hb : b = 47
      · subst b
        simp [reader]
      · have hb' : 47 ≠ b := by omega
        by_cases hm : 47 ∈ bs
        · simp [reader, hb, hb', hm]
        · simp [reader, hb, hb', hm]

private theorem mem_flatten_replicate (k : Nat) (s : Bytes) :
    47 ∈ (List.replicate k s).flatten ↔ 0 < k ∧ 47 ∈ s := by
  induction k with
  | zero => simp
  | succ k ih =>
      rw [List.replicate_succ, List.flatten_cons, List.mem_append, ih]
      constructor
      · intro h
        rcases h with h | ⟨_, h⟩
        · exact ⟨by omega, h⟩
        · exact ⟨by omega, h⟩
      · rintro ⟨_, h⟩
        exact Or.inl h

/-! The two transition tables a phrase can compile to. -/
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

private theorem power_const : ∀ n : Nat, power n [1, 1] = if n = 0 then [0, 1] else [1, 1] := by
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
        by_cases hhalf : n / 2 = 0
        · have hn1 : n = 1 := by omega
          subst n
          simp [hh, chain, applyT]
        · have hh' : power (n / 2) [1, 1] = [1, 1] := by simpa [hhalf] using hh
          simp [hh', chain, applyT]

private theorem compile_table (x : Rope) :
    compile reader x = if 47 ∈ expand x then [1, 1] else [0, 1] := by
  induction x with
  | lit s =>
      change (List.range 2).map (fun q => s.foldl reader.step q) =
        if 47 ∈ s then [1, 1] else [0, 1]
      rw [show List.range 2 = [0, 1] by decide]
      by_cases hs : 47 ∈ s <;> simp [step_fold, hs]
  | cat a b iha ihb =>
      rw [compile, iha, ihb, expand]
      by_cases ha : 47 ∈ expand a <;> by_cases hb : 47 ∈ expand b <;>
        simp [ha, hb, chain, applyT]
  | rep k a ih =>
      rw [compile, ih]
      change power k (if 47 ∈ expand a then [1, 1] else [0, 1]) =
        if 47 ∈ (List.replicate k (expand a)).flatten then [1, 1] else [0, 1]
      by_cases hs : 47 ∈ expand a
      · rw [if_pos hs]
        by_cases hk : k = 0
        · subst k
          simpa using (power_const 0)
        · have hmem : 47 ∈ (List.replicate k (expand a)).flatten :=
            (mem_flatten_replicate k (expand a)).2 ⟨Nat.pos_of_ne_zero hk, hs⟩
          rw [if_pos hmem]
          simp [power_const, hk]
      · rw [if_neg hs]
        have hmem : ¬ 47 ∈ (List.replicate k (expand a)).flatten := by
          intro h
          exact hs ((mem_flatten_replicate k (expand a)).1 h).2
        rw [if_neg hmem]
        simp [power_id]

theorem sound_RepeatedState : ContainsSlash.RepeatedState.Goal := by
  intro x n q hq
  have hq' : q < 2 := by simpa [reader] using hq
  have hq01 : q = 0 ∨ q = 1 := by omega
  rcases hq01 with rfl | rfl <;>
    by_cases hs : 47 ∈ expand x <;>
      by_cases hn : n = 0 <;>
        simp [applyT, compile_table, step_fold, power_const, power_id,
          mem_flatten_replicate, hs, hn]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_GrammarScans_ContainsSlash_RepeatedState
