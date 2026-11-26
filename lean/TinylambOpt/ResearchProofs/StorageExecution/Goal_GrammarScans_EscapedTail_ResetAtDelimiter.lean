import TinylambOpt.ResearchGoals.StorageExecution.GrammarScans

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_GrammarScans_EscapedTail_ResetAtDelimiter

open TinylambOpt.ResearchGoals.StorageExecution.GrammarScans
open TinylambOpt.ResearchGoals.StorageExecution.GrammarScans.EscapedTail
open TinylambOpt.ResearchGoals.Compression.Wire

/-! ## Small-input truth checks (the delimiter byte 47 resets, then 92 flips to the accepting state) -/

#eval matched reader (.lit [47, 92]) 0
#eval matched reader (.cat (.lit [1, 2]) (.lit [47, 92])) 0
#eval matched reader (.cat (.lit [92]) (.lit [47, 92])) 0
#eval matched reader (.rep 3 (.lit [92, 47])) 0

/-! ## Generic table / power helpers -/

private theorem chain_length (a b : Table) : (chain a b).length = a.length := by
  simp [chain]

private theorem applyT_chain (a b : Table) (q : Nat) (hq : q < a.length) :
    applyT (chain a b) q = applyT b (applyT a q) := by
  have hq' : q < (chain a b).length := by simpa [chain_length]
  simp [applyT, List.getElem?_eq_getElem hq, chain]

private theorem power_length (n : Nat) (t : Table) : (power n t).length = t.length := by
  induction n using Nat.strongRecOn with
  | _ n ih =>
      unfold power
      by_cases hn : n = 0
      · simp [hn]
      · simp only [hn, dite_false]
        by_cases heven : n % 2 == 0
        · simp [heven, chain_length, ih (n / 2) (by omega)]
        · simp [heven, chain_length, ih (n / 2) (by omega)]

private theorem compile_length (x : Rope) : (compile reader x).length = reader.states := by
  induction x with
  | lit s => simp [compile, reader]
  | cat a b ihA ihB => simp [compile, chain_length, ihA]
  | rep n a ihA =>
      simp only [compile]
      induction n using Nat.strongRecOn with
      | _ n ihN =>
          unfold power
          by_cases hn : n = 0
          · simp [hn, ihA]
          · simp only [hn, dite_false]
            by_cases heven : n % 2 == 0
            · simp [heven, chain_length, ihN (n / 2) (by omega)]
            · simp [heven, chain_length, ihN (n / 2) (by omega)]

private theorem power_zero (t : Table) : power 0 t = List.range t.length := by
  unfold power
  simp

private theorem applyT_power_zero (t : Table) (q : Nat) (hq : q < t.length) :
    applyT (power 0 t) q = q := by
  rw [power_zero]
  simp [applyT, List.getElem?_range hq]

/-! ## foldl composition -/

private theorem foldl_append_step (xs ys : Bytes) (q : Nat) :
    (xs ++ ys).foldl reader.step q = ys.foldl reader.step (xs.foldl reader.step q) := by
  induction xs generalizing q with
  | nil => simp
  | cons x xs ih => simp [ih]

private theorem replicate_succ_eq_append (n : Nat) (xs : Bytes) :
    List.replicate (n + 1) xs = List.replicate n xs ++ [xs] := by
  induction n with
  | zero => simp
  | succ n ih => simpa [List.replicate_succ] using ih

private theorem foldl_flatten_replicate_succ (n : Nat) (xs : Bytes) (q : Nat) :
    ((List.replicate (n + 1) xs).flatten).foldl reader.step q =
      xs.foldl reader.step (((List.replicate n xs).flatten).foldl reader.step q) := by
  rw [replicate_succ_eq_append]
  simp [List.flatten_append]

private theorem foldl_flatten_replicate_add (m n : Nat) (xs : Bytes) (q : Nat) :
    ((List.replicate (m + n) xs).flatten).foldl reader.step q =
      ((List.replicate n xs).flatten).foldl reader.step
        (((List.replicate m xs).flatten).foldl reader.step q) := by
  induction n generalizing q with
  | zero => simp
  | succ n ih =>
      rw [show m + (n + 1) = (m + n) + 1 by omega]
      rw [foldl_flatten_replicate_succ]
      rw [ih]
      rw [foldl_flatten_replicate_succ]

/-! ## State bounds and compiled-table correctness -/

private theorem stepBounded (q b : Nat) (hq : q < reader.states) :
    reader.step q b < reader.states := by
  have hq2 : q < 2 := by simpa [reader] using hq
  change (if b == 92 then 1 - q else 0) < 2
  by_cases hb : b == 92 <;> simp [hb] <;> omega

private theorem foldl_step_bounded (xs : Bytes) {q : Nat} (hq : q < reader.states) :
    xs.foldl reader.step q < reader.states := by
  induction xs generalizing q with
  | nil => simpa
  | cons x xs ih => exact ih (stepBounded q x hq)

private theorem applyT_compile_eq_foldl (x : Rope) {q : Nat} (hq : q < reader.states) :
    applyT (compile reader x) q = (expand x).foldl reader.step q := by
  induction x generalizing q with
  | lit s =>
      have hq' : q < (List.range reader.states).length := by simpa [reader] using hq
      simp [compile, applyT, List.getElem?_eq_getElem hq', expand]
  | cat a b ihA ihB =>
      have hqA : applyT (compile reader a) q < reader.states := by
        rw [ihA hq]
        exact foldl_step_bounded _ hq
      rw [compile]
      rw [applyT_chain (compile reader a) (compile reader b) q (by simp [compile_length, hq])]
      rw [ihB hqA, ihA hq]
      simp [expand]
  | rep n a ihA =>
      have hpow : ∀ m q, q < reader.states →
          applyT (power m (compile reader a)) q =
            ((List.replicate m (expand a)).flatten).foldl reader.step q := by
        intro m
        induction m using Nat.strongRecOn with
        | _ m ihM =>
            intro q hq
            by_cases hm : m = 0
            · subst m
              simp [applyT_power_zero, compile_length, hq]
            · have hpos : 0 < m := Nat.pos_of_ne_zero hm
              have hdiv : m / 2 < m := Nat.div_lt_self hpos (by decide)
              have ihH := ihM (m / 2) hdiv
              unfold power
              simp only [hm, dite_false]
              by_cases heven : m % 2 == 0
              · have hevenNat : m % 2 = 0 := by simpa using heven
                simp [heven]
                rw [applyT_chain (power (m / 2) (compile reader a))
                      (power (m / 2) (compile reader a)) q
                    (by rw [power_length, compile_length]; exact hq)]
                rw [ihH q hq]
                have hbnd : ((List.replicate (m / 2) (expand a)).flatten).foldl reader.step q <
                    reader.states := foldl_step_bounded _ hq
                rw [ihH _ hbnd]
                rw [← foldl_flatten_replicate_add]
                have hm2 : m / 2 + m / 2 = m := by omega
                rw [hm2]
              · have hoddNat : m % 2 = 1 := by
                  have hlt : m % 2 < 2 := Nat.mod_lt m (by decide)
                  have hne : m % 2 ≠ 0 := by
                    intro h0
                    apply heven
                    simp [h0]
                  omega
                simp [heven]
                rw [applyT_chain (chain (power (m / 2) (compile reader a))
                      (power (m / 2) (compile reader a))) (compile reader a) q
                    (by rw [chain_length, power_length, compile_length]; exact hq)]
                rw [applyT_chain (power (m / 2) (compile reader a))
                      (power (m / 2) (compile reader a)) q
                    (by rw [power_length, compile_length]; exact hq)]
                rw [ihH q hq]
                have hbnd1 : ((List.replicate (m / 2) (expand a)).flatten).foldl reader.step q <
                    reader.states := foldl_step_bounded _ hq
                rw [ihH _ hbnd1]
                rw [← foldl_flatten_replicate_add]
                have hbnd2 : ((List.replicate (m / 2 + m / 2) (expand a)).flatten).foldl reader.step q <
                    reader.states := foldl_step_bounded _ hq
                rw [ihA hbnd2]
                rw [← foldl_flatten_replicate_succ]
                have hm2 : m / 2 + m / 2 + 1 = m := by omega
                rw [hm2]
      rw [compile, expand]
      exact hpow n q hq

private theorem matched_eq_foldl (x : Rope) {q : Nat} (hq : q < reader.states) :
    matched reader x q = reader.accept ((expand x).foldl reader.step q) := by
  simp [matched, applyT_compile_eq_foldl x hq]

/-! ## Main result -/

theorem sound_ResetAtDelimiter : EscapedTail.ResetAtDelimiter.Goal := by
  intro a
  rw [matched_eq_foldl (.cat a (.lit [47, 92])) (by decide : 0 < reader.states)]
  simp [expand, reader]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_GrammarScans_EscapedTail_ResetAtDelimiter
