import TinylambOpt.ResearchGoals.StorageExecution.GrammarScans

/-!
A compressed string scan uses grammar or chunk summaries to evaluate string predicates
without always reconstructing the full value.

Proved

- `EndsInDigit.LastByte` — `∀ x, matched reader x 0 = (expand x).getLast?.any (fun b =>
  48≤b && b≤57)`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_GrammarScans_EndsInDigit_LastByte

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.GrammarScans
open TinylambOpt.ResearchGoals.StorageExecution.GrammarScans.EndsInDigit

/-! Truth check on small inputs. The samples cover literals (including the
empty one), concatenation and repetition (including an empty phrase), and
compare `matched` directly with the last-byte digit predicate. -/

private def samples : List Rope :=
  [ .lit [], .lit [48], .lit [57], .lit [65], .lit [48, 57], .lit [57, 65, 48],
    .cat (.lit [48]) (.lit [65]), .cat (.lit [65, 49]) (.lit [57]),
    .rep 0 (.lit [48]), .rep 2 (.lit [57]), .rep 3 (.lit []),
    .rep 2 (.lit [48, 65, 57]), .cat (.rep 2 (.lit [65])) (.lit [50]) ]

private def check : Bool :=
  samples.all fun x =>
    matched reader x 0 == (expand x).getLast?.any (fun b => 48 ≤ b && b ≤ 57)

#eval check

example : check := by native_decide

/-! ## Generic table infrastructure -/

private theorem chain_length (a b : Table) : (chain a b).length = a.length := by
  simp [chain]

private theorem applyT_chain (a b : Table) (q : Nat) (hq : q < a.length) :
    applyT (chain a b) q = applyT b (applyT a q) := by
  simp [applyT, List.getElem?_eq_getElem hq, chain]

private theorem power_length (n : Nat) (t : Table) : (power n t).length = t.length := by
  induction n using Nat.strongRecOn with
  | _ n ih =>
      by_cases hn : n = 0
      · simp [power.eq_1, hn]
      · rw [power.eq_1]
        simp only [hn, dite_false]
        have hdiv : n / 2 < n := Nat.div_lt_self (Nat.pos_of_ne_zero hn) (by omega)
        have ihH : (power (n / 2) t).length = t.length := ih (n / 2) hdiv
        by_cases heven : n % 2 == 0
        · simp [heven, chain_length, ihH]
        · simp [heven, chain_length, ihH]

private theorem compile_length (x : Rope) : (compile reader x).length = reader.states := by
  induction x with
  | lit s => simp [compile, reader]
  | cat a b ihA ihB => simp [compile, chain_length, ihA]
  | rep n a ihA => simp [compile, power_length, ihA]

private theorem getD_range (i n : Nat) (hi : i < n) : (List.range n)[i]?.getD i = i := by
  have hi' : i < (List.range n).length := by simpa using hi
  simp [List.getElem?_eq_getElem hi']

private theorem applyT_power_zero (t : Table) (q : Nat) (hq : q < t.length) :
    applyT (power 0 t) q = q := by
  rw [power.eq_1]
  simpa [applyT] using getD_range q t.length hq

/-! ## foldl composition -/

private theorem foldl_append_step (xs ys : Bytes) (q : Nat) :
    (xs ++ ys).foldl reader.step q = ys.foldl reader.step (xs.foldl reader.step q) := by
  induction xs generalizing q with
  | nil => simp
  | cons x xs ih => simp [ih]

private theorem replicate_add (n m : Nat) (xs : Bytes) :
    List.replicate (n + m) xs = List.replicate n xs ++ List.replicate m xs := by
  induction n with
  | zero => simp
  | succ n ih =>
      rw [show Nat.succ n + m = (n + m) + 1 by omega, List.replicate_succ, ih]
      rw [List.replicate_succ]
      rfl

private theorem foldl_flatten_replicate_succ (n : Nat) (xs : Bytes) (q : Nat) :
    ((List.replicate (n + 1) xs).flatten).foldl reader.step q =
      xs.foldl reader.step (((List.replicate n xs).flatten).foldl reader.step q) := by
  have hrep : List.replicate (n + 1) xs = List.replicate n xs ++ [xs] := by
    simpa [List.replicate_one] using replicate_add n 1 xs
  rw [hrep, List.flatten_append, foldl_append_step]
  simp

private theorem foldl_flatten_replicate_add (m n : Nat) (xs : Bytes) (q : Nat) :
    ((List.replicate (m + n) xs).flatten).foldl reader.step q =
      ((List.replicate n xs).flatten).foldl reader.step
        (((List.replicate m xs).flatten).foldl reader.step q) := by
  induction n generalizing q with
  | zero => simp
  | succ n ih =>
      rw [show m + Nat.succ n = (m + n) + 1 by omega, foldl_flatten_replicate_succ]
      rw [ih]
      rw [foldl_flatten_replicate_succ]

/-! ## Step bounds and compiled-table correctness -/

private theorem bool_toNat_lt_two (b : Bool) : b.toNat < 2 := by
  cases b <;> decide

private theorem stepBounded (q b : Nat) (_hq : q < reader.states) :
    reader.step q b < reader.states := by
  simpa [reader] using bool_toNat_lt_two (decide (48 ≤ b ∧ b ≤ 57))

private theorem foldl_step_bounded (xs : Bytes) {q : Nat} (hq : q < reader.states) :
    xs.foldl reader.step q < reader.states := by
  induction xs generalizing q with
  | nil => simpa
  | cons x xs ih => exact ih (stepBounded q x hq)

private theorem applyT_compile_eq_foldl (x : Rope) {q : Nat} (hq : q < reader.states) :
    applyT (compile reader x) q = (expand x).foldl reader.step q := by
  induction x generalizing q with
  | lit s =>
      have hq' : q < (List.range reader.states).length := by simpa using hq
      simp [compile, expand, applyT, List.getElem?_eq_getElem hq']
  | cat a b ihA ihB =>
      have hqA : applyT (compile reader a) q < reader.states := by
        rw [ihA hq]
        exact foldl_step_bounded _ hq
      rw [compile]
      rw [applyT_chain (compile reader a) (compile reader b) q
        (by simp [compile_length, hq])]
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
              have hdiv : m / 2 < m := Nat.div_lt_self hpos (by omega)
              have ihH := ihM (m / 2) hdiv
              rw [power.eq_1]
              simp only [hm, dite_false]
              have hHq : applyT (power (m / 2) (compile reader a)) q =
                  ((List.replicate (m / 2) (expand a)).flatten).foldl reader.step q := ihH q hq
              have hHqBound : ((List.replicate (m / 2) (expand a)).flatten).foldl reader.step q <
                  reader.states := foldl_step_bounded _ hq
              have hsq : applyT (chain (power (m / 2) (compile reader a))
                    (power (m / 2) (compile reader a))) q =
                  ((List.replicate (m / 2 + m / 2) (expand a)).flatten).foldl reader.step q := by
                rw [applyT_chain _ _ q (by rw [power_length, compile_length]; exact hq)]
                rw [hHq, ihH _ hHqBound, foldl_flatten_replicate_add]
              have hsqBound : ((List.replicate (m / 2 + m / 2) (expand a)).flatten).foldl reader.step q <
                  reader.states := foldl_step_bounded _ hq
              cases hmod : m % 2 with
              | zero =>
                  have hm2 : m / 2 + m / 2 = m := by omega
                  simp
                  rw [hsq]
                  simp [hm2]
              | succ k =>
                  have hk : k = 0 := by omega
                  subst k
                  have hm2 : m / 2 + m / 2 + 1 = m := by omega
                  simp
                  rw [applyT_chain (chain (power (m / 2) (compile reader a))
                      (power (m / 2) (compile reader a))) (compile reader a) q
                    (by rw [chain_length, power_length, compile_length]; exact hq)]
                  rw [hsq, ihA hsqBound]
                  have h1 : ((List.replicate (m / 2 + m / 2 + 1) (expand a)).flatten).foldl reader.step q =
                      (expand a).foldl reader.step
                        (((List.replicate (m / 2 + m / 2) (expand a)).flatten).foldl reader.step q) := by
                    rw [foldl_flatten_replicate_succ]
                  simpa [hm2] using h1.symm
      simp only [compile]
      rw [hpow n q hq]
      simp [expand]

private theorem matched_eq_foldl (x : Rope) {q : Nat} (hq : q < reader.states) :
    matched reader x q = reader.accept ((expand x).foldl reader.step q) := by
  simp [matched, applyT_compile_eq_foldl x hq]

/-! ## EndsInDigit specific fold -/

private theorem fold_step (xs : Bytes) (q : Nat) :
    xs.foldl reader.step q =
      match xs.getLast? with
      | none => q
      | some b => (decide (48 ≤ b ∧ b ≤ 57)).toNat := by
  induction xs generalizing q with
  | nil => simp
  | cons b bs ih =>
      simp only [List.foldl_cons]
      rw [ih]
      cases hlast : bs.getLast? with
      | none =>
          have hbs : bs = [] := List.getLast?_eq_none_iff.mp hlast
          subst bs
          simp [reader]
      | some v =>
          simp [List.getLast?_cons, hlast]

private theorem digitNat_eq_one (b : Nat) :
    ((decide (48 ≤ b ∧ b ≤ 57)).toNat == 1) = (48 ≤ b && b ≤ 57) := by
  by_cases h1 : 48 ≤ b <;> by_cases h2 : b ≤ 57 <;> simp [h1, h2]

theorem sound_LastByte : EndsInDigit.LastByte.Goal := by
  intro x
  rw [matched_eq_foldl x (by decide : 0 < reader.states)]
  rw [fold_step]
  cases h : (expand x).getLast? with
  | none => simp [reader]
  | some b =>
      change ((decide (48 ≤ b ∧ b ≤ 57)).toNat == 1) = (48 ≤ b && b ≤ 57)
      exact digitNat_eq_one b

end TinylambOpt.ResearchProofs.StorageExecution.Goal_GrammarScans_EndsInDigit_LastByte
