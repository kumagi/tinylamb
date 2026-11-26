import TinylambOpt.ResearchGoals.StorageExecution.GrammarScans

/-!
A compressed string scan uses grammar or chunk summaries to evaluate string predicates
without always reconstructing the full value.

Proved

- `QuotedField.QuoteParity` — `∀ x, matched reader x 0 = decide ((expand x).count 34 %
  2 = 1)`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_GrammarScans_QuotedField_QuoteParity

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.GrammarScans
open TinylambOpt.ResearchGoals.StorageExecution.GrammarScans.QuotedField

/-! Truth check on small ropes. Every sample is a well-formed `Rope`; the goal
has no extra hypotheses, so all of them are genuine test inputs. -/

private def samples : List Rope :=
  [ .lit [], .lit [34], .lit [65], .lit [34, 34], .lit [65, 34, 66],
    .cat (.lit [34]) (.lit [65]), .cat (.lit [65, 34]) (.lit [34]),
    .rep 0 (.lit [34]), .rep 2 (.lit [65]), .rep 3 (.lit [34, 34]),
    .rep 2 (.lit [34, 65]), .cat (.rep 2 (.lit [65])) (.lit [34]) ]

private def check : Bool :=
  samples.all fun x => matched reader x 0 == decide ((expand x).count 34 % 2 = 1)

#eval check

example : check := by native_decide

/-! ## Generic table / power correctness (self-contained, same pattern as the
sibling proofs in this directory). -/

private theorem chain_length (a b : Table) : (chain a b).length = a.length := by
  simp [chain]

private theorem applyT_chain (a b : Table) (q : Nat) (hq : q < a.length) :
    applyT (chain a b) q = applyT b (applyT a q) := by
  have hq' : q < (chain a b).length := by simpa [chain_length] using hq
  simp [applyT, chain, List.getElem?_eq_getElem hq', List.getElem?_eq_getElem hq]

private theorem power_length (n : Nat) (t : Table) : (power n t).length = t.length := by
  induction n using Nat.strongRecOn with
  | _ n ih =>
      by_cases hn : n = 0
      · simp [power, hn]
      · have hpos : 0 < n := by omega
        have hhalf : (power (n / 2) t).length = t.length :=
          ih (n / 2) (Nat.div_lt_self hpos (by decide : 1 < 2))
        unfold power
        rw [dif_neg hn]
        dsimp only
        by_cases heven : n % 2 == 0
        · simp [heven, chain_length, hhalf]
        · simp [heven, chain_length, hhalf]

private theorem compile_length (x : Rope) : (compile reader x).length = reader.states := by
  induction x with
  | lit s => simp [compile]
  | cat a b ihA ihB => simp [compile, chain_length, ihA]
  | rep n a ihA =>
      change (power n (compile reader a)).length = reader.states
      rw [power_length]
      exact ihA

private theorem getD_range (i n : Nat) (hi : i < n) : (List.range n)[i]?.getD i = i := by
  have hi' : i < (List.range n).length := by simpa using hi
  simp [List.getElem?_eq_getElem hi']

private theorem applyT_power_zero (t : Table) (q : Nat) (hq : q < t.length) :
    applyT (power 0 t) q = q := by
  simp [power, applyT, getD_range q t.length hq]

private theorem foldl_append_step (xs ys : Bytes) (q : Nat) :
    (xs ++ ys).foldl reader.step q = ys.foldl reader.step (xs.foldl reader.step q) := by
  induction xs generalizing q with
  | nil => simp
  | cons x xs ih => simp [ih]

private theorem replicate_succ_last (n : Nat) (xs : Bytes) :
    List.replicate (n + 1) xs = List.replicate n xs ++ [xs] := by
  induction n with
  | zero => simp
  | succ n ih =>
      calc
        List.replicate (Nat.succ n + 1) xs = xs :: List.replicate (n + 1) xs := by
          rw [List.replicate_succ]
        _ = xs :: (List.replicate n xs ++ [xs]) := by rw [ih]
        _ = (xs :: List.replicate n xs) ++ [xs] := by rfl
        _ = List.replicate (n + 1) xs ++ [xs] := by rw [List.replicate_succ]

private theorem foldl_flatten_replicate_succ (n : Nat) (xs : Bytes) (q : Nat) :
    ((List.replicate (n + 1) xs).flatten).foldl reader.step q =
      xs.foldl reader.step (((List.replicate n xs).flatten).foldl reader.step q) := by
  rw [replicate_succ_last]
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

/-! ## `QuotedField.reader` specifics -/

private theorem stepBounded : ∀ q b, q < reader.states → reader.step q b < reader.states := by
  intro q b hq
  have hq2 : q < 2 := by simpa [reader] using hq
  by_cases hb : b = 34
  · simp [reader, hb]
    omega
  · simpa [reader, hb] using hq2

private theorem foldl_step_bounded (xs : Bytes) {q : Nat} (hq : q < reader.states) :
    xs.foldl reader.step q < reader.states := by
  induction xs generalizing q with
  | nil => simpa
  | cons x xs ih => exact ih (stepBounded q x hq)

/-- The compiled table of a rope agrees with interpreting its expanded bytes
from any valid state. -/
private theorem applyT_compile_eq_foldl (x : Rope) {q : Nat} (hq : q < reader.states) :
    applyT (compile reader x) q = (expand x).foldl reader.step q := by
  induction x generalizing q with
  | lit s =>
      have hq' : q < (List.range reader.states).length := by simpa using hq
      simp [compile, applyT, expand, List.getElem?_eq_getElem hq']
  | cat a b ihA ihB =>
      have hqA' : (expand a).foldl reader.step q < reader.states :=
        foldl_step_bounded (expand a) hq
      rw [show compile reader (.cat a b) = chain (compile reader a) (compile reader b) by rfl]
      rw [applyT_chain (compile reader a) (compile reader b) q (by rw [compile_length]; exact hq)]
      rw [ihA hq, ihB hqA']
      simp [expand, foldl_append_step]
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
              simp [applyT_power_zero (compile reader a) q (by rw [compile_length]; exact hq)]
            · have hpos : 0 < m := by omega
              have hhalf : m / 2 < m := Nat.div_lt_self hpos (by decide : 1 < 2)
              have ihH := ihM (m / 2) hhalf
              unfold power
              rw [dif_neg hm]
              dsimp only
              have hlen_h : (power (m / 2) (compile reader a)).length = reader.states := by
                rw [power_length, compile_length]
              have hHq : applyT (power (m / 2) (compile reader a)) q =
                  ((List.replicate (m / 2) (expand a)).flatten).foldl reader.step q := ihH q hq
              have hHqBound : ((List.replicate (m / 2) (expand a)).flatten).foldl reader.step q <
                  reader.states := foldl_step_bounded _ hq
              have hsq : applyT (chain (power (m / 2) (compile reader a))
                                         (power (m / 2) (compile reader a))) q =
                  ((List.replicate (m / 2 + m / 2) (expand a)).flatten).foldl reader.step q := by
                rw [applyT_chain _ _ q (by rw [hlen_h]; exact hq)]
                rw [hHq]
                rw [show applyT (power (m / 2) (compile reader a))
                        (((List.replicate (m / 2) (expand a)).flatten).foldl reader.step q) =
                    ((List.replicate (m / 2) (expand a)).flatten).foldl reader.step
                      (((List.replicate (m / 2) (expand a)).flatten).foldl reader.step q) from
                  ihH _ hHqBound]
                rw [foldl_flatten_replicate_add]
              by_cases heven : m % 2 == 0
              · rw [if_pos heven]
                have hmzero : m % 2 = 0 := by simpa [beq_iff_eq] using heven
                have hm2 : m / 2 + m / 2 = m := by omega
                simpa [hm2] using hsq
              · rw [if_neg heven]
                rw [applyT_chain (chain (power (m / 2) (compile reader a))
                                         (power (m / 2) (compile reader a)))
                                  (compile reader a) q
                    (by rw [chain_length, hlen_h]; exact hq)]
                rw [hsq]
                have hsqBound : ((List.replicate (m / 2 + m / 2) (expand a)).flatten).foldl reader.step q <
                    reader.states := foldl_step_bounded _ hq
                rw [ihA hsqBound]
                have hmodne : m % 2 ≠ 0 := by
                  intro h0
                  apply heven
                  simp [h0]
                have hmodlt : m % 2 < 2 := Nat.mod_lt _ (by decide)
                have hm2 : m / 2 + m / 2 + 1 = m := by omega
                have hlast := foldl_flatten_replicate_succ (m / 2 + m / 2) (expand a) q
                rw [hm2] at hlast
                exact hlast.symm
      change applyT (power n (compile reader a)) q =
        ((List.replicate n (expand a)).flatten).foldl reader.step q
      exact hpow n q hq

/-- The step function toggles on byte 34, so folding a byte string from state
`q ∈ {0,1}` yields `1` exactly when `count 34 + q` is odd. -/
private theorem foldl_parity (xs : Bytes) (q : Nat) (hq : q < 2) :
    xs.foldl reader.step q = if (xs.count 34 + q) % 2 = 1 then 1 else 0 := by
  induction xs generalizing q with
  | nil =>
      have hq01 : q = 0 ∨ q = 1 := by omega
      rcases hq01 with rfl | rfl <;> simp
  | cons b bs ih =>
      simp only [List.foldl_cons, List.count_cons]
      have hq1 : reader.step q b < 2 := by
        have hq' : q < reader.states := by simpa [reader] using hq
        have : reader.step q b < reader.states := stepBounded q b hq'
        simpa [reader] using this
      rw [ih _ hq1]
      have hmod : bs.count 34 % 2 < 2 := Nat.mod_lt _ (by decide : 0 < 2)
      have hm01 : bs.count 34 % 2 = 0 ∨ bs.count 34 % 2 = 1 := by omega
      have hq01 : q = 0 ∨ q = 1 := by omega
      rcases hm01 with hm | hm <;> rcases hq01 with rfl | rfl
      all_goals
        by_cases hb : b = 34
        · subst hb
          simp [reader, hm, Nat.add_mod, Nat.mul_mod]
        · have hb' : (b == 34) = false := by simpa using hb
          simp [reader, hb, hb', hm, Nat.add_mod, Nat.mul_mod]

theorem sound_QuoteParity : QuotedField.QuoteParity.Goal := by
  intro x
  have hq : (0 : Nat) < reader.states := by decide
  have hmatched : matched reader x 0 = reader.accept ((expand x).foldl reader.step 0) := by
    simp only [matched]
    rw [applyT_compile_eq_foldl x hq]
  rw [hmatched]
  rw [foldl_parity (expand x) 0 (by decide : (0 : Nat) < 2)]
  simp only [reader]
  have hlt : (expand x).count 34 % 2 < 2 := Nat.mod_lt _ (by decide : 0 < 2)
  cases h : (expand x).count 34 % 2 with
  | zero => simp
  | succ n =>
      have hn : n = 0 := by omega
      subst n
      simp

end TinylambOpt.ResearchProofs.StorageExecution.Goal_GrammarScans_QuotedField_QuoteParity
