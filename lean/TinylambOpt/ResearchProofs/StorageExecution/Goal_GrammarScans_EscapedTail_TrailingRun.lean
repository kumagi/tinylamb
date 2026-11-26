import TinylambOpt.ResearchGoals.StorageExecution.GrammarScans

/-!
A compressed string scan uses grammar or chunk summaries to evaluate string predicates
without always reconstructing the full value.

Proved

- `EscapedTail.TrailingRun` — `∀ x, matched reader x 0 = decide (((expand
  x).reverse.takeWhile (· == 92)).length%2=1)`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_GrammarScans_EscapedTail_TrailingRun

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.GrammarScans
open TinylambOpt.ResearchGoals.StorageExecution.GrammarScans.EscapedTail

/-! Truth check on small ropes. Every sample is a well-formed `Rope`; the goal
has no extra hypotheses, so all of them are genuine test inputs. -/

private def samples : List Rope :=
  [ .lit [], .lit [92], .lit [65], .lit [92, 92], .lit [65, 92, 66],
    .lit [92, 65, 92], .lit [92, 92, 92, 65], .lit [65, 92, 92, 92],
    .cat (.lit [92]) (.lit [65]), .cat (.lit [65, 92]) (.lit [92]),
    .rep 0 (.lit [92]), .rep 2 (.lit [65]), .rep 3 (.lit [92, 92]),
    .rep 2 (.lit [92, 65]), .cat (.rep 2 (.lit [65])) (.lit [92]) ]

private def check : Bool :=
  samples.all fun x =>
    matched reader x 0 == decide (((expand x).reverse.takeWhile (· == 92)).length % 2 = 1)

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
        rw [dite_eq_right hn]
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

/-! ## `EscapedTail.reader` specifics -/

private theorem stepBounded : ∀ q b, q < reader.states → reader.step q b < reader.states := by
  intro q b hq
  have hq2 : q < 2 := by simpa [reader] using hq
  by_cases hb : b = 92
  · subst b
    simp [reader]
    omega
  · simp [reader, hb]

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
              rw [dite_eq_right hm]
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
              · rw [ite_eq_left heven]
                have hmzero : m % 2 = 0 := by simpa [beq_iff_eq] using heven
                have hm2 : m / 2 + m / 2 = m := by omega
                simpa [hm2] using hsq
              · rw [ite_eq_right heven]
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

/-! ## Trailing backslash run -/

private def tailRun (s : Bytes) : Nat := (s.reverse.takeWhile (· == 92)).length

private theorem tailRun_append (pre : Bytes) (b : Nat) :
    tailRun (pre ++ [b]) = if b = 92 then tailRun pre + 1 else 0 := by
  by_cases hb : b = 92
  · subst b
    simp [tailRun, List.reverse_append, List.takeWhile_cons]
  · simp [tailRun, List.reverse_append, List.takeWhile_cons, hb]

/-- Folding from state 0 yields the parity of the trailing run of backslashes:
a non-backslash byte resets the state to 0, a backslash toggles it. -/
private theorem foldl_run (s : Bytes) : s.foldl reader.step 0 = tailRun s % 2 := by
  induction hlen : s.length using Nat.strongRecOn generalizing s with
  | _ n ih =>
      cases hs : s.getLast? with
      | none =>
          have hempty : s = [] := List.getLast?_eq_none_iff.mp hs
          subst s
          simp [tailRun, reader]
      | some b =>
          obtain ⟨pre, hpre⟩ := List.getLast?_eq_some_iff.mp hs
          subst s
          have hprelen : pre.length < n := by
            rw [← hlen]
            simp
          have ihpre := ih pre.length hprelen pre rfl
          rw [List.foldl_append]
          simp only [List.foldl_cons, List.foldl_nil, ihpre]
          rw [tailRun_append]
          by_cases hb : b = 92
          · rw [ite_eq_left hb]
            have hbeq : (b == 92) = true := by simpa using hb
            have hadd : (tailRun pre + 1) % 2 = (tailRun pre % 2 + 1) % 2 := by
              rw [Nat.add_mod]
            rw [hadd]
            have hm01 : tailRun pre % 2 = 0 ∨ tailRun pre % 2 = 1 := by
              have hlt := Nat.mod_lt (tailRun pre) (by decide : 0 < 2)
              omega
            rcases hm01 with hm | hm <;> simp [reader, hb, hbeq, hm]
          · rw [ite_eq_right hb]
            have hbeq : (b == 92) = false := by simpa using hb
            simp [reader, hb, hbeq]

theorem sound_TrailingRun : EscapedTail.TrailingRun.Goal := by
  intro x
  have hq : (0 : Nat) < reader.states := by decide
  unfold matched
  rw [applyT_compile_eq_foldl x hq]
  change (reader.accept ((expand x).foldl reader.step 0)) =
    decide (((expand x).reverse.takeWhile (· == 92)).length % 2 = 1)
  rw [foldl_run (expand x)]
  change (tailRun (expand x) % 2 == 1) = decide (tailRun (expand x) % 2 = 1)
  by_cases h : tailRun (expand x) % 2 = 1 <;> simp [h]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_GrammarScans_EscapedTail_TrailingRun
