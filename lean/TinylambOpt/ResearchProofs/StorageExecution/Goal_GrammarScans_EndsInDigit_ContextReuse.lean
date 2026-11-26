import TinylambOpt.ResearchGoals.StorageExecution.GrammarScans

/-!
A compressed string scan uses grammar or chunk summaries to evaluate string predicates
without always reconstructing the full value.

Proved

- `EndsInDigit.ContextReuse` — Substitute a different grammar with the same transition
  table inside a larger row.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_GrammarScans_EndsInDigit_ContextReuse

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.GrammarScans
open TinylambOpt.ResearchGoals.StorageExecution.GrammarScans.EndsInDigit

/-!
  Truth check on small inputs.  Every sample pair `x, y` has *equal* compiled
  tables (this is checked by `check`), so a mismatch would be a genuine
  counterexample.  The samples cover empty phrases, digit/non-digit tails,
  concatenation and repetition, and both valid start states `q = 0, 1`.
-/

private def samples : List (Rope × Rope × Rope × Rope) :=
  [ (.lit [], .lit [], .lit [], .lit []),
    (.lit [65], .lit [48], .lit [48], .lit [49]),
    (.lit [48, 57], .cat (.lit []) (.lit [65]), .lit [48, 65], .lit [57, 66]),
    (.rep 2 (.lit [49]), .lit [50], .rep 2 (.lit [56]), .lit [48]),
    (.cat (.lit [65]) (.rep 3 (.lit [48])), .lit [], .rep 0 (.lit [48]), .lit []),
    (.lit [49], .lit [65, 48], .cat (.lit [48]) (.lit [65]), .rep 2 (.lit [65])) ]

private def check : Bool :=
  samples.all fun p =>
    let a := p.1
    let b := p.2.1
    let x := p.2.2.1
    let y := p.2.2.2
    compile reader x == compile reader y &&
      (List.range reader.states).all fun q =>
        (expand (.cat a (.cat x b))).foldl reader.step q ==
          (expand (.cat a (.cat y b))).foldl reader.step q

#eval check

example : check := by native_decide

/-!
  Proof.  `EndsInDigit.reader.step` ignores the incoming state and always
  returns a state below `reader.states = 2`.  Hence a compiled table records
  exactly the fold of the phrase from each of the two possible states, and
  equal tables give equal folds at the intermediate state reached after `a`.
  Appending `b` then preserves that equality.
-/

private theorem chain_length (a b : Table) : (chain a b).length = a.length := by
  simp [chain]

private theorem power_length (n : Nat) (t : Table) : (power n t).length = t.length := by
  induction n using Nat.strongRecOn with
  | _ n ih =>
      unfold power
      by_cases hn : n = 0
      · simp [hn]
      · simp only [hn, dite_false]
        have hlt : n / 2 < n := Nat.div_lt_self (Nat.pos_of_ne_zero hn) (by decide)
        have ihh := ih (n / 2) hlt
        by_cases he : n % 2 == 0 <;> simp [he, chain_length, ihh]

private theorem compile_length (x : Rope) : (compile reader x).length = reader.states := by
  induction x with
  | lit s => simp [compile]
  | cat a b iha _ => simp [compile, chain_length, iha]
  | rep n a ih => simp [compile, power_length, ih]

private theorem applyT_chain (a b : Table) (q : Nat) (hq : q < a.length) :
    applyT (chain a b) q = applyT b (applyT a q) := by
  simp [applyT, List.getElem?_eq_getElem hq, chain]

private theorem getD_range (i n : Nat) (hi : i < n) : (List.range n)[i]?.getD i = i := by
  have hi' : i < (List.range n).length := by simpa using hi
  simp [List.getElem?_eq_getElem hi']

private theorem applyT_power_zero (t : Table) (q : Nat) (hq : q < t.length) :
    applyT (power 0 t) q = q := by
  simp [power, applyT, getD_range q t.length hq]

private theorem stepBounded : ∀ q b, q < reader.states → reader.step q b < reader.states := by
  intro q b hq
  change (decide (48 ≤ b ∧ b ≤ 57)).toNat < 2
  by_cases h1 : 48 ≤ b <;> by_cases h2 : b ≤ 57 <;> simp [h1, h2]

private theorem foldl_step_bounded (xs : Bytes) {q : Nat} (hq : q < reader.states) :
    xs.foldl reader.step q < reader.states := by
  induction xs generalizing q with
  | nil => simpa
  | cons x xs ih => exact ih (stepBounded q x hq)

private theorem foldl_append_step (xs ys : Bytes) (q : Nat) :
    (xs ++ ys).foldl reader.step q = ys.foldl reader.step (xs.foldl reader.step q) := by
  induction xs generalizing q with
  | nil => simp
  | cons x xs ih => simp [ih]

private theorem replicate_succ_append (n : Nat) (xs : Bytes) :
    List.replicate (n + 1) xs = List.replicate n xs ++ [xs] := by
  simpa using (List.replicate_append_replicate (n := n) (a := xs) (m := 1)).symm

private theorem foldl_flatten_replicate_succ (n : Nat) (xs : Bytes) (q : Nat) :
    ((List.replicate (n + 1) xs).flatten).foldl reader.step q =
      xs.foldl reader.step (((List.replicate n xs).flatten).foldl reader.step q) := by
  rw [replicate_succ_append]
  simp [List.flatten_append]

private theorem foldl_flatten_replicate_add (m n : Nat) (xs : Bytes) (q : Nat) :
    ((List.replicate (m + n) xs).flatten).foldl reader.step q =
      ((List.replicate n xs).flatten).foldl reader.step
        (((List.replicate m xs).flatten).foldl reader.step q) := by
  induction n generalizing q with
  | zero => simp
  | succ n ih =>
      rw [show m + (n + 1) = (m + n) + 1 by omega]
      simp only [foldl_flatten_replicate_succ]
      simp [ih]

private theorem applyT_compile_eq_foldl (x : Rope) {q : Nat} (hq : q < reader.states) :
    applyT (compile reader x) q = (expand x).foldl reader.step q := by
  induction x generalizing q with
  | lit s =>
      have hq2 : q < 2 := by simpa [reader] using hq
      simp [compile, applyT, reader, hq2, expand]
  | cat a b ihA ihB =>
      have hqA : (expand a).foldl reader.step q < reader.states :=
        foldl_step_bounded _ hq
      rw [compile, applyT_chain (compile reader a) (compile reader b) q
          (by simpa [compile_length] using hq)]
      rw [ihA hq, ihB hqA]
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
              let h := power (m / 2) (compile reader a)
              let sq := chain h h
              have hHq : applyT h q =
                  ((List.replicate (m / 2) (expand a)).flatten).foldl reader.step q := by
                exact ihH q hq
              have hHqBound : ((List.replicate (m / 2) (expand a)).flatten).foldl
                  reader.step q < reader.states := foldl_step_bounded _ hq
              have hsq : applyT sq q =
                  ((List.replicate (m / 2 + m / 2) (expand a)).flatten).foldl reader.step q := by
                dsimp [sq]
                rw [applyT_chain h h q (by simp [h, power_length, compile_length, hq])]
                rw [hHq, ihH _ hHqBound, foldl_flatten_replicate_add]
              have hsqBound : ((List.replicate (m / 2 + m / 2) (expand a)).flatten).foldl
                  reader.step q < reader.states := foldl_step_bounded _ hq
              cases hmod : m % 2 with
              | zero =>
                  have hm2 : m / 2 + m / 2 = m := by omega
                  simp
                  change applyT sq q = ((List.replicate m (expand a)).flatten).foldl reader.step q
                  simpa [hm2] using hsq
              | succ k =>
                  have hk : k = 0 := by omega
                  subst k
                  simp
                  have hm2 : m / 2 + m / 2 + 1 = m := by omega
                  change applyT (chain sq (compile reader a)) q =
                    ((List.replicate m (expand a)).flatten).foldl reader.step q
                  rw [applyT_chain sq (compile reader a) q
                    (by simpa [sq, h, chain_length, power_length, compile_length] using hq)]
                  rw [hsq, ihA hsqBound]
                  have h1 : ((List.replicate (m / 2 + m / 2 + 1) (expand a)).flatten).foldl
                      reader.step q =
                      (expand a).foldl reader.step
                        (((List.replicate (m / 2 + m / 2) (expand a)).flatten).foldl reader.step q) := by
                    rw [foldl_flatten_replicate_succ]
                  simpa [hm2] using h1.symm
      change applyT (power n (compile reader a)) q =
        ((List.replicate n (expand a)).flatten).foldl reader.step q
      exact hpow n q hq

theorem sound_ContextReuse : EndsInDigit.ContextReuse.Goal := by
  intro a b x y q hq hxy
  have hqa : (expand a).foldl reader.step q < reader.states :=
    foldl_step_bounded _ hq
  have hx1 := applyT_compile_eq_foldl x hqa
  have hy1 := applyT_compile_eq_foldl y hqa
  have hx' : (expand x).foldl reader.step ((expand a).foldl reader.step q) =
      (expand y).foldl reader.step ((expand a).foldl reader.step q) := by
    rw [← hx1, ← hy1, hxy]
  simp [expand, hx']

end TinylambOpt.ResearchProofs.StorageExecution.Goal_GrammarScans_EndsInDigit_ContextReuse
