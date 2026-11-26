import TinylambOpt.ResearchGoals.StorageExecution.GrammarScans

/-!
A compressed string scan uses grammar or chunk summaries to evaluate string predicates
without always reconstructing the full value.

Proved

- `EndsInDigit.NonemptyTail` — `∀ a b, 0<extent b → matched reader (.cat a b) 0 =
  matched reader b 0`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_GrammarScans_EndsInDigit_NonemptyTail

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.GrammarScans
open TinylambOpt.ResearchGoals.StorageExecution.GrammarScans.EndsInDigit

/-! Truth check on small inputs. Every sample tail `b` satisfies `0 < extent b`,
so a mismatch below would be a genuine counterexample. -/

private def samples : List (Rope × Rope) :=
  [ (.lit [], .lit [48]), (.lit [65, 49], .lit [57]),
    (.rep 3 (.lit [48, 65]), .lit [57, 65]),
    (.cat (.lit [49]) (.rep 2 (.lit [65])), .lit [65, 48]),
    (.lit [], .cat (.lit []) (.lit [50, 97])),
    (.rep 2 (.lit [48, 57]), .lit [57]) ]

private def check : Bool :=
  samples.all fun p => 0 < extent p.2 &&
    matched reader (.cat p.1 p.2) 0 == matched reader p.2 0

#eval check

example : check := by native_decide

/-! The reader ignores its incoming state, so after any nonempty byte string
the state is just `digit` of the last byte. We first show that `compile`
agrees with folding `expand`, then reduce the goal to that reset property. -/

private def digit (b : Nat) : Nat := (decide (48 ≤ b ∧ b ≤ 57)).toNat

private theorem step_const (q b : Nat) : reader.step q b = digit b := by
  rfl

private theorem digit_lt_two (b : Nat) : digit b < 2 := by
  unfold digit
  cases decide (48 ≤ b ∧ b ≤ 57) <;> decide

private theorem foldl_step_bounded (xs : Bytes) (q : Nat) (hq : q < 2) :
    xs.foldl reader.step q < 2 := by
  induction xs generalizing q with
  | nil => simpa
  | cons b bs ih =>
      apply ih
      rw [step_const]
      exact digit_lt_two b

private theorem foldl_append_step (xs ys : Bytes) (q : Nat) :
    (xs ++ ys).foldl reader.step q = ys.foldl reader.step (xs.foldl reader.step q) := by
  induction xs generalizing q with
  | nil => simp
  | cons x xs ih => simp [ih]

private theorem foldl_append_singleton (xs : Bytes) (b q : Nat) :
    (xs ++ [b]).foldl reader.step q = digit b := by
  simp [step_const]

private theorem chain_length (a b : Table) : (chain a b).length = a.length := by
  simp [chain]

private theorem applyT_chain (a b : Table) (q : Nat) (hq : q < a.length) :
    applyT (chain a b) q = applyT b (applyT a q) := by
  have hq' : q < (chain a b).length := by simpa [chain_length] using hq
  simp [applyT, List.getElem?_eq_getElem hq, chain]

private theorem power_length (n : Nat) (t : Table) : (power n t).length = t.length := by
  induction n using Nat.strongRecOn with
  | _ n ih =>
      unfold power
      split
      · simp
      · split
        · simp [chain_length, ih (n / 2) (Nat.div_lt_self (by omega) (by decide))]
        · simp [chain_length, ih (n / 2) (Nat.div_lt_self (by omega) (by decide))]

private theorem compile_length (x : Rope) : (compile reader x).length = 2 := by
  induction x with
  | lit s => simp [compile, reader]
  | cat a b ihA ihB => simp [compile, chain_length, ihA]
  | rep n a ihA =>
      simp only [compile]
      rw [power_length, ihA]

private theorem cons_eq_append_of_forall {α : Type} [DecidableEq α] (a : α) (xs : List α)
    (h : ∀ b ∈ xs, b = a) : a :: xs = xs ++ [a] := by
  induction xs with
  | nil => simp
  | cons b bs ih =>
      have hb : b = a := h b (by simp)
      subst b
      have hbs : ∀ c ∈ bs, c = a := by
        intro c hc
        exact h c (by simp [hc])
      have ih' : a :: bs = bs ++ [a] := ih hbs
      rw [show (a :: bs) ++ [a] = a :: (bs ++ [a]) by simp]
      rw [ih']

private theorem replicate_succ_append (n : Nat) (xs : List Nat) :
    List.replicate (n + 1) xs = List.replicate n xs ++ [xs] := by
  rw [List.replicate_succ]
  exact cons_eq_append_of_forall xs (List.replicate n xs) (by
    intro b hb
    rw [List.mem_replicate] at hb
    exact hb.2)

private theorem foldl_flatten_replicate_succ (n : Nat) (xs : Bytes) (q : Nat) :
    ((List.replicate (n + 1) xs).flatten).foldl reader.step q =
      xs.foldl reader.step (((List.replicate n xs).flatten).foldl reader.step q) := by
  rw [replicate_succ_append]
  simp

private theorem foldl_flatten_replicate_add (m n : Nat) (xs : Bytes) (q : Nat) :
    ((List.replicate (m + n) xs).flatten).foldl reader.step q =
      ((List.replicate n xs).flatten).foldl reader.step
        (((List.replicate m xs).flatten).foldl reader.step q) := by
  induction n generalizing q with
  | zero => simp
  | succ n ih =>
      rw [show m + (n + 1) = (m + n) + 1 by omega]
      rw [foldl_flatten_replicate_succ (m + n) xs q, ih]
      rw [foldl_flatten_replicate_succ n xs (((List.replicate m xs).flatten).foldl reader.step q)]

private theorem applyT_compile_eq_foldl (x : Rope) (q : Nat) (hq : q < 2) :
    applyT (compile reader x) q = (expand x).foldl reader.step q := by
  induction x generalizing q with
  | lit s =>
      have hq' : q < (List.range reader.states).length := by simpa [reader] using hq
      simp [compile, applyT, expand, List.getElem?_eq_getElem hq']
  | cat a b ihA ihB =>
      have hqa : q < (compile reader a).length := by simpa [compile_length] using hq
      have hqA : applyT (compile reader a) q < 2 := by
        rw [ihA q hq]
        exact foldl_step_bounded (expand a) q hq
      rw [compile, applyT_chain _ _ _ hqa, ihB _ hqA, ihA q hq]
      simp [expand]
  | rep n a ihA =>
      have hpow : ∀ m q, q < 2 →
          applyT (power m (compile reader a)) q =
            ((List.replicate m (expand a)).flatten).foldl reader.step q := by
        intro m
        induction m using Nat.strongRecOn with
        | _ m ihM =>
            intro q hq
            by_cases hm : m = 0
            · subst m
              have hq' : q < (List.range (compile reader a).length).length := by
                simpa [compile_length] using hq
              simp [applyT, power, List.getElem?_eq_getElem hq']
            · have hpos : 0 < m := by omega
              have hdiv : m / 2 < m := Nat.div_lt_self hpos (by decide)
              have hH := ihM (m / 2) hdiv
              have hHq : applyT (power (m / 2) (compile reader a)) q =
                  ((List.replicate (m / 2) (expand a)).flatten).foldl reader.step q := hH q hq
              have hHqBound : ((List.replicate (m / 2) (expand a)).flatten).foldl reader.step q < 2 :=
                foldl_step_bounded _ q hq
              have hlenH : q < (power (m / 2) (compile reader a)).length := by
                rw [power_length, compile_length]
                exact hq
              unfold power
              simp only [hm, dite_false]
              by_cases hmod : (m % 2 == 0) = true
              · simp [hmod]
                rw [applyT_chain (power (m / 2) (compile reader a)) (power (m / 2) (compile reader a)) q hlenH]
                rw [hHq, hH _ hHqBound]
                rw [(foldl_flatten_replicate_add (m / 2) (m / 2) (expand a) q).symm]
                have hmod0 : m % 2 = 0 := by simpa using hmod
                have hm22 : m / 2 + m / 2 = m := by
                  have hdecomp := Nat.div_add_mod m 2
                  omega
                simp [hm22]
              · simp [hmod]
                have hsq : applyT (chain (power (m / 2) (compile reader a)) (power (m / 2) (compile reader a))) q =
                    ((List.replicate (m / 2 + m / 2) (expand a)).flatten).foldl reader.step q := by
                  rw [applyT_chain (power (m / 2) (compile reader a)) (power (m / 2) (compile reader a)) q hlenH,
                    hHq, hH _ hHqBound]
                  exact (foldl_flatten_replicate_add (m / 2) (m / 2) (expand a) q).symm
                have hsqLen : q < (chain (power (m / 2) (compile reader a)) (power (m / 2) (compile reader a))).length := by
                  rw [chain_length, power_length, compile_length]
                  exact hq
                rw [applyT_chain (chain (power (m / 2) (compile reader a)) (power (m / 2) (compile reader a)))
                      (compile reader a) q hsqLen, hsq]
                have hsqBound : ((List.replicate (m / 2 + m / 2) (expand a)).flatten).foldl reader.step q < 2 :=
                  foldl_step_bounded _ q hq
                rw [ihA _ hsqBound]
                have hmodne : m % 2 ≠ 0 := by
                  intro h
                  apply hmod
                  simp [h]
                have hlt : m % 2 < 2 := Nat.mod_lt m (by decide)
                have hm_eq : m = m / 2 + m / 2 + 1 := by
                  have hdecomp := Nat.div_add_mod m 2
                  omega
                have hrep : List.replicate m (expand a) =
                    List.replicate (m / 2 + m / 2 + 1) (expand a) :=
                  congrArg (fun k => List.replicate k (expand a)) hm_eq
                rw [hrep]
                rw [foldl_flatten_replicate_succ (m / 2 + m / 2) (expand a) q]
      rw [compile, hpow n q hq]
      simp [expand]

private theorem matched_eq_foldl (x : Rope) :
    matched reader x 0 = reader.accept ((expand x).foldl reader.step 0) := by
  simp [matched, applyT_compile_eq_foldl x 0 (by decide : 0 < 2)]

private theorem expand_length (x : Rope) : (expand x).length = extent x := by
  induction x with
  | lit s => simp [expand, extent]
  | cat a b ihA ihB => simp [expand, extent, ihA, ihB]
  | rep k a ihA =>
      simp [expand, extent, ihA]

private theorem exists_snoc {xs : List Nat} (h : xs ≠ []) : ∃ ys v, xs = ys ++ [v] := by
  induction xs with
  | nil => contradiction
  | cons x xs ih =>
      by_cases hxs : xs = []
      · subst xs
        exact ⟨[], x, by simp⟩
      · obtain ⟨ys, v, hv⟩ := ih hxs
        exact ⟨x :: ys, v, by simp [hv]⟩

theorem sound_NonemptyTail : EndsInDigit.NonemptyTail.Goal := by
  intro a b hne
  rw [matched_eq_foldl (.cat a b), matched_eq_foldl b]
  have hbne : expand b ≠ [] := by
    intro h
    have hlen : (expand b).length = 0 := by simp [h]
    have : extent b = 0 := by simpa [expand_length] using hlen
    omega
  obtain ⟨xs, v, hv⟩ := exists_snoc hbne
  simp [expand, hv, step_const]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_GrammarScans_EndsInDigit_NonemptyTail
