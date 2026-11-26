import TinylambOpt.ResearchGoals.StorageExecution.GrammarScans

/-!
A compressed string scan uses grammar or chunk summaries to evaluate string predicates
without always reconstructing the full value.

Proved

- `SlashDepth.RepeatedState` — Exponentiation of the table skips arbitrarily long
  repeated strings.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_GrammarScans_SlashDepth_RepeatedState

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.GrammarScans
open TinylambOpt.ResearchGoals.StorageExecution.GrammarScans.SlashDepth

/-! Small-input truth check: literals, concatenations and repetitions at several
    state/period counts. -/
private def samples : List Rope :=
  [ .lit [], .lit [47], .lit [65, 66], .lit [47, 47],
    .cat (.lit [65]) (.lit [66, 47]), .cat (.lit [47]) (.lit [65]),
    .rep 0 (.lit [47]), .rep 3 (.lit [65]), .rep 2 (.lit [65, 47]),
    .cat (.rep 2 (.lit [65, 66])) (.lit [47]),
    .rep 2 (.cat (.lit [47]) (.lit [65])),
    .lit [47, 47, 65, 47, 47, 47] ]

private def check : Bool :=
  samples.all fun x => (List.range 8).all fun n => (List.range 3).all fun q =>
    applyT (power n (compile reader x)) q ==
      ((List.replicate n (expand x)).flatten).foldl reader.step q

#eval check
example : check := by native_decide

/-! The state space is closed: every transition stays in `{0,1,2}`. -/
private theorem step_lt (q b : Nat) (hq : q < 3) : reader.step q b < 3 := by
  by_cases hb : b = 47
  · simp [reader, hb]
    omega
  · simpa [reader, hb] using hq

private theorem foldl_lt (s : Bytes) (q : Nat) (hq : q < 3) :
    s.foldl reader.step q < 3 := by
  induction s generalizing q with
  | nil => simpa using hq
  | cons b bs ih =>
      rw [List.foldl_cons]
      exact ih _ (step_lt q b hq)

/-! A self-contained iteration operator (no Mathlib `f^[n]` here). -/
private def it (f : Nat → Nat) : Nat → Nat → Nat
  | 0, q => q
  | n + 1, q => it f n (f q)

private theorem it_commute (f : Nat → Nat) : ∀ n q, it f n (f q) = f (it f n q) := by
  intro n
  induction n with
  | zero => intro q; rfl
  | succ n ih => intro q; simp [it, ih]

private theorem it_add (f : Nat → Nat) : ∀ a b q, it f (a + b) q = it f a (it f b q) := by
  intro a
  induction a with
  | zero => intro b q; simp [it]
  | succ a ih =>
      intro b q
      rw [Nat.succ_add]
      simp [it, ih, it_commute]

private theorem it_lt (f : Nat → Nat) (hclose : ∀ q, q < 3 → f q < 3) :
    ∀ n q, q < 3 → it f n q < 3 := by
  intro n
  induction n with
  | zero => intro q hq; simpa [it] using hq
  | succ n ih => intro q hq; simp [it]; exact ih _ (hclose q hq)

private theorem it_congr (f g : Nat → Nat) (hfg : ∀ q, q < 3 → f q = g q)
    (hfclose : ∀ q, q < 3 → f q < 3) :
    ∀ n q, q < 3 → it f n q = it g n q := by
  intro n
  induction n with
  | zero => intro q hq; rfl
  | succ n ih =>
      intro q hq
      have hgq : g q < 3 := by rw [← hfg q hq]; exact hfclose q hq
      rw [it, it]
      rw [hfg q hq]
      exact ih _ hgq

/-! Table utilities. -/
private theorem chain_length (a b : Table) : (chain a b).length = a.length := by
  simp [chain]

private theorem power_length (n : Nat) (t : Table) : (power n t).length = t.length := by
  induction n using Nat.strongRecOn with
  | ind n ih =>
      by_cases hn : n = 0
      · subst n
        simp [power]
      · unfold power
        rw [dif_neg hn]
        dsimp
        have hlt : n / 2 < n := Nat.div_lt_self (by omega) (by decide)
        by_cases he : n % 2 == 0
        · rw [if_pos he, chain_length, ih (n / 2) hlt]
        · rw [if_neg he, chain_length, chain_length, ih (n / 2) hlt]

private theorem applyT_chain (a b : Table) (q : Nat) (hq : q < a.length) :
    applyT (chain a b) q = applyT b (applyT a q) := by
  simp [applyT, chain, hq]

private theorem applyT_range (q : Nat) (hq : q < 3) : applyT (List.range 3) q = q := by
  simp [applyT, List.getElem?_range hq]

/-! `power n t` is the n-fold composition of `applyT t`, as long as the table
    has length 3 and maps `{0,1,2}` into itself. -/
private theorem power_iter (t : Table) (hlen : t.length = 3)
    (hclose : ∀ q, q < 3 → applyT t q < 3) :
    ∀ n q, q < 3 → applyT (power n t) q = it (applyT t) n q := by
  intro n
  induction n using Nat.strongRecOn with
  | ind n ih =>
      intro q hq
      by_cases hz : n = 0
      · subst n
        simpa [power, hlen, it] using (applyT_range q hq)
      · have hlt : n / 2 < n := Nat.div_lt_self (by omega) (by decide)
        have ihh := ih (n / 2) hlt
        have hlen_h : (power (n / 2) t).length = 3 := by rw [power_length, hlen]
        have hclose_h : ∀ r, r < 3 → applyT (power (n / 2) t) r < 3 := by
          intro r hr
          rw [ihh r hr]
          exact it_lt (applyT t) hclose (n / 2) r hr
        unfold power
        rw [dif_neg hz]
        by_cases he : n % 2 == 0
        · rw [if_pos he]
          rw [applyT_chain _ _ _ (by simpa [hlen_h] using hq)]
          rw [ihh _ (hclose_h q hq)]
          rw [ihh q hq]
          rw [← it_add]
          have hnmod : n % 2 = 0 := by simpa using he
          exact congrArg (fun m => it (applyT t) m q) (by omega)
        · rw [if_neg he]
          rw [applyT_chain _ _ _ (by simpa [chain, hlen_h] using hq)]
          rw [applyT_chain _ _ _ (by simpa [hlen_h] using hq)]
          rw [ihh _ (hclose_h q hq)]
          rw [ihh q hq]
          rw [← it_add]
          have hstep : applyT t (it (applyT t) (n / 2 + n / 2) q) =
              it (applyT t) (n / 2 + n / 2 + 1) q := by
            have h := it_add (applyT t) 1 (n / 2 + n / 2) q
            rw [show n / 2 + n / 2 + 1 = 1 + (n / 2 + n / 2) by omega]
            exact h.symm
          rw [hstep]
          have hnmod : n % 2 ≠ 0 := by simpa using he
          exact congrArg (fun m => it (applyT t) m q) (by omega)

private theorem it_foldl (s : Bytes) :
    ∀ k q, q < 3 → it (fun q => s.foldl reader.step q) k q =
      ((List.replicate k s).flatten).foldl reader.step q := by
  intro k
  induction k with
  | zero => intro q hq; simp [it]
  | succ k ih =>
      intro q hq
      simp [it, List.replicate_succ, List.flatten_cons, List.foldl_append]
      rw [ih _ (foldl_lt s q hq)]

private theorem compile_length (x : Rope) : (compile reader x).length = 3 := by
  induction x with
  | lit s => simp [compile, reader]
  | cat a b iha ihb => simp [compile, chain, iha]
  | rep k a ih => simp [compile, ih, power_length]

/-! Compiling a rope gives a table whose `applyT` is the fold over its expansion. -/
private theorem applyT_compile (x : Rope) (q : Nat) (hq : q < 3) :
    applyT (compile reader x) q = (expand x).foldl reader.step q := by
  induction x generalizing q with
  | lit s =>
      simp [compile, applyT, reader, hq, expand]
  | cat a b iha ihb =>
      rw [compile]
      rw [applyT_chain _ _ _ (by rw [compile_length]; exact hq)]
      rw [iha q hq]
      rw [ihb _ (foldl_lt (expand a) q hq)]
      rw [expand, List.foldl_append]
  | rep k a ih =>
      rw [compile]
      have hclose_a : ∀ r, r < 3 → applyT (compile reader a) r < 3 := by
        intro r hr
        rw [ih r hr]
        exact foldl_lt (expand a) r hr
      rw [power_iter (compile reader a) (compile_length a) hclose_a k q hq]
      rw [it_congr (applyT (compile reader a)) ((expand a).foldl reader.step)
        (fun r hr => ih r hr) hclose_a k q hq]
      rw [it_foldl (expand a) k q hq]
      rfl

theorem sound_RepeatedState : SlashDepth.RepeatedState.Goal := by
  intro x n q hq
  have hq' : q < 3 := by simpa [reader] using hq
  have hclose : ∀ r, r < 3 → applyT (compile reader x) r < 3 := by
    intro r hr
    rw [applyT_compile x r hr]
    exact foldl_lt (expand x) r hr
  rw [power_iter (compile reader x) (compile_length x) hclose n q hq']
  rw [it_congr (applyT (compile reader x)) ((expand x).foldl reader.step)
    (fun r hr => applyT_compile x r hr) hclose n q hq']
  rw [it_foldl (expand x) n q hq']

end TinylambOpt.ResearchProofs.StorageExecution.Goal_GrammarScans_SlashDepth_RepeatedState
