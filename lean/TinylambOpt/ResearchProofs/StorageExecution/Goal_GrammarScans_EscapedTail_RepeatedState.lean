import TinylambOpt.ResearchGoals.StorageExecution.GrammarScans

/-!
A compressed string scan uses grammar or chunk summaries to evaluate string predicates
without always reconstructing the full value.

Proved

- `EscapedTail.RepeatedState` — Exponentiation of the table skips arbitrarily long
  repeated strings.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_GrammarScans_EscapedTail_RepeatedState

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.GrammarScans
open TinylambOpt.ResearchGoals.StorageExecution.GrammarScans.EscapedTail

/-! Small-input truth check: literals, concatenations and repetitions, for
    several counts and both starting states. -/
private def samples : List Rope :=
  [ .lit [], .lit [92], .lit [47], .lit [92, 92], .lit [92, 47],
    .lit [47, 92], .lit [92, 47, 92], .lit [92, 92, 92],
    .cat (.lit [92]) (.lit [92]), .cat (.lit [47]) (.lit [92]),
    .rep 0 (.lit [92]), .rep 3 (.lit [92]), .rep 2 (.lit [92, 47]),
    .cat (.rep 2 (.lit [92])) (.lit [47, 92]) ]

private def check : Bool :=
  samples.all fun x => (List.range 9).all fun n => (List.range 2).all fun q =>
    applyT (power n (compile reader x)) q ==
      ((List.replicate n (expand x)).flatten).foldl reader.step q

#eval check

example : check := by native_decide

/-! A tail-recursive function iterate, used to describe repeated folds. -/
private def iter (f : Nat → Nat) : Nat → Nat → Nat
  | 0, q => q
  | n + 1, q => iter f n (f q)

private theorem iter_succ (f : Nat → Nat) (n q : Nat) :
    iter f (n + 1) q = iter f n (f q) := rfl

private theorem iter_comm (f : Nat → Nat) (n q : Nat) :
    iter f n (f q) = f (iter f n q) := by
  induction n generalizing q with
  | zero => rfl
  | succ n ih => simp only [iter]; exact ih (f q)

private theorem iter_succ_apply (f : Nat → Nat) (n q : Nat) :
    iter f (n + 1) q = f (iter f n q) := by
  rw [iter_succ, iter_comm]

private theorem iter_add (f : Nat → Nat) (a b q : Nat) :
    iter f (a + b) q = iter f a (iter f b q) := by
  induction a generalizing q with
  | zero => simp [iter]
  | succ a ih => simpa [Nat.succ_add, iter_succ, iter_comm] using ih (f q)

private theorem iter_bound (f : Nat → Nat) (hmap : ∀ r, r < 2 → f r < 2) :
    ∀ n q, q < 2 → iter f n q < 2 := by
  intro n
  induction n with
  | zero => intro q hq; simpa [iter] using hq
  | succ n ih =>
      intro q hq
      rw [iter_succ]
      exact ih (f q) (hmap q hq)

private theorem iter_congr (f g : Nat → Nat) (hmap : ∀ r, r < 2 → f r < 2)
    (heq : ∀ r, r < 2 → f r = g r) :
    ∀ n q, q < 2 → iter f n q = iter g n q := by
  intro n
  induction n with
  | zero => intro q hq; rfl
  | succ n ih =>
      intro q hq
      rw [iter_succ, iter_succ]
      simpa [heq q hq] using ih (f q) (hmap q hq)

private theorem step_bound (s : Bytes) (q : Nat) (hq : q < 2) :
    s.foldl reader.step q < 2 := by
  induction s generalizing q with
  | nil => simpa using hq
  | cons b bs ih =>
      rw [List.foldl_cons]
      apply ih
      by_cases hb : b = 92
      · simp [reader, hb]
        omega
      · simp [reader, hb]

private theorem fold_replicate (s : Bytes) (k q : Nat) :
    ((List.replicate k s).flatten).foldl reader.step q =
      iter (fun r => s.foldl reader.step r) k q := by
  induction k generalizing q with
  | zero => simp [iter]
  | succ k ih =>
      simp [List.replicate_succ, List.foldl_append, iter_succ, ih]

private theorem applyT_chain (a b : Table) (q : Nat)
    (ha : a.length = 2) (hq : q < 2) (_haq : applyT a q < 2) :
    applyT (chain a b) q = applyT b (applyT a q) := by
  have hqalen : q < a.length := by omega
  simp [chain, applyT, List.getElem?_eq_getElem hqalen]

private theorem power_zero (t : Table) : power 0 t = List.range t.length := by
  rw [power.eq_1 0 t]
  simp

private theorem power_pos (n : Nat) (t : Table) (hn : n ≠ 0) :
    power n t = let h := power (n / 2) t
                let sq := chain h h
                if n % 2 == 0 then sq else chain sq t := by
  rw [power.eq_1 n t]
  simp [hn]

/-! Fast exponentiation composes the transition on every state, as long as
    every state stays inside the (two-element) state space. -/
private theorem power_spec (t : Table) (htlen : t.length = 2)
    (htb : ∀ r, r < 2 → applyT t r < 2) :
    ∀ n q, q < 2 →
      (power n t).length = 2 ∧
        applyT (power n t) q = iter (fun r => applyT t r) n q := by
  let f : Nat → Nat := fun r => applyT t r
  have hmap : ∀ r, r < 2 → f r < 2 := by
    intro r hr; exact htb r hr
  intro n
  induction n using Nat.strongRecOn with
  | ind n ih =>
      intro q hq
      by_cases hn : n = 0
      · subst n
        rw [power_zero]
        constructor
        · simp [htlen]
        · have hqrange : q < (List.range t.length).length := by simpa [htlen] using hq
          simp [applyT, List.getElem?_eq_getElem hqrange, iter]
      · have hhalf_lt : n / 2 < n := Nat.div_lt_self (by omega) (by decide)
        have ihh := ih (n / 2) hhalf_lt
        have hhalf_len : (power (n / 2) t).length = 2 := (ihh q hq).1
        have hhalf_spec : applyT (power (n / 2) t) q = iter f (n / 2) q := (ihh q hq).2
        have hhalf_bound : ∀ r, r < 2 → applyT (power (n / 2) t) r < 2 := by
          intro r hr
          rw [(ihh r hr).2]
          exact iter_bound f hmap (n / 2) r hr
        have hsq_spec :
            applyT (chain (power (n / 2) t) (power (n / 2) t)) q =
              iter f (n / 2 + n / 2) q := by
          have hchain := applyT_chain (power (n / 2) t) (power (n / 2) t) q
              hhalf_len hq (hhalf_bound q hq)
          have hiter : iter f (n / 2) q < 2 := iter_bound f hmap (n / 2) q hq
          calc
            applyT (chain (power (n / 2) t) (power (n / 2) t)) q
                = applyT (power (n / 2) t) (applyT (power (n / 2) t) q) := hchain
            _ = applyT (power (n / 2) t) (iter f (n / 2) q) := by rw [hhalf_spec]
            _ = iter f (n / 2) (iter f (n / 2) q) := (ihh (iter f (n / 2) q) hiter).2
            _ = iter f (n / 2 + n / 2) q := by rw [iter_add]
        have hsq_len : (chain (power (n / 2) t) (power (n / 2) t)).length = 2 := by
          simp [chain, hhalf_len]
        have hsq_bound : applyT (chain (power (n / 2) t) (power (n / 2) t)) q < 2 := by
          rw [hsq_spec]
          exact iter_bound f hmap (n / 2 + n / 2) q hq
        by_cases hpar : n % 2 = 0
        · have hn_eq : n = n / 2 + n / 2 := by
            have hdiv := Nat.div_add_mod n 2
            omega
          constructor
          · rw [power_pos n t hn]
            simp [hpar, hsq_len]
          · rw [power_pos n t hn]
            simp [hpar]
            rw [hsq_spec, ← hn_eq]
        · have hpar1 : n % 2 = 1 := by
            have hlt : n % 2 < 2 := Nat.mod_lt n (by decide)
            omega
          have hn_eq : n = n / 2 + n / 2 + 1 := by
            have hdiv := Nat.div_add_mod n 2
            omega
          constructor
          · rw [power_pos n t hn]
            simp [hpar, chain, hhalf_len]
          · rw [power_pos n t hn]
            simp [hpar]
            have hchain := applyT_chain (chain (power (n / 2) t) (power (n / 2) t)) t q
                hsq_len hq hsq_bound
            rw [hchain, hsq_spec]
            conv => rhs; rw [hn_eq]
            rw [iter_succ_apply]

/-! Compiled phrases always have a two-state table whose entries are states. -/
private theorem compile_good (x : Rope) :
    (compile reader x).length = 2 ∧ ∀ q, q < 2 → applyT (compile reader x) q < 2 := by
  induction x with
  | lit s =>
      constructor
      · simp [compile, reader]
      · intro q hq
        change applyT ((List.range 2).map (fun r => s.foldl reader.step r)) q < 2
        have hq2 : q < (List.range 2).length := by simpa using hq
        simpa [applyT, List.getElem?_eq_getElem hq2] using step_bound s q hq
  | cat a b iha ihb =>
      constructor
      · simp [compile, chain, iha.1]
      · intro q hq
        have haq : applyT (compile reader a) q < 2 := iha.2 q hq
        rw [compile, applyT_chain (compile reader a) (compile reader b) q iha.1 hq haq]
        exact ihb.2 (applyT (compile reader a) q) haq
  | rep k a ih =>
      constructor
      · rw [compile]
        exact (power_spec (compile reader a) ih.1 ih.2 k 0 (by decide)).1
      · intro q hq
        rw [compile]
        have hp := (power_spec (compile reader a) ih.1 ih.2 k q hq).2
        rw [hp]
        exact iter_bound (fun r => applyT (compile reader a) r)
          (fun r hr => ih.2 r hr) k q hq

private theorem compile_spec (x : Rope) (q : Nat) (hq : q < 2) :
    applyT (compile reader x) q = (expand x).foldl reader.step q := by
  induction x generalizing q with
  | lit s =>
      have hq2 : q < (List.range reader.states).length := by
        have : reader.states = 2 := by rfl
        simpa [this] using hq
      simp [compile, applyT, expand, List.getElem?_eq_getElem hq2]
  | cat a b iha ihb =>
      have haq : applyT (compile reader a) q < 2 := (compile_good a).2 q hq
      rw [compile, expand, applyT_chain (compile reader a) (compile reader b) q
        (compile_good a).1 hq haq]
      rw [ihb (applyT (compile reader a) q) haq, iha q hq]
      rw [List.foldl_append]
  | rep k a ih =>
      rw [compile, expand]
      have hp := (power_spec (compile reader a) (compile_good a).1
        (compile_good a).2 k q hq).2
      rw [hp, fold_replicate]
      apply iter_congr
      · intro r hr
        exact (compile_good a).2 r hr
      · intro r hr
        exact ih r hr
      · exact hq

theorem sound_RepeatedState : EscapedTail.RepeatedState.Goal := by
  intro x n q hq
  have hq' : q < 2 := by simpa [reader] using hq
  have hlen : (compile reader x).length = 2 := (compile_good x).1
  have hbnd : ∀ r, r < 2 → applyT (compile reader x) r < 2 := (compile_good x).2
  rw [(power_spec (compile reader x) hlen hbnd n q hq').2, fold_replicate]
  apply iter_congr
  · intro r hr
    exact hbnd r hr
  · intro r hr
    exact compile_spec x r hr
  · exact hq'

end TinylambOpt.ResearchProofs.StorageExecution.Goal_GrammarScans_EscapedTail_RepeatedState
