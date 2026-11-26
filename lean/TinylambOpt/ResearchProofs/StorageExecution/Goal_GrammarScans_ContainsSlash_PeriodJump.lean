import TinylambOpt.ResearchGoals.StorageExecution.GrammarScans

/-!
A compressed string scan uses grammar or chunk summaries to evaluate string predicates
without always reconstructing the full value.

Proved

- `ContainsSlash.PeriodJump` — A cycle in a run of identical phrases permits skipping
  whole periods.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_GrammarScans_ContainsSlash_PeriodJump

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.GrammarScans
open TinylambOpt.ResearchGoals.StorageExecution.GrammarScans.ContainsSlash

/-! Truth check on small inputs; inputs violating the hypothesis are ignored. -/

private def samples : List Rope :=
  [ .lit [], .lit [47], .lit [65], .lit [47, 65], .lit [65, 47],
    .cat (.lit [47]) (.lit [65]), .cat (.lit [65]) (.lit [47]),
    .rep 0 (.lit [47]), .rep 3 (.lit [65]), .rep 2 (.lit [47]),
    .rep 2 (.lit []), .cat (.rep 2 (.lit [65, 66])) (.lit [47]) ]

private def check : Bool :=
  samples.all fun x => (List.range 2).all fun q =>
    (List.range 5).all fun period =>
      period == 0 ||
        (List.range 6).all fun n =>
          applyT (power period (compile reader x)) q != q ||
            matched reader (.rep n x) q == matched reader (.rep (n % period) x) q

#eval check

example : check := by native_decide

/-! ## Basic table helpers -/

private theorem chain_length (a b : Table) : (chain a b).length = a.length := by
  simp [chain]

private theorem applyT_chain (a b : Table) (q : Nat) (hq : q < a.length) :
    applyT (chain a b) q = applyT b (applyT a q) := by
  have hq' : q < (chain a b).length := by simpa [chain_length] using hq
  simp [applyT, chain, List.getElem?_eq_getElem hq, List.getElem?_eq_getElem hq']

private theorem power_length (n : Nat) (t : Table) : (power n t).length = t.length := by
  induction n using Nat.strongRecOn with
  | ind n ih =>
      by_cases hn : n = 0
      · simp [power, hn]
      · have hdiv : n / 2 < n := Nat.div_lt_self (Nat.pos_of_ne_zero hn) (by decide)
        have ihH := ih (n / 2) hdiv
        unfold power
        by_cases he : n % 2 == 0 <;> simp [hn, ihH, chain_length, he]

/-! ## `ContainsSlash.reader` folds and phrase contents -/

private theorem foldl_reader (xs : Bytes) (q : Nat) :
    xs.foldl reader.step q = if xs.contains 47 = true then 1 else q := by
  induction xs generalizing q with
  | nil => simp
  | cons b bs ih =>
      simp only [List.foldl_cons]
      rw [ih]
      by_cases hb : b = 47
      · subst b
        simp [reader, List.contains_cons]
      · have hbe : ¬ 47 = b := by omega
        simp [reader, hb, hbe, List.contains_cons]

private theorem contains_flatten_replicate_false (s : Bytes) (hnot : 47 ∉ s) :
    ∀ n, ((List.replicate n s).flatten).contains 47 = false := by
  intro n
  induction n with
  | zero => simp
  | succ k ih =>
      rw [List.replicate_succ, List.flatten_cons, List.contains_append, ih]
      have hsf : s.contains 47 = false := by
        cases h : s.contains 47
        · rfl
        · exact (hnot ((List.contains_iff_mem).1 h)).elim
      simp [hsf, hnot]

private theorem contains_flatten_replicate (s : Bytes) (n : Nat) :
    ((List.replicate n s).flatten).contains 47 = if n = 0 then false else s.contains 47 := by
  cases n with
  | zero => simp
  | succ k =>
      rw [List.replicate_succ, List.flatten_cons, List.contains_append]
      simp only [Nat.succ_ne_zero, ↓reduceIte]
      cases hs : s.contains 47 with
      | false =>
          have hnot : 47 ∉ s := by
            intro hm
            have : s.contains 47 = true := (List.contains_iff_mem).2 hm
            rw [hs] at this
            contradiction
          rw [contains_flatten_replicate_false s hnot k]
          simp [hs]
      | true => simp [hs]

/-! ## Every compiled table is one of two concrete tables -/

private abbrev T0 : Table := [0, 1]  -- phrase is slash-free: 0 |-> 0, 1 |-> 1
private abbrev T1 : Table := [1, 1]  -- phrase has a slash:   0 |-> 1, 1 |-> 1

private theorem range_two : List.range 2 = ([0, 1] : List Nat) := by decide

private theorem power_T0 : ∀ n, power n T0 = T0 := by
  intro n
  induction n using Nat.strongRecOn with
  | ind n ih =>
      by_cases hn : n = 0
      · simp [power, hn, T0, range_two]
      · have hdiv : n / 2 < n := Nat.div_lt_self (Nat.pos_of_ne_zero hn) (by decide)
        have ihH := ih (n / 2) hdiv
        unfold power
        simp [hn, ihH, T0, chain, applyT]

private theorem power_T1 : ∀ n, power n T1 = if n = 0 then T0 else T1 := by
  intro n
  induction n using Nat.strongRecOn with
  | ind n ih =>
      by_cases hn : n = 0
      · simp [power, hn, T0, T1, range_two]
      · have hdiv : n / 2 < n := Nat.div_lt_self (Nat.pos_of_ne_zero hn) (by decide)
        have ihH := ih (n / 2) hdiv
        unfold power
        simp only [hn, dite_false]
        rw [ihH]
        by_cases hhalf : n / 2 = 0
        · have hn1 : n = 1 := by omega
          subst n
          simp [chain, applyT, T0, T1]
        · simp [hhalf, chain, applyT, T0, T1]

private theorem compile_table (x : Rope) :
    compile reader x = if (expand x).contains 47 = true then T1 else T0 := by
  induction x with
  | lit s =>
      have hstates : reader.states = 2 := rfl
      by_cases hm : 47 ∈ s <;>
        simp [compile, foldl_reader, T0, T1, hstates, expand, hm, range_two,
          List.contains_iff_mem]
  | cat a b ihA ihB =>
      rw [compile, ihA, ihB]
      simp only [expand, List.contains_append]
      cases ha : (expand a).contains 47 <;> cases hb : (expand b).contains 47 <;>
        simp [ha, hb, chain, applyT, T0, T1]
  | rep n a ihA =>
      rw [compile, ihA]
      change power n (if (expand a).contains 47 = true then T1 else T0) =
        if ((List.replicate n (expand a)).flatten).contains 47 = true then T1 else T0
      rw [contains_flatten_replicate]
      cases hca : (expand a).contains 47 with
      | false =>
          simp [hca, power_T0, T0, T1]
      | true =>
          cases n with
          | zero => simp [hca, power, T0, T1, range_two]
          | succ k => simp [hca, power_T1, T0, T1]

/-! ## Characterisation of `matched` and the goal -/

private theorem matched_char (y : Rope) (q : Nat) (hq : q < reader.states) :
    matched reader y q = ((q == 1) || (expand y).contains 47) := by
  have hq01 : q = 0 ∨ q = 1 := by simp [reader] at hq; omega
  rw [matched, compile_table y]
  rcases hq01 with rfl | rfl <;> cases h : (expand y).contains 47 <;>
    simp [h, applyT, reader, T0, T1, List.contains_iff_mem]

theorem sound_PeriodJump : ContainsSlash.PeriodJump.Goal := by
  intro x q period n hq hperiod hcycle
  have hq01 : q = 0 ∨ q = 1 := by simp [reader] at hq; omega
  rcases hq01 with rfl | rfl
  · -- From 0 a cycle back to 0 is only possible when the phrase has no slash.
    have hxnone : (expand x).contains 47 = false := by
      cases h : (expand x).contains 47
      · rfl
      · have hm : 47 ∈ expand x := (List.contains_iff_mem).1 h
        have hpne : period ≠ 0 := Nat.ne_of_gt hperiod
        rw [compile_table x] at hcycle
        simp [hm, hpne, power_T1, applyT, T0, T1] at hcycle
    have hxnot : 47 ∉ expand x := by
      intro hm
      have hb : (expand x).contains 47 = true := (List.contains_iff_mem).2 hm
      rw [hxnone] at hb
      contradiction
    have hn0 : ((List.replicate n (expand x)).flatten).contains 47 = false := by
      rw [contains_flatten_replicate]
      by_cases hzero : n = 0 <;> simp [hzero, hxnone, hxnot]
    have hn1 : ((List.replicate (n % period) (expand x)).flatten).contains 47 = false := by
      rw [contains_flatten_replicate]
      by_cases hzero : n % period = 0 <;> simp [hzero, hxnone, hxnot]
    rw [matched_char (.rep n x) 0 (by decide),
        matched_char (.rep (n % period) x) 0 (by decide)]
    change (expand (.rep n x)).contains 47 = (expand (.rep (n % period) x)).contains 47
    rw [show expand (.rep n x) = (List.replicate n (expand x)).flatten by rfl,
        show expand (.rep (n % period) x) = (List.replicate (n % period) (expand x)).flatten by rfl,
        hn0, hn1]
  · -- State 1 is absorbing, hence both sides accept.
    rw [matched_char (.rep n x) 1 (by decide),
        matched_char (.rep (n % period) x) 1 (by decide)]
    simp

end TinylambOpt.ResearchProofs.StorageExecution.Goal_GrammarScans_ContainsSlash_PeriodJump
