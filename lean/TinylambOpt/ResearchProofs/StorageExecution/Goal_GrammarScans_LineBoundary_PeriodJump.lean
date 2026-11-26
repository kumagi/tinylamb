import TinylambOpt.ResearchGoals.StorageExecution.GrammarScans

/-!
A compressed string scan uses grammar or chunk summaries to evaluate string predicates
without always reconstructing the full value.

Proved

- `LineBoundary.PeriodJump` — A cycle in a run of identical phrases permits skipping
  whole periods.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_GrammarScans_LineBoundary_PeriodJump

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.GrammarScans
open TinylambOpt.ResearchGoals.StorageExecution.GrammarScans.LineBoundary

/-! Truth check on small inputs.  Inputs whose hypothesis fails are ignored. -/

private def samples : List Rope :=
  [ .lit [], .lit [13], .lit [10], .lit [13, 10], .lit [65, 13, 10], .lit [10, 13],
    .cat (.lit [65, 13]) (.lit [10]), .cat (.lit [13]) (.lit [10, 65]),
    .rep 0 (.lit [13]), .rep 2 (.lit [13, 10]), .rep 3 (.lit []),
    .rep 2 (.lit [65, 13, 10]), .cat (.rep 2 (.lit [13])) (.lit [10]) ]

private def check : Bool :=
  samples.all fun x => (List.range 3).all fun q =>
    (List.range 1 ++ [1, 2, 3, 4, 5]).all fun period =>
      (List.range 7).all fun n =>
        applyT (power period (compile reader x)) q != q ||
          matched reader (.rep n x) q == matched reader (.rep (n % period) x) q

#eval check

example : check := by native_decide

/-! ## Generic table / power correctness (self-contained) -/

private theorem chain_length (a b : Table) : (chain a b).length = a.length := by
  simp [chain]

private theorem applyT_chain (a b : Table) (q : Nat) (hq : q < a.length) :
    applyT (chain a b) q = applyT b (applyT a q) := by
  have hq' : q < (chain a b).length := by simpa [chain_length] using hq
  simp [applyT, chain, List.getElem?_eq_getElem hq]

private theorem power_length (n : Nat) (t : Table) : (power n t).length = t.length := by
  induction n using Nat.strongRecOn with
  | _ n ih =>
      by_cases hn : n = 0
      · simp [power, hn]
      · have hpos : 0 < n := by omega
        have hhalf : (power (n / 2) t).length = t.length :=
          ih (n / 2) (Nat.div_lt_self hpos (by decide : 1 < 2))
        unfold power
        simp only [hn, dite_false]
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

/-! ## `LineBoundary.reader` specifics -/

private theorem stepBounded : ∀ q b, q < reader.states → reader.step q b < reader.states := by
  intro q b hq
  by_cases h13 : b = 13 <;> by_cases h10 : b = 10 <;>
    by_cases hq1 : q = 1 <;> simp [reader, h13, h10, hq1]

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
      simp [compile, applyT, expand, List.getElem?_eq_getElem hq']
  | cat a b ihA ihB =>
      have hqA' : (expand a).foldl reader.step q < reader.states :=
        foldl_step_bounded (expand a) hq
      rw [show compile reader (.cat a b) = chain (compile reader a) (compile reader b) by rfl]
      rw [applyT_chain (compile reader a) (compile reader b) q (by rw [compile_length]; exact hq)]
      rw [ihA hq, ihB hqA']
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
              simp [applyT_power_zero (compile reader a) q (by rw [compile_length]; exact hq)]
            · have hpos : 0 < m := by omega
              have hhalf : m / 2 < m := Nat.div_lt_self hpos (by decide : 1 < 2)
              have ihH := ihM (m / 2) hhalf
              unfold power
              simp only [hm, dite_false]
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
              · simp [heven]
                have hmzero : m % 2 = 0 := by simpa [beq_iff_eq] using heven
                have hm2 : m / 2 + m / 2 = m := by omega
                simpa [hm2] using hsq
              · simp [heven]
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

private theorem applyT_power_eq_foldl_replicate (x : Rope) (n q : Nat) (hq : q < reader.states) :
    applyT (power n (compile reader x)) q =
      ((List.replicate n (expand x)).flatten).foldl reader.step q := by
  have h := applyT_compile_eq_foldl (.rep n x) hq
  simpa [compile, expand] using h

/-! ## Period-jump argument -/

private theorem matched_eq_accept_fold (x : Rope) (n q : Nat) (hq : q < reader.states) :
    matched reader (.rep n x) q =
      reader.accept (((List.replicate n (expand x)).flatten).foldl reader.step q) := by
  rw [matched]
  change reader.accept (applyT (power n (compile reader x)) q) =
    reader.accept (((List.replicate n (expand x)).flatten).foldl reader.step q)
  rw [applyT_power_eq_foldl_replicate x n q hq]

private theorem cycle_fold (x : Rope) (q period : Nat) (hq : q < reader.states)
    (hcycle : applyT (power period (compile reader x)) q = q) :
    ((List.replicate period (expand x)).flatten).foldl reader.step q = q := by
  rw [← applyT_power_eq_foldl_replicate x period q hq]
  exact hcycle

private theorem fold_period_mul (x : Rope) (q period : Nat)
    (hcyc : ((List.replicate period (expand x)).flatten).foldl reader.step q = q) :
    ∀ k r, ((List.replicate (period * k + r) (expand x)).flatten).foldl reader.step q =
      ((List.replicate r (expand x)).flatten).foldl reader.step q := by
  intro k
  induction k with
  | zero => intro r; simp
  | succ k ih =>
      intro r
      calc
        ((List.replicate (period * (k + 1) + r) (expand x)).flatten).foldl reader.step q =
            ((List.replicate (period + (period * k + r)) (expand x)).flatten).foldl reader.step q := by
              have hsum : period * (k + 1) + r = period + (period * k + r) := by
                rw [Nat.mul_succ]
                omega
              rw [hsum]
        _ = ((List.replicate (period * k + r) (expand x)).flatten).foldl reader.step
              (((List.replicate period (expand x)).flatten).foldl reader.step q) :=
              foldl_flatten_replicate_add period (period * k + r) (expand x) q
        _ = ((List.replicate (period * k + r) (expand x)).flatten).foldl reader.step q := by rw [hcyc]
        _ = ((List.replicate r (expand x)).flatten).foldl reader.step q := ih r

theorem sound_PeriodJump : LineBoundary.PeriodJump.Goal := by
  intro x q period n hq hperiod hcycle
  have hcyc := cycle_fold x q period hq hcycle
  have hn : period * (n / period) + n % period = n := by
    have hd := Nat.div_add_mod n period
    omega
  have hfold0 := fold_period_mul x q period hcyc (n / period) (n % period)
  have hfold : ((List.replicate n (expand x)).flatten).foldl reader.step q =
      ((List.replicate (n % period) (expand x)).flatten).foldl reader.step q := by
    calc
      ((List.replicate n (expand x)).flatten).foldl reader.step q =
          ((List.replicate (period * (n / period) + n % period) (expand x)).flatten).foldl reader.step q := by
            rw [hn]
      _ = ((List.replicate (n % period) (expand x)).flatten).foldl reader.step q := hfold0
  rw [matched_eq_accept_fold x n q hq,
      matched_eq_accept_fold x (n % period) q hq]
  exact congrArg reader.accept hfold

end TinylambOpt.ResearchProofs.StorageExecution.Goal_GrammarScans_LineBoundary_PeriodJump
