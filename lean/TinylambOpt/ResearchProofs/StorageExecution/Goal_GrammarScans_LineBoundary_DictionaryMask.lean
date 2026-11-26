import TinylambOpt.ResearchGoals.StorageExecution.GrammarScans

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_GrammarScans_LineBoundary_DictionaryMask

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.GrammarScans
open TinylambOpt.ResearchGoals.StorageExecution.GrammarScans.LineBoundary

/-! Truth check on small dictionaries.  Every probed state satisfies the
hypothesis `q < reader.states`; `sampleIds` also contains an out-of-range code
so that the `getD false` path is exercised. -/

private def samples : List Rope :=
  [ .lit [], .lit [13], .lit [10], .lit [13, 10], .lit [65, 13, 10], .lit [10, 13],
    .cat (.lit [65, 13]) (.lit [10]), .cat (.lit [13]) (.lit [10, 65]),
    .rep 0 (.lit [13]), .rep 2 (.lit [13, 10]), .rep 3 (.lit []),
    .rep 2 (.lit [65, 13, 10]), .cat (.rep 2 (.lit [13])) (.lit [10]) ]

private def sampleIds : List Nat := [0, 1, 2, 9]

private def check : Bool :=
  (List.range reader.states).all fun q =>
    rows reader samples sampleIds q =
      sampleIds.map (fun i =>
        (samples[i]?.map (fun x => reader.accept ((expand x).foldl reader.step q))).getD false)

#eval check

example : check := by native_decide

/-! ## Generic table / power correctness (self-contained) -/

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

private theorem applyT_power_zero (t : Table) (q : Nat) (hq : q < t.length) :
    applyT (power 0 t) q = q := by
  simp [power, applyT]
  have hi : q < (List.range t.length).length := by simpa using hq
  simp [List.getElem?_eq_getElem hi]

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

/-! ## `LineBoundary.reader` specifics

Its step function always lands in one of the three states, hence every
compiled table entry is in range and every fold stays closed. -/

private theorem stepBounded : ∀ q b, q < reader.states → reader.step q b < reader.states := by
  intro q b hq
  by_cases h13 : b = 13 <;> by_cases h10 : q = 1 ∧ b = 10 <;>
    simp [reader, h13, h10]

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

/-! ## The goal -/

theorem sound_DictionaryMask : LineBoundary.DictionaryMask.Goal := by
  intro d ids q hq
  unfold rows matched
  apply List.map_congr_left
  intro i hi
  simp only [List.getElem?_map]
  cases hd : d[i]? with
  | none => rfl
  | some x => simp [hd, applyT_compile_eq_foldl x hq]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_GrammarScans_LineBoundary_DictionaryMask
