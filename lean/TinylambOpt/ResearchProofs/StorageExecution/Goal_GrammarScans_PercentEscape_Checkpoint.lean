import TinylambOpt.ResearchGoals.StorageExecution.GrammarScans

/-!
A compressed string scan uses grammar or chunk summaries to evaluate string predicates
without always reconstructing the full value.

Proved

- `PercentEscape.Checkpoint` — A saved state at a prefix resumes scanning across
  further phrase boundaries.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_GrammarScans_PercentEscape_Checkpoint

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.GrammarScans
open TinylambOpt.ResearchGoals.StorageExecution.GrammarScans.PercentEscape

/-! Truth check on small inputs (all with q < reader.states = 4). -/

private structure Sample where
  rope : Rope
  q : Nat
  a : Nat
  b : Nat

private def samples : List Sample :=
  [ { rope := .lit [], q := 0, a := 0, b := 0 }
  , { rope := .lit [37], q := 0, a := 0, b := 1 }
  , { rope := .lit [37, 52, 70], q := 1, a := 1, b := 2 }
  , { rope := .lit [37, 52, 70, 37, 65, 70], q := 2, a := 2, b := 4 }
  , { rope := .cat (.lit [37]) (.lit [52, 70]), q := 0, a := 1, b := 2 }
  , { rope := .cat (.lit [37, 52]) (.lit [70, 37]), q := 1, a := 0, b := 4 }
  , { rope := .rep 0 (.lit [37, 52, 70]), q := 3, a := 0, b := 2 }
  , { rope := .rep 3 (.lit [37, 52, 70]), q := 0, a := 2, b := 5 }
  , { rope := .rep 2 (.lit []), q := 2, a := 1, b := 3 }
  , { rope := .cat (.rep 2 (.lit [37, 52, 70])) (.lit [37, 70]), q := 1, a := 4, b := 4 }
  ]

private def sampleOK (s : Sample) : Bool :=
  decide (s.q < reader.states && partialScan reader s.rope (s.a + s.b) s.q ==
    (((expand s.rope).drop s.a).take s.b).foldl reader.step (partialScan reader s.rope s.a s.q))

private def check : Bool := samples.all sampleOK

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

/-! ## `PercentEscape.reader` specifics -/

private theorem stepBounded : ∀ q b, q < reader.states → reader.step q b < reader.states := by
  intro q b hq
  have hq4 : q < 4 := by simpa [reader] using hq
  simp [reader]
  by_cases h37 : b = 37
  · simp [h37]
  · by_cases hhex : (48 ≤ b ∧ b ≤ 57 ∨ 65 ≤ b ∧ b ≤ 70) ∨ 97 ≤ b ∧ b ≤ 102
    · by_cases hq12 : q = 1 ∨ q = 2
      · simp [h37, hhex, hq12]
        omega
      · simp [h37, hhex, hq12]
    · simp [h37, hhex]

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

private theorem applyT_power_eq_foldl_replicate (x : Rope) (n q : Nat) (hq : q < reader.states) :
    applyT (power n (compile reader x)) q =
      ((List.replicate n (expand x)).flatten).foldl reader.step q := by
  have h := applyT_compile_eq_foldl (.rep n x) hq
  simpa [compile, expand] using h

/-! ## Prefix scanning agrees with folding over the taken prefix -/

private theorem gc_sum_replicate_nat (n m : Nat) :
    (List.replicate n m).sum = n * m := by
  induction n with
  | zero => simp
  | succ n ih => simp [List.replicate_succ, ih, Nat.succ_mul, Nat.add_comm]

private theorem gc_length_expand (x : Rope) : extent x = (expand x).length := by
  induction x with
  | lit s => rfl
  | cat a b iha ihb => simp [extent, expand, iha, ihb]
  | rep k a iha =>
      simp only [extent, expand, List.length_flatten, List.map_replicate,
        gc_sum_replicate_nat, iha]

private theorem gc_flatten_replicate_add (a b : Nat) (s : Bytes) :
    (List.replicate (a + b) s).flatten =
      (List.replicate a s).flatten ++ (List.replicate b s).flatten := by
  induction a with
  | zero => simp
  | succ a ih =>
      rw [Nat.succ_add, List.replicate_succ, List.flatten_cons, ih,
        List.replicate_succ, List.flatten_cons]
      simp [List.append_assoc]

private theorem gc_take_flatten_replicate_part (k n : Nat) (s : List Nat)
    (hs : 0 < s.length) (hlt : n < k * s.length) :
    ((List.replicate k s).flatten).take n =
      (List.replicate (n / s.length) s).flatten ++ s.take (n % s.length) := by
  induction k using Nat.strongRecOn generalizing n with
  | ind k ih =>
      cases k with
      | zero => simp at hlt
      | succ k =>
          rw [List.replicate_succ, List.flatten_cons, List.take_append]
          by_cases hn : n ≤ s.length
          · by_cases heq : n = s.length
            · subst n
              rw [Nat.sub_self, List.take_zero, List.append_nil,
                Nat.div_self hs, Nat.mod_self s.length]
              simp [List.replicate_succ, List.flatten_cons]
            · have hltn : n < s.length := by omega
              rw [Nat.sub_eq_zero_of_le hn, List.take_zero, List.append_nil,
                Nat.div_eq_of_lt hltn, Nat.mod_eq_of_lt hltn]
              simp
          · have hgt : s.length < n := by omega
            have hsub : n - s.length < k * s.length := by
              rw [Nat.succ_mul] at hlt
              omega
            have ih' := ih k (Nat.lt_succ_self k) (n - s.length) hsub
            rw [ih']
            have hrec : n = s.length + (n - s.length) := by omega
            have hdiv : n / s.length = (n - s.length) / s.length + 1 :=
              (congrArg (fun t => t / s.length) hrec).trans
                (Nat.add_div_left (n - s.length) hs)
            have hmod : n % s.length = (n - s.length) % s.length :=
              (congrArg (fun t => t % s.length) hrec).trans
                (Nat.add_mod_left s.length (n - s.length))
            rw [hdiv, hmod, List.replicate_succ, List.flatten_cons]
            simp [List.append_assoc, List.take_of_length_le (Nat.le_of_lt hgt)]

private theorem prefixSeek_pe : ∀ x q n, q < reader.states →
    partialScan reader x n q = ((expand x).take n).foldl reader.step q := by
  intro x q n hq
  induction x generalizing q n with
  | lit s => rfl
  | cat a b iha ihb =>
      rw [partialScan]
      by_cases hn : n ≤ extent a
      · rw [if_pos hn, iha q n hq]
        rw [expand, List.take_append_of_le_length (by simpa [gc_length_expand] using hn)]
      · rw [if_neg hn, applyT_compile_eq_foldl a hq]
        rw [expand, List.take_append,
          List.take_of_length_le (by simpa [gc_length_expand] using Nat.le_of_not_ge hn),
          List.foldl_append]
        simpa [gc_length_expand] using
          ihb _ (n - extent a) (foldl_step_bounded (expand a) hq)
  | rep k a iha =>
      rw [partialScan]
      by_cases hs : extent a = 0
      · rw [if_pos (by simp [hs])]
        have hlen : (expand a).length = 0 := by rw [← gc_length_expand, hs]
        have hempty : expand a = [] := List.length_eq_zero_iff.mp hlen
        change q = (((List.replicate k (expand a)).flatten).take n).foldl reader.step q
        rw [hempty]
        simp
      · rw [if_neg (by simp [hs])]
        have hs0 : 0 < extent a := Nat.pos_of_ne_zero hs
        by_cases hfull : min k (n / extent a) = k
        · rw [if_pos (by simpa using hfull), hfull]
          have hkdiv : k ≤ n / extent a := by
            cases Nat.le_total k (n / extent a) with
            | inl h => exact h
            | inr h =>
                have hmin := Nat.min_eq_right h
                omega
          have hnge : k * extent a ≤ n := by
            rw [Nat.mul_comm k (extent a)]
            have hmul := Nat.mul_le_mul_left (extent a) hkdiv
            have hdivle := Nat.mul_div_le n (extent a)
            omega
          rw [applyT_power_eq_foldl_replicate a k q hq]
          rw [List.take_of_length_le (by
            rw [← gc_length_expand (Rope.rep k a)]
            simp only [extent, List.length_flatten, List.map_replicate, gc_sum_replicate_nat]
            exact hnge)]
          rfl
        · rw [if_neg (by simpa using hfull)]
          have hnk : n / extent a < k := by
            cases Nat.le_total (n / extent a) k with
            | inl h =>
                have hmin := Nat.min_eq_right h
                omega
            | inr h =>
                have hmin := Nat.min_eq_left h
                exact False.elim (hfull hmin)
          have hnlt : n < k * extent a := by
            have hd := Nat.div_add_mod n (extent a)
            have hmod := Nat.mod_lt n hs0
            have hsucc := Nat.succ_le_of_lt hnk
            have hmul := Nat.mul_le_mul_left (extent a) hsucc
            calc n = extent a * (n / extent a) + n % extent a := hd.symm
              _ < extent a * (n / extent a) + extent a := Nat.add_lt_add_left hmod _
              _ = extent a * (n / extent a).succ := by rw [Nat.mul_succ]
              _ ≤ extent a * k := hmul
              _ = k * extent a := Nat.mul_comm _ _
          rw [Nat.min_eq_right (Nat.le_of_lt hnk)]
          change partialScan reader a (n % extent a)
            (applyT (power (n / extent a) (compile reader a)) q) = _
          rw [applyT_power_eq_foldl_replicate a (n / extent a) q hq, expand]
          rw [gc_take_flatten_replicate_part k n (expand a)
            (by simpa [gc_length_expand] using hs0) (by simpa [gc_length_expand] using hnlt)]
          rw [← gc_length_expand a, List.foldl_append]
          exact iha _ (n % extent a) (foldl_step_bounded
            ((List.replicate (n / extent a) (expand a)).flatten) hq)

private theorem take_add_eq_append (xs : List Nat) (a b : Nat) :
    xs.take (a + b) = xs.take a ++ (xs.drop a).take b := by
  induction xs generalizing a b with
  | nil => simp
  | cons x xs ih =>
      cases a with
      | zero => simp
      | succ a => simp [Nat.succ_add, ih]

/-! ## The checkpoint theorem -/

theorem sound_Checkpoint : PercentEscape.Checkpoint.Goal := by
  intro x q a b hq
  rw [prefixSeek_pe x q (a + b) hq, prefixSeek_pe x q a hq]
  rw [take_add_eq_append (expand x) a b, foldl_append_step]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_GrammarScans_PercentEscape_Checkpoint
