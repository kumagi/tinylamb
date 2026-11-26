import TinylambOpt.ResearchGoals.StorageExecution.GrammarScans

/-!
A compressed string scan uses grammar or chunk summaries to evaluate string predicates
without always reconstructing the full value.

Proved

- `ContainsSlash.SearchOnce` — `∀ a k, 0<k → matched reader (.rep k a) 0 = matched
  reader a 0`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_GrammarScans_ContainsSlash_SearchOnce

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.GrammarScans
open TinylambOpt.ResearchGoals.StorageExecution.GrammarScans.ContainsSlash

/-! Truth check on small inputs (the hypothesis `0 < k` is respected). -/

private def samples : List Rope :=
  [ .lit [], .lit [47], .lit [65], .lit [47, 65], .lit [65, 47],
    .cat (.lit [47]) (.lit [65]), .cat (.lit [65]) (.lit [47]),
    .rep 0 (.lit [47]), .rep 3 (.lit [65]), .rep 2 (.lit [47]),
    .rep 2 (.lit []), .cat (.rep 2 (.lit [65, 66])) (.lit [47]) ]

private def check : Bool :=
  samples.all fun a => (List.range 5).all fun k =>
    k == 0 || matched reader (.rep k a) 0 == matched reader a 0

#eval check

example : check := by native_decide

/-! ## Two-state table plumbing -/

private theorem chain_length (a b : Table) : (chain a b).length = a.length := by
  simp [chain]

private theorem applyT_chain_len2 (a b : Table) (ha : a.length = 2) (_hb : b.length = 2)
    (q : Nat) (hq : q < 2) : applyT (chain a b) q = applyT b (applyT a q) := by
  have hq' : q < (chain a b).length := by simpa [chain_length, ha] using hq
  have hqa : q < a.length := by simpa [ha] using hq
  simp [applyT, chain, List.getElem?_eq_getElem hq', List.getElem?_eq_getElem hqa]

private theorem power_length (n : Nat) (t : Table) : (power n t).length = t.length := by
  induction n using Nat.strongRecOn with
  | _ n ih =>
      by_cases hn : n = 0
      · simp [power, hn]
      · have hdiv : n / 2 < n := Nat.div_lt_self (Nat.pos_of_ne_zero hn) (by decide)
        have ihh := ih (n / 2) hdiv
        unfold power
        simp only [hn, dite_false]
        split <;> simp [chain_length, ihh]

private theorem compile_len2 (x : Rope) : (compile reader x).length = 2 := by
  induction x with
  | lit s => simp [compile, reader]
  | cat a b ihA ihB => simp [compile, chain_length, ihA, ihB]
  | rep n a ihA => simp [compile, power_length, ihA]

private theorem foldl_append_step (r : Reader) (xs ys : Bytes) (q : Nat) :
    (xs ++ ys).foldl r.step q = ys.foldl r.step (xs.foldl r.step q) := by
  induction xs generalizing q with
  | nil => simp
  | cons x xs ih => simp [ih]

private theorem foldl_flatten_replicate_succ (r : Reader) (n : Nat) (xs : Bytes) (q : Nat) :
    ((List.replicate (n + 1) xs).flatten).foldl r.step q =
      ((List.replicate n xs).flatten).foldl r.step (xs.foldl r.step q) := by
  rw [show List.replicate (n + 1) xs = [xs] ++ List.replicate n xs by
    simp [List.replicate_succ]]
  simp [List.flatten_append, foldl_append_step]

private theorem foldl_flatten_replicate_comm (r : Reader) (n : Nat) (xs : Bytes) (q : Nat) :
    ((List.replicate n xs).flatten).foldl r.step (xs.foldl r.step q) =
      xs.foldl r.step (((List.replicate n xs).flatten).foldl r.step q) := by
  induction n generalizing q with
  | zero => simp
  | succ n ih => simp [foldl_flatten_replicate_succ, ih]

private theorem foldl_flatten_replicate_add (r : Reader) (m n : Nat) (xs : Bytes) (q : Nat) :
    ((List.replicate (m + n) xs).flatten).foldl r.step q =
      ((List.replicate n xs).flatten).foldl r.step (((List.replicate m xs).flatten).foldl r.step q) := by
  induction n generalizing q with
  | zero => simp
  | succ n ih =>
      calc
        ((List.replicate (m + (n + 1)) xs).flatten).foldl r.step q =
            ((List.replicate ((m + n) + 1) xs).flatten).foldl r.step q := by
              rw [show m + (n + 1) = (m + n) + 1 by omega]
        _ = ((List.replicate (m + n) xs).flatten).foldl r.step (xs.foldl r.step q) := by
              rw [foldl_flatten_replicate_succ]
        _ = ((List.replicate n xs).flatten).foldl r.step
              (((List.replicate m xs).flatten).foldl r.step (xs.foldl r.step q)) := by
              rw [ih]
        _ = ((List.replicate n xs).flatten).foldl r.step
              (xs.foldl r.step (((List.replicate m xs).flatten).foldl r.step q)) := by
              rw [foldl_flatten_replicate_comm]
        _ = ((List.replicate (n + 1) xs).flatten).foldl r.step
              (((List.replicate m xs).flatten).foldl r.step q) := by
              rw [foldl_flatten_replicate_succ]

/-! ## The slash reader: folding a byte string remembers only "seen a slash". -/

private theorem foldl_reader (xs : Bytes) (q : Nat) :
    xs.foldl reader.step q = if xs.contains 47 then 1 else q := by
  induction xs generalizing q with
  | nil => simp
  | cons b bs ih =>
      simp only [List.foldl_cons, List.contains_cons]
      by_cases hb : b == 47
      · have hb' : b = 47 := by simpa using hb
        subst b
        have h1 : bs.foldl reader.step 1 = 1 := by
          clear ih
          induction bs with
          | nil => simp
          | cons c cs ih2 =>
              simp only [List.foldl_cons]
              have : reader.step 1 c = 1 := by simp [reader]
              rw [this, ih2]
        simpa [reader] using h1
      · have hb' : b ≠ 47 := by simpa using hb
        have hstep : reader.step q b = q := by simp [reader, hb']
        rw [hstep, ih]
        have hb47 : (47 == b) = false := by simpa using (show 47 ≠ b by omega)
        simp [hb47]

private theorem fold_bound (xs : Bytes) (q : Nat) (hq : q < 2) :
    xs.foldl reader.step q < 2 := by
  rw [foldl_reader]
  split <;> omega

/-! ## `compile` agrees with folding for every start state below 2. -/

private theorem compile_applyT (x : Rope) (q : Nat) (hq : q < 2) :
    applyT (compile reader x) q = (expand x).foldl reader.step q := by
  induction x generalizing q with
  | lit s =>
      simp [compile, applyT, foldl_reader, expand, reader] at hq ⊢
      have hq' : q < (List.range 2).length := by simpa using hq
      have hq1 : (List.range 2)[q]? = some q := by
        rw [List.getElem?_eq_getElem hq']
        simp
      rw [hq1]
      simp
  | cat a b ihA ihB =>
      rw [compile, applyT_chain_len2 (compile reader a) (compile reader b)
        (compile_len2 a) (compile_len2 b) q hq]
      rw [ihA q hq]
      have hqA : (expand a).foldl reader.step q < 2 := fold_bound _ q hq
      rw [ihB _ hqA]
      simp [expand, foldl_append_step]
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
              have hlen : (compile reader a).length = 2 := compile_len2 a
              have hq' : q < (List.range 2).length := by simpa using hq
              simp [power, applyT, hlen, List.getElem?_eq_getElem hq']
            · have hdiv : m / 2 < m := Nat.div_lt_self (Nat.pos_of_ne_zero hm) (by decide)
              have ihh := ihM (m / 2) hdiv
              unfold power
              simp only [hm, dite_false]
              let h := power (m / 2) (compile reader a)
              change applyT (if (m % 2 == 0) = true then chain h h
                             else chain (chain h h) (compile reader a)) q =
                ((List.replicate m (expand a)).flatten).foldl reader.step q
              have hlen_h : h.length = 2 := by simp [h, power_length, compile_len2]
              have hlen_sq : (chain h h).length = 2 := by simp [chain_length, hlen_h]
              have hHq : applyT h q =
                  ((List.replicate (m / 2) (expand a)).flatten).foldl reader.step q := by
                simpa [h] using ihh q hq
              have hHqBound : applyT h q < 2 := by
                rw [hHq]
                exact fold_bound _ q hq
              have hsq : applyT (chain h h) q =
                  ((List.replicate (m / 2 + m / 2) (expand a)).flatten).foldl reader.step q := by
                calc
                  applyT (chain h h) q = applyT h (applyT h q) := by
                    rw [applyT_chain_len2 h h hlen_h hlen_h q hq]
                  _ = ((List.replicate (m / 2) (expand a)).flatten).foldl reader.step
                        (((List.replicate (m / 2) (expand a)).flatten).foldl reader.step q) := by
                      have hih2 : applyT h
                          (((List.replicate (m / 2) (expand a)).flatten).foldl reader.step q) =
                          ((List.replicate (m / 2) (expand a)).flatten).foldl reader.step
                            (((List.replicate (m / 2) (expand a)).flatten).foldl reader.step q) := by
                        have hbnd : ((List.replicate (m / 2) (expand a)).flatten).foldl reader.step q < 2 :=
                          fold_bound _ q hq
                        simpa [h] using ihh (((List.replicate (m / 2) (expand a)).flatten).foldl reader.step q) hbnd
                      rw [hHq, hih2]
                  _ = ((List.replicate (m / 2 + m / 2) (expand a)).flatten).foldl reader.step q := by
                      rw [← foldl_flatten_replicate_add reader (m / 2) (m / 2) (expand a) q]
              cases hmod : m % 2 with
              | zero =>
                  have hm2 : m / 2 + m / 2 = m := by omega
                  simp [hmod]
                  simpa [hm2] using hsq
              | succ k =>
                  have hk : k = 0 := by omega
                  subst hk
                  simp [hmod]
                  have hm2 : m / 2 + m / 2 + 1 = m := by omega
                  have hfoldBound :
                      ((List.replicate (m / 2 + m / 2) (expand a)).flatten).foldl reader.step q < 2 :=
                    fold_bound _ q hq
                  rw [applyT_chain_len2 (chain h h) (compile reader a)
                    hlen_sq (compile_len2 a) q hq]
                  rw [hsq, ihA _ hfoldBound]
                  calc
                    (expand a).foldl reader.step
                        (((List.replicate (m / 2 + m / 2) (expand a)).flatten).foldl reader.step q)
                        = ((List.replicate (m / 2 + m / 2) (expand a)).flatten).foldl reader.step
                            ((expand a).foldl reader.step q) := by
                          rw [← foldl_flatten_replicate_comm reader (m / 2 + m / 2) (expand a) q]
                    _ = ((List.replicate ((m / 2 + m / 2) + 1) (expand a)).flatten).foldl
                            reader.step q := by
                          rw [foldl_flatten_replicate_succ]
                    _ = ((List.replicate m (expand a)).flatten).foldl reader.step q := by
                          rw [hm2]
      rw [compile]
      rw [hpow n q hq]
      simp [expand]

private theorem matched_eq_contains (x : Rope) :
    matched reader x 0 = (expand x).contains 47 := by
  rw [matched, compile_applyT x 0 (by decide : 0 < 2), foldl_reader]
  cases h : (expand x).contains 47 <;> simp [h, reader]

private theorem contains_flatten_replicate (s : Bytes) {k : Nat} (hk : 0 < k) :
    ((List.replicate k s).flatten).contains 47 = s.contains 47 := by
  cases k with
  | zero => omega
  | succ k =>
      rw [List.replicate_succ, List.flatten_cons]
      cases h : s.contains 47 with
      | false =>
          have hnone : 47 ∉ s := by simpa [List.contains_iff_mem] using h
          have hnot : 47 ∉ (s ++ (List.replicate k s).flatten) := by
            intro hm
            simp only [List.mem_append, List.mem_singleton] at hm
            rcases hm with hm | hm
            · exact hnone hm
            · rw [List.mem_flatten] at hm
              rcases hm with ⟨l, hl, h47⟩
              rw [List.mem_replicate] at hl
              exact hnone (hl.2 ▸ h47)
          cases hc : (s ++ (List.replicate k s).flatten).contains 47 with
          | false => rfl
          | true => exact (hnot (by simpa using hc)).elim
      | true =>
          have hmem : 47 ∈ s := by simpa [List.contains_iff_mem] using h
          have hc : (s ++ (List.replicate k s).flatten).contains 47 = true := by
            simpa [List.contains_iff_mem] using
              (show 47 ∈ s ++ (List.replicate k s).flatten by simp [hmem])
          rw [hc]

theorem sound_SearchOnce : ContainsSlash.SearchOnce.Goal := by
  intro a k hk
  rw [matched_eq_contains, matched_eq_contains]
  simp only [expand]
  exact contains_flatten_replicate (expand a) hk

end TinylambOpt.ResearchProofs.StorageExecution.Goal_GrammarScans_ContainsSlash_SearchOnce
