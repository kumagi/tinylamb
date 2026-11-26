import TinylambOpt.ResearchGoals.StorageExecution.GrammarScans

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_GrammarScans_AsciiOnly_SearchOnce

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.GrammarScans
open TinylambOpt.ResearchGoals.StorageExecution.GrammarScans.AsciiOnly

/-! The goals module declares `SearchOnce.Goal` only inside `ContainsSlash`; there is no
    `AsciiOnly.SearchOnce.Goal`.  The statement below is that same "one accepted phrase
    stays accepted under positive repetition" shape specialized to `AsciiOnly.reader`.
    For this reader acceptance means "every byte is 7-bit ASCII", which is invariant under
    a positive number of repetitions of a phrase. -/

/-- Truth check on small inputs, respecting the hypothesis `0 < k`. -/
private def samples : List Rope :=
  [ .lit [], .lit [0], .lit [127], .lit [128], .lit [65, 128], .lit [255, 65],
    .cat (.lit [65]) (.lit [200]), .cat (.lit [200]) (.lit [65]),
    .rep 0 (.lit [128]), .rep 3 (.lit [65]), .rep 2 (.lit [128]),
    .rep 2 (.lit []), .cat (.rep 2 (.lit [65, 66])) (.lit [200]) ]

private def check : Bool :=
  samples.all fun a => (List.range 6).all fun k =>
    k == 0 || matched reader (.rep k a) 0 == matched reader a 0

#eval check

example : check := by native_decide

/-! ## Table plumbing -/

private theorem chain_length (a b : Table) : (chain a b).length = a.length := by
  simp [chain]

private theorem applyT_chain_len2 (a b : Table) (ha : a.length = 2) (_hb : b.length = 2)
    (q : Nat) (hq : q < 2) : applyT (chain a b) q = applyT b (applyT a q) := by
  have hq' : q < (chain a b).length := by simpa [chain_length, ha] using hq
  have hqa : q < a.length := by simpa [ha] using hq
  simp [applyT, chain, List.getElem?_eq_getElem hqa]

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

private theorem applyT_power_zero (t : Table) (q : Nat) (hq : q < t.length) :
    applyT (power 0 t) q = q := by
  have hq' : q < (List.range t.length).length := by simpa using hq
  simp [power, applyT, List.getElem?_eq_getElem hq']

private theorem compile_len2 (x : Rope) : (compile reader x).length = 2 := by
  induction x with
  | lit s => simp [compile, reader]
  | cat a b ihA ihB => simp [compile, chain_length, ihA]
  | rep n a ihA => simp [compile, power_length, ihA]

private theorem foldl_append_step (xs ys : Bytes) (q : Nat) :
    (xs ++ ys).foldl reader.step q = ys.foldl reader.step (xs.foldl reader.step q) := by
  induction xs generalizing q with
  | nil => simp
  | cons x xs ih => simp [ih]

private theorem replicate_add (m n : Nat) (xs : Bytes) :
    List.replicate (m + n) xs = List.replicate m xs ++ List.replicate n xs := by
  induction m with
  | zero => simp
  | succ m ih =>
      rw [Nat.succ_add]
      simp only [List.replicate_succ, List.cons_append, ih]

private theorem foldl_flatten_replicate_add (m n : Nat) (xs : Bytes) (q : Nat) :
    ((List.replicate (m + n) xs).flatten).foldl reader.step q =
      ((List.replicate n xs).flatten).foldl reader.step
        (((List.replicate m xs).flatten).foldl reader.step q) := by
  rw [replicate_add]
  rw [List.flatten_append]
  exact foldl_append_step _ _ _

/-! ## The AsciiOnly reader: state 1 absorbs, state 0 accepts. -/

private theorem step_bound (q b : Nat) (hq : q < 2) : reader.step q b < 2 := by
  by_cases hb : b < 128
  · simpa [reader, hb] using hq
  · simp [reader, hb]

private theorem foldl_bound (xs : Bytes) (q : Nat) (hq : q < 2) :
    xs.foldl reader.step q < 2 := by
  induction xs generalizing q with
  | nil => simpa
  | cons b bs ih => exact ih (reader.step q b) (step_bound q b hq)

private theorem foldl_one (xs : Bytes) : xs.foldl reader.step 1 = 1 := by
  induction xs with
  | nil => simp
  | cons b bs ih =>
      have hs : reader.step 1 b = 1 := by simp [reader]
      simpa [hs] using ih

private theorem foldl_zero (xs : Bytes) :
    xs.foldl reader.step 0 = if xs.all (fun b => b < 128) then 0 else 1 := by
  induction xs with
  | nil => simp
  | cons b bs ih =>
      by_cases hb : b < 128
      · have hs : reader.step 0 b = 0 := by simp [reader, hb]
        rw [List.foldl_cons, hs, ih]
        simp [hb]
      · have hs : reader.step 0 b = 1 := by simp [reader, hb]
        rw [List.foldl_cons, hs, foldl_one]
        simp [hb]

private theorem compile_applyT (x : Rope) (q : Nat) (hq : q < 2) :
    applyT (compile reader x) q = (expand x).foldl reader.step q := by
  induction x generalizing q with
  | lit s =>
      have hq' : q < (List.range reader.states).length := by simpa [reader] using hq
      simp [compile, applyT, List.getElem?_eq_getElem hq', expand]
  | cat a b ihA ihB =>
      have hqA : applyT (compile reader a) q < 2 := by
        rw [ihA q hq]
        exact foldl_bound (expand a) q hq
      rw [compile, applyT_chain_len2 (compile reader a) (compile reader b)
        (compile_len2 a) (compile_len2 b) q hq]
      rw [ihB _ hqA, ihA q hq]
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
              have hlen : (compile reader a).length = 2 := compile_len2 a
              have hq' : q < (compile reader a).length := by simpa [hlen] using hq
              rw [applyT_power_zero (compile reader a) q hq']
              simp
            · have hdiv : m / 2 < m := Nat.div_lt_self (Nat.pos_of_ne_zero hm) (by decide)
              have ihh := ihM (m / 2) hdiv
              have hlen_h : (power (m / 2) (compile reader a)).length = 2 := by
                rw [power_length, compile_len2]
              have hlen_sq : (chain (power (m / 2) (compile reader a))
                  (power (m / 2) (compile reader a))).length = 2 := by
                rw [chain_length, hlen_h]
              have hHq : applyT (power (m / 2) (compile reader a)) q =
                  ((List.replicate (m / 2) (expand a)).flatten).foldl reader.step q := ihh q hq
              have hHqBound : applyT (power (m / 2) (compile reader a)) q < 2 := by
                rw [hHq]
                exact foldl_bound _ q hq
              have hsq : applyT (chain (power (m / 2) (compile reader a))
                    (power (m / 2) (compile reader a))) q =
                  ((List.replicate (m / 2 + m / 2) (expand a)).flatten).foldl reader.step q := by
                rw [applyT_chain_len2 _ _ hlen_h hlen_h q hq]
                rw [ihh _ hHqBound, hHq]
                exact (foldl_flatten_replicate_add (m / 2) (m / 2) (expand a) q).symm
              have hsqBound : applyT (chain (power (m / 2) (compile reader a))
                    (power (m / 2) (compile reader a))) q < 2 := by
                rw [hsq]
                exact foldl_bound _ q hq
              unfold power
              simp only [hm, dite_false]
              by_cases heven : m % 2 == 0
              · simp [heven]
                have hmod0 : m % 2 = 0 := beq_iff_eq.mp heven
                have hm2 : m / 2 + m / 2 = m := by omega
                simpa [hm2] using hsq
              · simp [heven]
                rw [applyT_chain_len2 _ _ hlen_sq (compile_len2 a) q hq]
                rw [ihA _ hsqBound, hsq]
                have hstep :
                    (expand a).foldl reader.step
                        (((List.replicate (m / 2 + m / 2) (expand a)).flatten).foldl
                          reader.step q) =
                      ((List.replicate ((m / 2 + m / 2) + 1) (expand a)).flatten).foldl
                        reader.step q := by
                  simpa [List.replicate_one] using
                    (foldl_flatten_replicate_add (m / 2 + m / 2) 1 (expand a) q).symm
                rw [hstep]
                have hmod0 : m % 2 ≠ 0 := by
                  intro h0
                  apply heven
                  simp [h0]
                have hmod1 : m % 2 = 1 := by omega
                have hm2 : (m / 2 + m / 2) + 1 = m := by omega
                rw [hm2]
      rw [compile]
      rw [hpow n q hq]
      simp [expand]

private theorem matched_eq_all (x : Rope) :
    matched reader x 0 = (expand x).all (fun b => b < 128) := by
  rw [matched, compile_applyT x 0 (by decide : 0 < 2), foldl_zero]
  cases h : (expand x).all (fun b => b < 128) <;> simp [reader]

private theorem all_flatten_replicate (s : Bytes) {k : Nat} (hk : 0 < k) :
    ((List.replicate k s).flatten).all (fun b => b < 128) = s.all (fun b => b < 128) := by
  cases k with
  | zero => omega
  | succ k =>
      induction k with
      | zero => simp
      | succ k ih =>
          rw [List.replicate_succ]
          simp [List.flatten_cons, List.all_append, ih]

theorem sound_SearchOnce : ∀ a k, 0 < k → matched reader (.rep k a) 0 = matched reader a 0 := by
  intro a k hk
  rw [matched_eq_all, matched_eq_all]
  simpa [expand] using all_flatten_replicate (expand a) hk

end TinylambOpt.ResearchProofs.StorageExecution.Goal_GrammarScans_AsciiOnly_SearchOnce
