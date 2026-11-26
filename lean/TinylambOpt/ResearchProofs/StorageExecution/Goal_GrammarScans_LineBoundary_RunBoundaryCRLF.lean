import TinylambOpt.ResearchGoals.StorageExecution.GrammarScans

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_GrammarScans_LineBoundary_RunBoundaryCRLF

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.GrammarScans
open TinylambOpt.ResearchGoals.StorageExecution.GrammarScans.LineBoundary

/-! ## Small-input truth check -/

private def samples : List Rope :=
  [ .lit [], .lit [13], .lit [10], .lit [65], .lit [0], .lit [255],
    .lit [13, 10], .cat (.lit []) (.lit [13]), .cat (.lit [13]) (.lit []),
    .rep 1 (.lit [10]), .rep 2 (.lit []), .cat (.rep 2 (.lit [65])) (.lit [13]) ]

private def check : Bool :=
  samples.all fun x =>
    if extent x == 1 then
      (List.range 12).all fun k =>
        if 1 < k then matched reader (.rep k x) 0 == false else true
    else true

#eval check

example : check := by native_decide

/-! ## Generic table / compile helpers -/

@[simp] theorem applyT_getD (t : Table) (q : Nat) : applyT t q = t[q]?.getD q := rfl

@[simp] theorem chain_length (a b : Table) : (chain a b).length = a.length := by
  simp [chain]

theorem applyT_chain (a b : Table) (q : Nat) (hq : q < a.length) :
    applyT (chain a b) q = applyT b (applyT a q) := by
  have hq' : q < (chain a b).length := by simpa [chain_length]
  simp [applyT, List.getElem?_eq_getElem hq, chain]

theorem compile_length (r : Reader) (x : Rope) : (compile r x).length = r.states := by
  induction x generalizing r with
  | lit s => simp [compile]
  | cat a b ihA ihB => simp [compile, chain_length, ihA]
  | rep n a ihA =>
      simp only [compile]
      induction n using Nat.strongRecOn with
      | _ n ihN =>
          unfold power
          by_cases hn : n = 0
          · simp [hn, ihA r]
          · have hdiv : n / 2 < n := Nat.div_lt_self (Nat.pos_of_ne_zero hn) (by decide)
            by_cases hmod : n % 2 = 0 <;> simp [hn, hmod, chain_length, ihN (n / 2) hdiv]

theorem power_length (n : Nat) (t : Table) : (power n t).length = t.length := by
  induction n using Nat.strongRecOn with
  | _ n ih =>
      unfold power
      by_cases hn : n = 0
      · simp [hn]
      · have hdiv : n / 2 < n := Nat.div_lt_self (Nat.pos_of_ne_zero hn) (by decide)
        by_cases hmod : n % 2 = 0 <;> simp [hn, hmod, chain_length, ih (n / 2) hdiv]

@[simp] theorem power_zero (t : Table) : power 0 t = List.range t.length := by
  unfold power
  simp

theorem applyT_power_zero (t : Table) (q : Nat) (hq : q < t.length) :
    applyT (power 0 t) q = q := by
  simp [applyT, List.getElem?_range hq]

/-! ## foldl composition -/

theorem foldl_append_step (r : Reader) (xs ys : Bytes) (q : Nat) :
    (xs ++ ys).foldl r.step q = ys.foldl r.step (xs.foldl r.step q) := by
  induction xs generalizing q with
  | nil => simp
  | cons x xs ih => simp [ih]

private theorem replicate_add (m n : Nat) (xs : Bytes) :
    List.replicate (m + n) xs = List.replicate m xs ++ List.replicate n xs := by
  induction m with
  | zero => simp
  | succ m ih =>
      calc
        List.replicate (Nat.succ m + n) xs
            = xs :: List.replicate (m + n) xs := by rw [Nat.succ_add, List.replicate_succ]
        _ = xs :: (List.replicate m xs ++ List.replicate n xs) := by rw [ih]
        _ = List.replicate (Nat.succ m) xs ++ List.replicate n xs := by
              rw [List.replicate_succ]
              simp

theorem foldl_flatten_replicate_succ (r : Reader) (n : Nat) (xs : Bytes) (q : Nat) :
    ((List.replicate (n + 1) xs).flatten).foldl r.step q =
      xs.foldl r.step (((List.replicate n xs).flatten).foldl r.step q) := by
  rw [replicate_add n 1 xs, List.flatten_append]
  simp

theorem foldl_flatten_replicate_add (r : Reader) (m n : Nat) (xs : Bytes) (q : Nat) :
    ((List.replicate (m + n) xs).flatten).foldl r.step q =
      ((List.replicate n xs).flatten).foldl r.step (((List.replicate m xs).flatten).foldl r.step q) := by
  rw [replicate_add m n xs, List.flatten_append]
  exact foldl_append_step r ((List.replicate m xs).flatten) ((List.replicate n xs).flatten) q

/-! ## Step boundedness and compile correctness -/

def StepBounded (r : Reader) : Prop := ∀ q b, q < r.states → r.step q b < r.states

theorem foldl_step_bounded {r : Reader} (hb : StepBounded r) (xs : Bytes) {q : Nat} (hq : q < r.states) :
    xs.foldl r.step q < r.states := by
  induction xs generalizing q with
  | nil => simpa
  | cons x xs ih => exact ih (hb q x hq)

theorem applyT_compile_eq_foldl {r : Reader} (hb : StepBounded r) (x : Rope) {q : Nat} (hq : q < r.states) :
    applyT (compile r x) q = (expand x).foldl r.step q := by
  induction x generalizing q with
  | lit s =>
      have hq' : q < (List.range r.states).length := by simpa using hq
      simp [compile, applyT, List.getElem?_eq_getElem hq', expand]
  | cat a b ihA ihB =>
      have hqA : applyT (compile r a) q < r.states := by
        rw [ihA hq]; exact foldl_step_bounded hb _ hq
      rw [compile, applyT_chain (compile r a) (compile r b) q (by simpa [compile_length] using hq)]
      rw [ihA hq] at hqA ⊢
      rw [ihB hqA]
      simp [expand]
  | rep n a ihA =>
      have hpow : ∀ m q, q < r.states →
          applyT (power m (compile r a)) q = ((List.replicate m (expand a)).flatten).foldl r.step q := by
        intro m
        induction m using Nat.strongRecOn with
        | _ m ihM =>
            intro q hq
            by_cases hm : m = 0
            · subst m
              have hq' : q < (compile r a).length := by simpa [compile_length] using hq
              simp [applyT, List.getElem?_range hq']
            · have hdiv : m / 2 < m := Nat.div_lt_self (Nat.pos_of_ne_zero hm) (by decide)
              unfold power
              simp only [hm, dite_false]
              have hHalf : applyT (power (m / 2) (compile r a)) q =
                  ((List.replicate (m / 2) (expand a)).flatten).foldl r.step q :=
                ihM (m / 2) hdiv q hq
              have hHalfBound : ((List.replicate (m / 2) (expand a)).flatten).foldl r.step q < r.states :=
                foldl_step_bounded hb _ hq
              have hSq : applyT (chain (power (m / 2) (compile r a)) (power (m / 2) (compile r a))) q =
                  ((List.replicate (m / 2 + m / 2) (expand a)).flatten).foldl r.step q := by
                rw [applyT_chain _ _ q (by rw [power_length, compile_length]; exact hq)]
                rw [hHalf, ihM (m / 2) hdiv _ hHalfBound, foldl_flatten_replicate_add]
              have hSqBound : ((List.replicate (m / 2 + m / 2) (expand a)).flatten).foldl r.step q < r.states :=
                foldl_step_bounded hb _ hq
              cases hmod : m % 2 with
              | zero =>
                  have hm2 : m / 2 + m / 2 = m := by omega
                  change applyT (chain (power (m / 2) (compile r a)) (power (m / 2) (compile r a))) q =
                    ((List.replicate m (expand a)).flatten).foldl r.step q
                  rw [hSq, hm2]
              | succ k =>
                  have hk : k = 0 := by omega
                  subst k
                  have hm2 : m / 2 + m / 2 + 1 = m := by omega
                  change applyT (chain (chain (power (m / 2) (compile r a)) (power (m / 2) (compile r a))) (compile r a)) q =
                    ((List.replicate m (expand a)).flatten).foldl r.step q
                  rw [applyT_chain _ _ q (by rw [chain_length, power_length, compile_length]; exact hq)]
                  rw [hSq, ihA hSqBound]
                  rw [← foldl_flatten_replicate_succ r (m / 2 + m / 2) (expand a) q]
                  simp [hm2]
      simpa [compile, expand] using (hpow n q hq)

@[simp] theorem expand_length (x : Rope) : (expand x).length = extent x := by
  induction x with
  | lit s => simp [expand, extent]
  | cat a b ihA ihB => simp [expand, extent, ihA, ihB]
  | rep k a ihA =>
      simp [expand, extent, ihA]

theorem matched_eq_foldl {r : Reader} (hb : StepBounded r) (x : Rope) {q : Nat} (hq : q < r.states) :
    matched r x q = r.accept ((expand x).foldl r.step q) := by
  simp only [matched]
  rw [show applyT (compile r x) q = (expand x).foldl r.step q from applyT_compile_eq_foldl hb x hq]

/-! ## StepBounded for LineBoundary -/

theorem LineBoundary_stepBounded : StepBounded reader := by
  intro q b hq
  simp only [reader]
  split
  · omega
  · split
    · omega
    · omega

/-! ## LineBoundary specifics -/

private theorem step_step_ne_two (b : Nat) (q : Nat) : reader.step (reader.step q b) b ≠ 2 := by
  simp only [reader]
  by_cases h13 : b = 13
  · have h13' : (b == 13) = true := by simpa using h13
    simp [h13']
  · have h13' : (b == 13) = false := by simpa using h13
    simp [h13']
    intro h
    by_cases hq10 : q = 1 ∧ b = 10 <;> simp [hq10] at h

private theorem foldl_rep_one (b : Nat) (k : Nat) (hk : 1 < k) (q : Nat) :
    (List.replicate k b).foldl reader.step q ≠ 2 := by
  induction k generalizing q with
  | zero => omega
  | succ k ih =>
      cases k with
      | zero => omega
      | succ k =>
          cases k with
          | zero =>
              simp [List.replicate_succ]
              exact step_step_ne_two b q
          | succ k =>
              have ih' := ih (by omega) (reader.step q b)
              simp only [List.replicate_succ, List.foldl_cons]
              exact ih'

theorem sound_RunBoundaryCRLF : LineBoundary.RunBoundaryCRLF.Goal := by
  intro x k hk hlen
  have hq : (0 : Nat) < reader.states := by decide
  rw [matched_eq_foldl LineBoundary_stepBounded (.rep k x) hq]
  have hexpLen : (expand x).length = 1 := by simpa [expand_length] using hlen
  obtain ⟨b, hb⟩ : ∃ b, expand x = [b] := by
    match h : expand x with
    | [] => simp [h] at hexpLen
    | [b] => exact ⟨b, rfl⟩
    | b1 :: b2 :: rest => simp [h] at hexpLen
  simp only [expand, hb]
  have hne : (List.replicate k b).foldl reader.step 0 ≠ 2 := foldl_rep_one b k hk 0
  simpa [reader, beq_eq_false_iff_ne] using hne

end TinylambOpt.ResearchProofs.StorageExecution.Goal_GrammarScans_LineBoundary_RunBoundaryCRLF
