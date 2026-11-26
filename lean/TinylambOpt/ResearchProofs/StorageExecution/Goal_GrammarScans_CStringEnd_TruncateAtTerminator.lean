import TinylambOpt.ResearchGoals.StorageExecution.GrammarScans

/-!
A compressed string scan uses grammar or chunk summaries to evaluate string predicates
without always reconstructing the full value.

Proved

- `CStringEnd.TruncateAtTerminator` — `∀ pre tail, !(expand pre).contains 0 → matched
  reader (.cat (.cat pre (.lit [0])) tail) 0 = decide (extent tail=0)`
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_GrammarScans_CStringEnd_TruncateAtTerminator

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.GrammarScans
open TinylambOpt.ResearchGoals.StorageExecution.GrammarScans.CStringEnd

/-! ## Small-input truth check -/

private def samples : List (Rope × Rope) :=
  [ (.lit [], .lit []), (.lit [], .lit [65]), (.lit [65, 66], .lit []),
    (.lit [65], .lit [0]), (.rep 2 (.lit [66, 67]), .lit []),
    (.cat (.lit [65]) (.rep 3 (.lit [66])), .lit [0, 65]),
    (.rep 0 (.lit [0]), .lit []), (.lit [255, 1], .cat (.lit []) (.lit [13])) ]

private def check : Bool :=
  samples.all fun p =>
    if !(expand p.1).contains 0 then
      matched reader (.cat (.cat p.1 (.lit [0])) p.2) 0 == decide (extent p.2 = 0)
    else true

#eval check

example : check := by native_decide

/-! ## Generic table / compile helpers -/

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

/-! ## foldl composition -/

theorem foldl_append_step (r : Reader) (xs ys : Bytes) (q : Nat) :
    (xs ++ ys).foldl r.step q = ys.foldl r.step (xs.foldl r.step q) := by
  induction xs generalizing q with
  | nil => simp
  | cons x xs ih => simp [ih]

private theorem replicate_succ_append (n : Nat) (xs : Bytes) :
    List.replicate (n + 1) xs = List.replicate n xs ++ [xs] := by
  induction n with
  | zero => simp
  | succ n ih =>
      simpa [List.replicate_succ, ih]

theorem foldl_flatten_replicate_succ (r : Reader) (n : Nat) (xs : Bytes) (q : Nat) :
    ((List.replicate (n + 1) xs).flatten).foldl r.step q =
      xs.foldl r.step (((List.replicate n xs).flatten).foldl r.step q) := by
  rw [replicate_succ_append, List.flatten_append]
  simp

theorem foldl_flatten_replicate_add (r : Reader) (m n : Nat) (xs : Bytes) (q : Nat) :
    ((List.replicate (m + n) xs).flatten).foldl r.step q =
      ((List.replicate n xs).flatten).foldl r.step
        (((List.replicate m xs).flatten).foldl r.step q) := by
  induction n generalizing q with
  | zero => simp
  | succ n ih =>
      rw [show m + (n + 1) = (m + n) + 1 by omega]
      rw [foldl_flatten_replicate_succ, ih]
      rw [replicate_succ_append, List.flatten_append]
      simp

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
              simp [power, applyT, List.getElem?_range hq']
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
  | rep k a ihA => simp [expand, extent, ihA]

theorem matched_eq_foldl {r : Reader} (hb : StepBounded r) (x : Rope) {q : Nat} (hq : q < r.states) :
    matched r x q = r.accept ((expand x).foldl r.step q) := by
  simp only [matched]
  rw [show applyT (compile r x) q = (expand x).foldl r.step q from applyT_compile_eq_foldl hb x hq]

/-! ## CStringEnd specifics -/

private theorem reader_stepBounded : StepBounded reader := by
  intro q b hq
  simp only [reader]
  split
  · split <;> omega
  · omega

private theorem foldl_no_zero (xs : Bytes) (h : !xs.contains 0) :
    xs.foldl reader.step 0 = 0 := by
  induction xs with
  | nil => simp
  | cons b bs ih =>
      have hmem : 0 ∉ b :: bs := by simpa using h
      have hb : b ≠ 0 := by
        intro hb0
        apply hmem
        simp [hb0]
      have hbsmem : 0 ∉ bs := by
        intro h0
        apply hmem
        simp [h0]
      have hbs : !bs.contains 0 := by simpa using hbsmem
      simp only [List.foldl_cons]
      rw [show reader.step 0 b = 0 by simp [reader, hb]]
      exact ih hbs

private theorem foldl_absorb_two (xs : Bytes) : xs.foldl reader.step 2 = 2 := by
  induction xs with
  | nil => simp
  | cons b bs ih =>
      simp only [List.foldl_cons]
      rw [show reader.step 2 b = 2 by simp [reader]]
      exact ih

private theorem foldl_after_one (xs : Bytes) :
    xs.foldl reader.step 1 = if xs.length = 0 then 1 else 2 := by
  cases xs with
  | nil => simp
  | cons b bs =>
      simp only [List.foldl_cons]
      rw [show reader.step 1 b = 2 by simp [reader]]
      rw [foldl_absorb_two]
      simp

theorem sound_TruncateAtTerminator : CStringEnd.TruncateAtTerminator.Goal := by
  intro pre tail hpre
  rw [matched_eq_foldl reader_stepBounded (.cat (.cat pre (.lit [0])) tail) (by decide : 0 < 3)]
  have hexpand : expand (.cat (.cat pre (.lit [0])) tail) =
      (expand pre ++ [0]) ++ expand tail := by
    simp [expand]
  rw [hexpand, foldl_append_step, foldl_append_step]
  have hfoldPre : (expand pre).foldl reader.step 0 = 0 := foldl_no_zero _ hpre
  have hq0 : reader.step ((expand pre).foldl reader.step 0) 0 = 1 := by
    rw [hfoldPre]
    simp [reader]
  have hfirst : [0].foldl reader.step ((expand pre).foldl reader.step 0) = 1 := by
    simpa using hq0
  rw [hfirst, foldl_after_one]
  by_cases h : extent tail = 0 <;> simp [h, reader]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_GrammarScans_CStringEnd_TruncateAtTerminator
