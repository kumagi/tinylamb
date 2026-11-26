import TinylambOpt.ResearchGoals.StorageExecution.GrammarScans

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_GrammarScans_EscapedTail_ContextReuse

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.GrammarScans
open TinylambOpt.ResearchGoals.StorageExecution.GrammarScans.EscapedTail

/-! Truth check on small inputs.  Phrase pairs whose compiled tables differ are
filtered out because they do not satisfy the premise of `contextReuse`. -/

private def samples : List Rope :=
  [ .lit [], .lit [92], .lit [65], .lit [92, 65], .lit [65, 92], .lit [92, 92],
    .lit [65, 92, 92, 92], .cat (.lit [92]) (.lit [65]),
    .cat (.lit [65]) (.lit [92, 92]), .rep 0 (.lit [92]), .rep 2 (.lit [92]),
    .rep 3 (.lit [65, 92]), .rep 2 (.lit [92, 65]),
    .cat (.rep 2 (.lit [92, 66])) (.lit [92]) ]

private def check : Bool :=
  (List.range reader.states).all fun q =>
    samples.all fun a =>
      samples.all fun b =>
        samples.all fun x =>
          samples.all fun y =>
            compile reader x != compile reader y ||
              (expand (.cat a (.cat x b))).foldl reader.step q ==
                (expand (.cat a (.cat y b))).foldl reader.step q

#eval check

example : check := by native_decide

/-! ## Generic transition-table facts (independent of the reader). -/

private theorem chain_length (a b : Table) : (chain a b).length = a.length := by
  simp [chain]

private theorem applyT_chain (a b : Table) (q : Nat) (hq : q < a.length) :
    applyT (chain a b) q = applyT b (applyT a q) := by
  have hq' : q < (chain a b).length := by simpa [chain_length] using hq
  have haopt : a[q]? = some (a[q]) := by simp [List.getElem?_eq_getElem hq]
  simp [applyT, chain, haopt]

private theorem power_length (n : Nat) (t : Table) : (power n t).length = t.length := by
  induction n using Nat.strongRecOn with
  | _ n ih =>
      by_cases hn : n = 0
      · simp [power, hn]
      · have hpos : 0 < n := by omega
        have hhalf : (power (n / 2) t).length = t.length :=
          ih (n / 2) (Nat.div_lt_self hpos (by decide : 1 < 2))
        unfold power
        rw [dite_eq_right hn]
        dsimp only
        by_cases heven : n % 2 == 0
        · simp [heven, chain_length, hhalf]
        · simp [heven, chain_length, hhalf]

private theorem compile_length (r : Reader) (x : Rope) : (compile r x).length = r.states := by
  induction x generalizing r with
  | lit s => simp [compile]
  | cat a b ihA ihB => simp [compile, chain_length, ihA]
  | rep n a ihA =>
      change (power n (compile r a)).length = r.states
      rw [power_length]
      exact ihA r

private theorem getD_range (i n : Nat) (hi : i < n) : (List.range n)[i]?.getD i = i := by
  have hi' : i < (List.range n).length := by simpa using hi
  simp [List.getElem?_eq_getElem hi']

private theorem applyT_power_zero (t : Table) (q : Nat) (hq : q < t.length) :
    applyT (power 0 t) q = q := by
  simp [power, applyT, getD_range q t.length hq]

private theorem foldl_append_step (r : Reader) (xs ys : Bytes) (q : Nat) :
    (xs ++ ys).foldl r.step q = ys.foldl r.step (xs.foldl r.step q) := by
  induction xs generalizing q with
  | nil => simp
  | cons x xs ih => simp [ih]

private theorem foldl_flatten_replicate_succ (r : Reader) (n : Nat) (xs : Bytes) (q : Nat) :
    ((List.replicate (n + 1) xs).flatten).foldl r.step q =
      xs.foldl r.step (((List.replicate n xs).flatten).foldl r.step q) := by
  rw [List.replicate_succ']
  simp [List.flatten_append]

private theorem foldl_flatten_replicate_add (r : Reader) (m n : Nat) (xs : Bytes) (q : Nat) :
    ((List.replicate (m + n) xs).flatten).foldl r.step q =
      ((List.replicate n xs).flatten).foldl r.step (((List.replicate m xs).flatten).foldl r.step q) := by
  induction n generalizing q with
  | zero => simp
  | succ n ih =>
      rw [show m + (n + 1) = (m + n) + 1 by omega]
      rw [foldl_flatten_replicate_succ r (m + n) xs q]
      rw [ih]
      rw [foldl_flatten_replicate_succ r n xs (List.foldl r.step q (List.replicate m xs).flatten)]

private def StepBounded (r : Reader) : Prop := ∀ q b, q < r.states → r.step q b < r.states

private theorem foldl_step_bounded {r : Reader} (hb : StepBounded r) (xs : Bytes) {q : Nat}
    (hq : q < r.states) : xs.foldl r.step q < r.states := by
  induction xs generalizing q with
  | nil => simpa
  | cons x xs ih => exact ih (hb q x hq)

private theorem applyT_compile_eq_foldl {r : Reader} (hb : StepBounded r) (x : Rope) {q : Nat}
    (hq : q < r.states) : applyT (compile r x) q = (expand x).foldl r.step q := by
  induction x generalizing q with
  | lit s =>
      have hqrange : q < (List.range r.states).length := by simpa using hq
      rw [show compile r (.lit s) = (List.range r.states).map (fun q => s.foldl r.step q) by rfl]
      simp [applyT, expand, List.getElem?_eq_getElem hqrange]
  | cat a b ihA ihB =>
      have hqA : applyT (compile r a) q < r.states := by
        rw [ihA hq]
        exact foldl_step_bounded hb (expand a) hq
      have ihBqA : applyT (compile r b) ((expand a).foldl r.step q) =
          (expand b).foldl r.step ((expand a).foldl r.step q) := by
        simpa [ihA hq] using ihB hqA
      rw [show compile r (.cat a b) = chain (compile r a) (compile r b) by rfl]
      rw [applyT_chain (compile r a) (compile r b) q (by rw [compile_length]; exact hq)]
      rw [ihA hq, ihBqA]
      simp [expand]
  | rep n a ihA =>
      have hpow : ∀ m q, q < r.states →
          applyT (power m (compile r a)) q =
            ((List.replicate m (expand a)).flatten).foldl r.step q := by
        intro m
        induction m using Nat.strongRecOn with
        | _ m ihM =>
            intro q hq
            by_cases hm : m = 0
            · subst m
              simp [applyT_power_zero (compile r a) q (by rw [compile_length]; exact hq)]
            · have hpos : 0 < m := by omega
              have hhalf : m / 2 < m := Nat.div_lt_self hpos (by decide : 1 < 2)
              have ihH := ihM (m / 2) hhalf
              have hHq : applyT (power (m / 2) (compile r a)) q =
                  ((List.replicate (m / 2) (expand a)).flatten).foldl r.step q := ihH q hq
              have hHqBound : ((List.replicate (m / 2) (expand a)).flatten).foldl r.step q <
                  r.states := foldl_step_bounded hb _ hq
              have hSq : applyT (chain (power (m / 2) (compile r a)) (power (m / 2) (compile r a))) q =
                  ((List.replicate (m / 2 + m / 2) (expand a)).flatten).foldl r.step q := by
                rw [applyT_chain _ _ q (by rw [power_length, compile_length]; exact hq)]
                rw [hHq]
                rw [show applyT (power (m / 2) (compile r a))
                      (((List.replicate (m / 2) (expand a)).flatten).foldl r.step q) =
                    ((List.replicate (m / 2) (expand a)).flatten).foldl r.step
                      (((List.replicate (m / 2) (expand a)).flatten).foldl r.step q) from
                  ihH _ hHqBound]
                rw [foldl_flatten_replicate_add]
              unfold power
              rw [dite_eq_right hm]
              dsimp only
              split
              · rename_i hev
                have hev0 : m % 2 = 0 := by simpa using hev
                have hm2 : m / 2 + m / 2 = m := by omega
                simpa [hm2] using hSq
              · rename_i hod
                have hod0 : m % 2 ≠ 0 := by simpa using hod
                have hmodlt : m % 2 < 2 := Nat.mod_lt m (by decide : 0 < 2)
                have hm2 : m / 2 + m / 2 + 1 = m := by omega
                rw [applyT_chain (chain (power (m / 2) (compile r a)) (power (m / 2) (compile r a)))
                      (compile r a) q (by rw [chain_length, power_length, compile_length]; exact hq)]
                rw [hSq]
                have hSqBound : ((List.replicate (m / 2 + m / 2) (expand a)).flatten).foldl r.step q <
                    r.states := foldl_step_bounded hb _ hq
                rw [ihA hSqBound]
                have hsucc := foldl_flatten_replicate_succ r (m / 2 + m / 2) (expand a) q
                simpa [hm2] using hsucc.symm
      show applyT (power n (compile r a)) q =
        List.foldl r.step q ((List.replicate n (expand a)).flatten)
      exact hpow n q hq

/-! ## `EscapedTail.reader` is closed on its two states, then context reuse. -/

private theorem stepBounded : StepBounded reader := by
  intro q b hq
  have hq' : q < 2 := by simpa [reader] using hq
  change (if b == 92 then 1 - q else 0) < 2
  split <;> omega

theorem sound_ContextReuse : EscapedTail.ContextReuse.Goal := by
  intro a b x y q hq hxy
  have hqa : (expand a).foldl reader.step q < reader.states :=
    foldl_step_bounded stepBounded (expand a) hq
  have hx := applyT_compile_eq_foldl stepBounded x hqa
  have hy := applyT_compile_eq_foldl stepBounded y hqa
  have hxy' : (expand x).foldl reader.step ((expand a).foldl reader.step q) =
      (expand y).foldl reader.step ((expand a).foldl reader.step q) := by
    rw [← hx, ← hy, hxy]
  simp [expand, hxy']

end TinylambOpt.ResearchProofs.StorageExecution.Goal_GrammarScans_EscapedTail_ContextReuse
