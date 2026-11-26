import TinylambOpt.ResearchGoals.StorageExecution.GrammarScans

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_GrammarScans_LineBoundary_ContextReuse

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.GrammarScans
open TinylambOpt.ResearchGoals.StorageExecution.GrammarScans.LineBoundary

/-! Truth check on small inputs.  Candidate phrase pairs whose compiled tables
differ are filtered out by the premise `compile reader x = compile reader y`;
only pairs with identical compiled behaviour are checked. -/

private def samples : List Rope :=
  [ .lit [], .lit [13], .lit [10], .lit [13, 10], .lit [65],
    .lit [13, 65], .lit [65, 10], .lit [10, 13],
    .cat (.lit [13]) (.lit [10]), .cat (.lit [65]) (.lit [13]),
    .rep 0 (.lit [13, 10]), .rep 2 (.lit [13]), .rep 3 (.lit [10]),
    .rep 2 (.lit []), .cat (.rep 2 (.lit [13, 65])) (.lit [10]) ]

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

/-! ## Generic table / power correctness -/

private theorem chain_length (a b : Table) : (chain a b).length = a.length := by
  simp [chain]

private theorem applyT_chain (a b : Table) (q : Nat) (hq : q < a.length) :
    applyT (chain a b) q = applyT b (applyT a q) := by
  have hq' : q < (chain a b).length := by simpa [chain_length]
  simp [applyT, List.getElem?_eq_getElem hq, chain]

private theorem compile_length (r : Reader) (x : Rope) : (compile r x).length = r.states := by
  induction x generalizing r with
  | lit s => simp [compile]
  | cat a b ihA ihB => simp [compile, chain_length, ihA]
  | rep n a ihA =>
      simp only [compile]
      induction n using Nat.strongRecOn with
      | _ n ihN =>
          unfold power
          by_cases hn : n = 0
          · simp only [hn, dite_true]
            simpa using ihA r
          · simp only [hn, dite_false]
            have hdiv : n / 2 < n := Nat.div_lt_self (Nat.pos_of_ne_zero hn) (by decide)
            have hp := ihN (n / 2) hdiv
            by_cases he : n % 2 == 0
            · simp [he, chain_length, hp]
            · simp [he, chain_length, hp]

private theorem power_length (n : Nat) (t : Table) : (power n t).length = t.length := by
  induction n using Nat.strongRecOn with
  | _ n ih =>
      unfold power
      by_cases hn : n = 0
      · simp [hn]
      · simp only [hn, dite_false]
        have hdiv : n / 2 < n := Nat.div_lt_self (Nat.pos_of_ne_zero hn) (by decide)
        have hp := ih (n / 2) hdiv
        by_cases he : n % 2 == 0
        · simp [he, chain_length, hp]
        · simp [he, chain_length, hp]

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

private theorem replicate_succ_append (n : Nat) (xs : Bytes) :
    List.replicate (n + 1) xs = List.replicate n xs ++ [xs] := by
  induction n with
  | zero => simp
  | succ n ih =>
      calc
        List.replicate ((n + 1) + 1) xs = xs :: List.replicate (n + 1) xs := by rw [List.replicate_succ]
        _ = xs :: (List.replicate n xs ++ [xs]) := by rw [ih]
        _ = (xs :: List.replicate n xs) ++ [xs] := by simp
        _ = List.replicate (n + 1) xs ++ [xs] := by rw [List.replicate_succ]

private theorem foldl_flatten_replicate_succ (r : Reader) (n : Nat) (xs : Bytes) (q : Nat) :
    ((List.replicate (n + 1) xs).flatten).foldl r.step q =
      xs.foldl r.step (((List.replicate n xs).flatten).foldl r.step q) := by
  rw [replicate_succ_append]
  simp [List.flatten_append]

private theorem foldl_flatten_replicate_add (r : Reader) (m n : Nat) (xs : Bytes) (q : Nat) :
    ((List.replicate (m + n) xs).flatten).foldl r.step q =
      ((List.replicate n xs).flatten).foldl r.step (((List.replicate m xs).flatten).foldl r.step q) := by
  induction n generalizing q with
  | zero => simp
  | succ n ih =>
      rw [show m + (n + 1) = (m + n) + 1 by omega]
      rw [foldl_flatten_replicate_succ, ih, foldl_flatten_replicate_succ]

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
      have hq' : q < (List.range r.states).length := by simpa using hq
      simp [compile, applyT, List.getElem?_eq_getElem hq', expand]
  | cat a b ihA ihB =>
      have hqA : applyT (compile r a) q < r.states := by
        rw [ihA hq]; exact foldl_step_bounded hb _ hq
      rw [compile, applyT_chain (compile r a) (compile r b) q (by simp [compile_length, hq])]
      rw [ihA hq]
      rw [expand]
      rw [foldl_append_step]
      exact ihB (foldl_step_bounded hb _ hq)
  | rep n a ihA =>
      have hpow : ∀ m q, q < r.states →
          applyT (power m (compile r a)) q = ((List.replicate m (expand a)).flatten).foldl r.step q := by
        intro m
        induction m using Nat.strongRecOn with
        | _ m ihM =>
            intro q hq
            by_cases hm : m = 0
            · subst m
              simp [applyT_power_zero, compile_length, hq]
            · have hpos : 0 < m := Nat.pos_of_ne_zero hm
              have hdiv : m / 2 < m := Nat.div_lt_self hpos (by decide)
              have ihH := ihM (m / 2) hdiv
              unfold power
              simp only [hm, dite_false]
              by_cases hmod : m % 2 == 0
              · simp [hmod]
                rw [applyT_chain (power (m / 2) (compile r a)) (power (m / 2) (compile r a)) q
                      (by rw [power_length, compile_length]; exact hq)]
                rw [ihH q hq]
                have hbnd : ((List.replicate (m / 2) (expand a)).flatten).foldl r.step q < r.states :=
                  foldl_step_bounded hb _ hq
                rw [ihH _ hbnd]
                rw [← foldl_flatten_replicate_add]
                have hmEven : m % 2 = 0 := by simpa using hmod
                have hm2 : m / 2 + m / 2 = m := by omega
                simp [hm2]
              · simp [hmod]
                have hhalfLen :
                    q < (chain (power (m / 2) (compile r a)) (power (m / 2) (compile r a))).length := by
                  rw [chain_length, power_length, compile_length]
                  exact hq
                rw [applyT_chain (chain (power (m / 2) (compile r a)) (power (m / 2) (compile r a)))
                      (compile r a) q hhalfLen]
                have hbnd1 : ((List.replicate (m / 2) (expand a)).flatten).foldl r.step q < r.states :=
                  foldl_step_bounded hb _ hq
                have hsq :
                    applyT (chain (power (m / 2) (compile r a)) (power (m / 2) (compile r a))) q =
                      ((List.replicate (m / 2 + m / 2) (expand a)).flatten).foldl r.step q := by
                  rw [applyT_chain (power (m / 2) (compile r a)) (power (m / 2) (compile r a)) q
                        (by rw [power_length, compile_length]; exact hq)]
                  rw [ihH q hq, ihH _ hbnd1, foldl_flatten_replicate_add]
                rw [hsq]
                have hbnd2 : ((List.replicate (m / 2 + m / 2) (expand a)).flatten).foldl r.step q < r.states :=
                  foldl_step_bounded hb _ hq
                rw [ihA hbnd2]
                have h1 : ((List.replicate (m / 2 + m / 2 + 1) (expand a)).flatten).foldl r.step q =
                    (expand a).foldl r.step (((List.replicate (m / 2 + m / 2) (expand a)).flatten).foldl r.step q) := by
                  rw [foldl_flatten_replicate_succ]
                have hmOdd : m % 2 = 1 := by
                  have hmod' : ¬ m % 2 = 0 := by simpa using hmod
                  omega
                have hm2 : m / 2 + m / 2 + 1 = m := by omega
                simpa [hm2] using h1.symm
      simpa [compile, expand] using hpow n q hq

/-! ## `LineBoundary.reader` and context reuse -/

private theorem stepBounded : StepBounded reader := by
  intro q b hq
  simp [reader]
  split
  · omega
  · split
    · omega
    · omega

theorem sound_ContextReuse : LineBoundary.ContextReuse.Goal := by
  intro a b x y q hq hxy
  have hqa : (expand a).foldl reader.step q < reader.states := foldl_step_bounded stepBounded _ hq
  have hx := applyT_compile_eq_foldl stepBounded x hqa
  have hy := applyT_compile_eq_foldl stepBounded y hqa
  have hxy' : (expand x).foldl reader.step ((expand a).foldl reader.step q) =
      (expand y).foldl reader.step ((expand a).foldl reader.step q) := by
    rw [← hx, ← hy, hxy]
  simp [expand, hxy']

end TinylambOpt.ResearchProofs.StorageExecution.Goal_GrammarScans_LineBoundary_ContextReuse
