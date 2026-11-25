import TinylambOpt.ResearchGoals.StorageExecution.GrammarScans

namespace TinylambOpt.ResearchProofs.StorageExecution.GrammarScans

open TinylambOpt.ResearchGoals.StorageExecution.GrammarScans
open TinylambOpt.ResearchGoals.Compression.Wire

set_option maxHeartbeats 0
set_option maxRecDepth 10000

/-! ## Generic helpers -/

@[simp] theorem applyT_getD (t : Table) (q : Nat) : applyT t q = t[q]?.getD q := rfl

@[simp] theorem chain_length (a b : Table) : (chain a b).length = a.length := by
  simp [chain]

theorem chain_getElem?_eq (a b : Table) (i : Nat) (hi : i < a.length) :
    (chain a b)[i]? = some (applyT b (a[i]?.getD i)) := by
  simp [chain, hi]

theorem chain_getElem?_none (a b : Table) (i : Nat) (hi : a.length ≤ i) :
    (chain a b)[i]? = none := by
  simp [chain, List.getElem?_eq_none_iff.mpr hi]

theorem applyT_chain (a b : Table) (q : Nat) (hq : q < a.length) :
    applyT (chain a b) q = applyT b (applyT a q) := by
  have hq' : q < (chain a b).length := by simpa [chain_length]
  simp [applyT, List.getElem?_eq_getElem hq', List.getElem?_eq_getElem hq, chain]

theorem applyT_chain_of_length_le (a b : Table) (q : Nat) (hq : a.length ≤ q) :
    applyT (chain a b) q = applyT b q := by
  have hq' : (chain a b).length ≤ q := by simpa [chain_length]
  simp [applyT, List.getElem?_eq_none hq', List.getElem?_eq_none hq]

@[simp] theorem compile_length (r : Reader) (x : Rope) : (compile r x).length = r.states := by
  induction x generalizing r with
  | lit s => simp [compile]
  | cat a b ihA ihB => simp [compile, chain_length, ihA, ihB]
  | rep n a ihA =>
      simp only [compile]
      induction n using Nat.strongRecOn with
      | _ n ihN =>
          unfold power
          split
          · simp
          · simp only
            split <;> simp [chain_length, ihA, ihN]

theorem power_length (n : Nat) (t : Table) : (power n t).length = t.length := by
  induction n using Nat.strongRecOn with
  | _ n ih =>
      unfold power
      split
      · simp
      · split <;> simp [chain_length, ih]

@[simp] theorem power_zero (t : Table) : power 0 t = List.range t.length := by
  unfold power
  simp

theorem power_one (t : Table) : power 1 t = t := by
  have h1 : ¬ (1 : Nat) = 0 := by decide
  unfold power
  simp [h1, chain]
  apply List.ext_getElem
  · simp [power_length]
  · intro i hi1 hi2
    have hi : i < t.length := by simpa [power_length] using hi1
    simp [applyT, List.getElem?_eq_getElem hi]

theorem getD_range (i n : Nat) (hi : i < n) : (List.range n)[i]?.getD i = i := by
  simp [List.getElem?_eq_getElem hi]

theorem applyT_power_zero (t : Table) (q : Nat) (hq : q < t.length) :
    applyT (power 0 t) q = q := by
  simp [applyT, getD_range q t.length hq]

/-! ## expand / reverse -/

@[simp] theorem expand_reverse (x : Rope) : expand (reverse x) = (expand x).reverse := by
  induction x with
  | lit s => simp [reverse, expand]
  | cat a b ihA ihB => simp [reverse, expand, ihA, ihB]
  | rep k a ihA =>
      simp [reverse, expand, ihA]
      induction k with
      | zero => simp
      | succ k ihK =>
          simp [List.replicate_succ, List.flatten_cons, ihK]

/-! ## foldl composition -/

theorem foldl_append_step (r : Reader) (xs ys : Bytes) (q : Nat) :
    (xs ++ ys).foldl r.step q = ys.foldl r.step (xs.foldl r.step q) := by
  induction xs generalizing q with
  | nil => simp
  | cons x xs ih => simp [ih]

theorem foldl_flatten_replicate (r : Reader) (n : Nat) (xs : Bytes) (q : Nat) :
    ((List.replicate n xs).flatten).foldl r.step q = Nat.iterate (fun s => xs.foldl r.step s) n q := by
  induction n generalizing q with
  | zero => simp
  | succ n ih =>
      simp [List.replicate_succ, List.flatten_cons, foldl_append_step, ih, Function.iterate_succ_apply']

/-! ## compile correctness -/

theorem applyT_compile_lit (r : Reader) (s : Bytes) (q : Nat) (hq : q < r.states) :
    applyT (compile r (.lit s)) q = s.foldl r.step q := by
  have hq' : q < (compile r (.lit s)).length := by simp [compile_length, hq]
  simp [compile, applyT, List.getElem?_eq_getElem hq']

theorem applyT_compile_cat (r : Reader) (a b : Rope) (q : Nat) (hq : q < r.states) :
    applyT (compile r (.cat a b)) q = applyT (compile r b) (applyT (compile r a) q) := by
  simp [compile]
  rw [applyT_chain]
  simp [compile_length, hq]

