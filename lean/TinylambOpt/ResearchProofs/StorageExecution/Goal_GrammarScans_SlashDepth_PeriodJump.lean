import TinylambOpt.ResearchGoals.StorageExecution.GrammarScans

/-!
A compressed string scan uses grammar or chunk summaries to evaluate string predicates
without always reconstructing the full value.

Proved

- `SlashDepth.PeriodJump` — A cycle in a run of identical phrases permits skipping
  whole periods.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_GrammarScans_SlashDepth_PeriodJump

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.GrammarScans
open TinylambOpt.ResearchGoals.StorageExecution.GrammarScans.SlashDepth

/-! ## Truth check on small inputs (all q < reader.states = 3). -/

private structure Sample where
  rope : Rope
  q : Nat
  period : Nat
  n : Nat

private def samples : List Sample :=
  [ { rope := .lit [], q := 0, period := 1, n := 0 }
  , { rope := .lit [47], q := 0, period := 1, n := 5 }
  , { rope := .lit [47], q := 2, period := 3, n := 7 }
  , { rope := .lit [47, 47], q := 1, period := 2, n := 5 }
  , { rope := .lit [65, 47, 66], q := 0, period := 1, n := 3 }
  , { rope := .lit [65, 47, 66], q := 2, period := 4, n := 9 }
  , { rope := .cat (.lit [47]) (.lit [47, 65]), q := 0, period := 2, n := 6 }
  , { rope := .cat (.lit [65]) (.lit [65, 47]), q := 1, period := 3, n := 8 }
  , { rope := .rep 2 (.lit [47, 65]), q := 0, period := 2, n := 5 }
  , { rope := .rep 3 (.lit []), q := 2, period := 2, n := 7 }
  , { rope := .rep 0 (.lit [47]), q := 1, period := 3, n := 4 }
  , { rope := .cat (.rep 2 (.lit [47])) (.lit [47]), q := 2, period := 5, n := 11 }
  , { rope := .lit [47, 47, 47], q := 2, period := 1, n := 0 }
  ]

private def sampleOK (s : Sample) : Bool :=
  decide (s.q < reader.states → 0 < s.period →
    applyT (power s.period (compile reader s.rope)) s.q = s.q →
      matched reader (.rep s.n s.rope) s.q =
        matched reader (.rep (s.n % s.period) s.rope) s.q)

private def check : Bool := samples.all sampleOK

#eval check

example : check := by native_decide

/-! ## Generic closed-reader facts. -/

private theorem sum_replicate_nat (n m : Nat) :
    (List.replicate n m).sum = n * m := by
  induction n with
  | zero => simp
  | succ n ih => simp [List.replicate_succ, ih, Nat.succ_mul, Nat.add_comm]

private theorem length_expand (x : Rope) : extent x = (expand x).length := by
  induction x with
  | lit s => rfl
  | cat a b iha ihb => simp [extent, expand, iha, ihb]
  | rep k a iha =>
      simp only [extent, expand, List.length_flatten, List.map_replicate,
        sum_replicate_nat, iha]

private theorem power_length (n : Nat) (t : Table) : (power n t).length = t.length := by
  induction n using Nat.strongRecOn generalizing t with
  | ind n ih =>
      unfold power
      split
      · simp [List.length_range]
      · rename_i hn
        split
        · simp only [chain, List.length_map]
          exact ih (n / 2) (Nat.div_lt_self (by omega) (by decide)) t
        · simp only [chain, List.length_map]
          exact ih (n / 2) (Nat.div_lt_self (by omega) (by decide)) t

private theorem compile_length (r : Reader) (x : Rope) :
    (compile r x).length = r.states := by
  induction x with
  | lit s => simp [compile]
  | cat a b iha ihb => simp [compile, chain, iha, ihb]
  | rep k a ih => simp [compile, power_length, ih]

private theorem applyT_chain (a b : Table) (q : Nat) (hq : q < a.length) :
    applyT (chain a b) q = applyT b (applyT a q) := by
  simp [applyT, chain, List.getElem?_map, List.getElem?_eq_getElem hq]

private def Closed (r : Reader) : Prop :=
  ∀ q b, q < r.states → r.step q b < r.states

private theorem foldl_closed (r : Reader) (h : Closed r) (s : Bytes) (q : Nat)
    (hq : q < r.states) : s.foldl r.step q < r.states := by
  induction s generalizing q with
  | nil => exact hq
  | cons b bs ih => exact ih (r.step q b) (h q b hq)

private theorem flatten_replicate_add (a b : Nat) (s : Bytes) :
    (List.replicate (a + b) s).flatten =
      (List.replicate a s).flatten ++ (List.replicate b s).flatten := by
  induction a with
  | zero => simp
  | succ a ih =>
      rw [Nat.succ_add, List.replicate_succ, List.flatten_cons, ih,
        List.replicate_succ, List.flatten_cons]
      simp [List.append_assoc]

private theorem power_apply_of (r : Reader) (h : Closed r) (x : Rope)
    (ha : ∀ q, q < r.states →
      applyT (compile r x) q = (expand x).foldl r.step q) (n : Nat)
    (q : Nat) (hq : q < r.states) :
    applyT (power n (compile r x)) q =
      ((List.replicate n (expand x)).flatten).foldl r.step q := by
  induction n using Nat.strongRecOn generalizing q with
  | ind n ih =>
      rw [power]
      split
      · rename_i hn
        subst n
        rw [compile_length]
        change (List.range r.states)[q]?.getD q = q
        simp [applyT, List.getElem?_range hq]
      · rename_i hn
        let m := n / 2
        let ht := power m (compile r x)
        have hm : m < n := Nat.div_lt_self (by omega) (by decide)
        have ih' := ih m hm
        have hlen : ht.length = r.states := by
          simp [ht, power_length, compile_length]
        split
        · rename_i heven
          change applyT (chain ht ht) q =
            ((List.replicate n (expand x)).flatten).foldl r.step q
          have hmod0 : n % 2 = 0 := by simpa using heven
          have heq : n = m + m := by
            change n = n / 2 + n / 2
            have hd := Nat.div_add_mod n 2
            omega
          rw [heq, flatten_replicate_add, List.foldl_append]
          rw [applyT_chain ht ht q (by rw [hlen]; exact hq), ih' q hq]
          exact ih' _ (foldl_closed r h ((List.replicate m (expand x)).flatten) q hq)
        · rename_i hodd
          change applyT (chain (chain ht ht) (compile r x)) q =
            ((List.replicate n (expand x)).flatten).foldl r.step q
          have heq : n = m + m + 1 := by
            change n = n / 2 + n / 2 + 1
            have hd := Nat.div_add_mod n 2
            have hne : n % 2 ≠ 0 := by
              intro hz
              apply hodd
              simp [hz]
            have hlt := Nat.mod_lt n (by decide : 0 < 2)
            omega
          rw [heq, List.replicate_succ', List.flatten_append]
          simp only [List.flatten_cons, List.flatten_nil, List.nil_append]
          rw [List.foldl_append, List.foldl_append]
          rw [applyT_chain (chain ht ht) (compile r x) q
            (by simp only [chain, List.length_map]; rw [hlen]; exact hq)]
          rw [applyT_chain ht ht q (by rw [hlen]; exact hq), ih' q hq]
          rw [ih' _ (foldl_closed r h ((List.replicate m (expand x)).flatten) q hq)]
          simp only [List.foldl_nil]
          rw [flatten_replicate_add, List.foldl_append]
          have hq1 := foldl_closed r h ((List.replicate m (expand x)).flatten) q hq
          have hq2 := foldl_closed r h ((List.replicate m (expand x)).flatten) _ hq1
          rw [ha _ hq2]

private theorem compile_apply (r : Reader) (h : Closed r) (x : Rope) (q : Nat)
    (hq : q < r.states) :
    applyT (compile r x) q = (expand x).foldl r.step q := by
  induction x generalizing q with
  | lit s =>
      simp only [compile, applyT, List.getElem?_map,
        List.getElem?_range (by simpa using hq), Option.map_some, Option.getD_some]
      rfl
  | cat a b iha ihb =>
      rw [compile, applyT_chain _ _ q (by simpa [compile_length] using hq),
        iha q hq]
      rw [ihb _ (foldl_closed r h (expand a) q hq)]
      simp [expand, List.foldl_append]
  | rep k a ih =>
      rw [compile]
      exact power_apply_of r h a ih k q hq

private theorem power_apply (r : Reader) (h : Closed r) (x : Rope) (n : Nat)
    (q : Nat) (hq : q < r.states) :
    applyT (power n (compile r x)) q =
      ((List.replicate n (expand x)).flatten).foldl r.step q := by
  have hv := compile_apply r h (Rope.rep n x) q hq
  simpa only [compile, expand] using hv

/-! ## SlashDepth-specific facts. -/

private theorem slash_closed : Closed reader := by
  intro q b hq
  have hq3 : q < 3 := by simpa [reader] using hq
  dsimp [reader]
  split
  · have hle : min 2 (q + 1) ≤ 2 := Nat.min_le_left _ _
    omega
  · exact hq3

private theorem min2_succ_add (q k : Nat) :
    min 2 (min 2 (q + 1) + k) = min 2 (q + 1 + k) := by
  by_cases h : q + 1 ≤ 2
  · rw [Nat.min_eq_right h]
  · have hmin : min 2 (q + 1) = 2 := Nat.min_eq_left (by omega)
    have hleft : min 2 (2 + k) = 2 := Nat.min_eq_left (by omega)
    have hright : min 2 (q + 1 + k) = 2 := Nat.min_eq_left (by omega)
    rw [hmin, hleft, hright]

private theorem fold_count (s : Bytes) (q : Nat) (hq : q < reader.states) :
    s.foldl reader.step q = min 2 (q + s.count 47) := by
  induction s generalizing q with
  | nil =>
      have hq3 : q ≤ 2 := by
        have : q < 3 := by simpa [reader] using hq
        omega
      simp [Nat.min_eq_right hq3]
  | cons b bs ih =>
      rw [List.foldl_cons, ih (reader.step q b) (slash_closed q b hq)]
      by_cases hb : b = 47
      · subst b
        simp [reader, List.count_cons, min2_succ_add, Nat.add_assoc,
          Nat.add_comm, Nat.add_left_comm]
      · simp [reader, hb, List.count_cons]

private theorem count_flatten_replicate (n : Nat) (s : Bytes) :
    (List.replicate n s).flatten.count 47 = n * s.count 47 := by
  induction n with
  | zero => simp
  | succ n ih =>
      rw [List.replicate_succ, List.flatten_cons, List.count_append, ih]
      simp [Nat.succ_mul, Nat.add_comm]

private theorem matched_rep (x : Rope) (k q : Nat) (hq : q < reader.states) :
    matched reader (.rep k x) q =
      decide (min 2 (q + k * (expand x).count 47) = 2) := by
  unfold matched
  change reader.accept (applyT (power k (compile reader x)) q) = _
  rw [power_apply reader slash_closed x k q hq]
  rw [fold_count _ _ hq]
  rw [count_flatten_replicate]
  rfl

/-! ## The goal. -/

theorem sound_PeriodJump : SlashDepth.PeriodJump.Goal := by
  intro x q period n hq hperiod hcycle
  have hcycle' := power_apply reader slash_closed x period q hq
  rw [hcycle'] at hcycle
  rw [fold_count _ _ hq] at hcycle
  rw [count_flatten_replicate] at hcycle
  let c := (expand x).count 47
  change min 2 (q + period * c) = q at hcycle
  have hcase : q = 2 ∨ c = 0 := by
    by_cases hle : q + period * c ≤ 2
    · have hmin : min 2 (q + period * c) = q + period * c := Nat.min_eq_right hle
      rw [hmin] at hcycle
      have hmul : period * c = 0 := by omega
      rcases Nat.mul_eq_zero.mp hmul with hp | hc
      · exact False.elim (by omega)
      · exact Or.inr hc
    · have hmin : min 2 (q + period * c) = 2 := Nat.min_eq_left (by omega)
      rw [hmin] at hcycle
      exact Or.inl hcycle.symm
  rw [matched_rep x n q hq, matched_rep x (n % period) q hq]
  change (min 2 (q + n * c) == 2) = (min 2 (q + (n % period) * c) == 2)
  rcases hcase with hq2 | hc0
  · subst q
    have hA : min 2 (2 + n * c) = 2 := Nat.min_eq_left (by omega)
    have hB : min 2 (2 + (n % period) * c) = 2 := Nat.min_eq_left (by omega)
    rw [hA, hB]
  · rw [hc0]
    have hq3 : q ≤ 2 := by
      have : q < 3 := by simpa [reader] using hq
      omega
    have hminq : min 2 q = q := Nat.min_eq_right hq3
    simp [hminq]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_GrammarScans_SlashDepth_PeriodJump
