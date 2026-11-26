import TinylambOpt.ResearchGoals.StorageExecution.GrammarScans

/-!
A compressed string scan uses grammar or chunk summaries to evaluate string predicates
without always reconstructing the full value.

Proved

- `EndsInDigit.DictionaryMask` — Cache acceptance once per phrase; a missing local
  dictionary code is rejected.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_GrammarScans_EndsInDigit_DictionaryMask

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.GrammarScans
open TinylambOpt.ResearchGoals.StorageExecution.GrammarScans.EndsInDigit

/-! Truth check on small dictionaries.  All scanned states satisfy the
hypothesis `q < reader.states`; `ids` deliberately contains in-range and
out-of-range dictionary codes, the latter exercising the `getD false` path.
The sample phrases mix digit and non-digit bytes, empty phrases,
concatenations and repetitions. -/

private def samples : List Rope :=
  [ .lit [], .lit [48], .lit [57], .lit [65], .lit [48, 57], .lit [57, 65, 48],
    .cat (.lit [48]) (.lit [65]), .cat (.lit [65, 49]) (.lit [57]),
    .rep 0 (.lit [48]), .rep 2 (.lit [57]), .rep 3 (.lit []),
    .rep 2 (.lit [48, 65, 57]), .cat (.rep 2 (.lit [65])) (.lit [50]) ]

private def sampleIds : List Nat := [0, 1, 2, 9]

private def check : Bool :=
  (List.range reader.states).all fun q =>
    rows reader samples sampleIds q =
      sampleIds.map (fun i =>
        (samples[i]?.map (fun x => reader.accept ((expand x).foldl reader.step q))).getD false)

#eval check

example : check := by native_decide

/-! Generic transition-table facts (self-contained copies; the shared proof
module is not imported). -/

private def EdClosed (r : Reader) : Prop :=
  ∀ q b, q < r.states → r.step q b < r.states

private theorem ed_foldl_closed (r : Reader) (h : EdClosed r) (s : Bytes) (q : Nat)
    (hq : q < r.states) : s.foldl r.step q < r.states := by
  induction s generalizing q with
  | nil => exact hq
  | cons b bs ih => exact ih (r.step q b) (h q b hq)

private theorem ed_power_length (n : Nat) (t : Table) :
    (power n t).length = t.length := by
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

private theorem ed_compile_length (r : Reader) (x : Rope) :
    (compile r x).length = r.states := by
  induction x with
  | lit s => simp [compile]
  | cat a b iha ihb => simp [compile, chain, iha]
  | rep k a ih => simp [compile, ed_power_length, ih]

private theorem ed_applyT_chain (a b : Table) (q : Nat) (hq : q < a.length) :
    applyT (chain a b) q = applyT b (applyT a q) := by
  simp [applyT, chain, List.getElem?_map, List.getElem?_eq_getElem hq]

private theorem ed_flatten_replicate_add (a b : Nat) (s : Bytes) :
    (List.replicate (a + b) s).flatten =
      (List.replicate a s).flatten ++ (List.replicate b s).flatten := by
  induction a with
  | zero => simp
  | succ a ih =>
      rw [Nat.succ_add, List.replicate_succ, List.flatten_cons, ih,
        List.replicate_succ, List.flatten_cons]
      simp [List.append_assoc]

private theorem ed_power_apply_of (r : Reader) (h : EdClosed r) (x : Rope)
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
        rw [ed_compile_length]
        change (List.range r.states)[q]?.getD q = q
        simp [List.getElem?_range hq]
      · rename_i hn
        let m := n / 2
        let ht := power m (compile r x)
        have hm : m < n := Nat.div_lt_self (by omega) (by decide)
        have ih' := ih m hm
        have hlen : ht.length = r.states := by
          simp [ht, ed_power_length, ed_compile_length]
        split
        · rename_i heven
          change applyT (chain ht ht) q =
            ((List.replicate n (expand x)).flatten).foldl r.step q
          have hmod0 : n % 2 = 0 := by simpa using heven
          have heq : n = m + m := by
            change n = n / 2 + n / 2
            have hd := Nat.div_add_mod n 2
            omega
          rw [heq, ed_flatten_replicate_add, List.foldl_append]
          rw [ed_applyT_chain ht ht q (by rw [hlen]; exact hq), ih' q hq]
          exact ih' _ (ed_foldl_closed r h
            ((List.replicate m (expand x)).flatten) q hq)
        · rename_i hodd
          change applyT (chain (chain ht ht) (compile r x)) q =
            ((List.replicate n (expand x)).flatten).foldl r.step q
          have hmod1 : n % 2 = 1 := by
            have hne : n % 2 ≠ 0 := by
              intro hz
              apply hodd
              simp [hz]
            have hlt := Nat.mod_lt n (by decide : 0 < 2)
            omega
          have heq : n = m + m + 1 := by
            change n = n / 2 + n / 2 + 1
            have hd := Nat.div_add_mod n 2
            omega
          rw [heq, List.replicate_succ', List.flatten_append]
          simp only [List.flatten_cons, List.flatten_nil]
          rw [List.foldl_append, List.foldl_append]
          rw [ed_applyT_chain (chain ht ht) (compile r x) q
            (by simp only [chain, List.length_map]; rw [hlen]; exact hq)]
          rw [ed_applyT_chain ht ht q (by rw [hlen]; exact hq), ih' q hq]
          rw [ih' _ (ed_foldl_closed r h
            ((List.replicate m (expand x)).flatten) q hq)]
          simp only [List.foldl_nil]
          rw [ed_flatten_replicate_add, List.foldl_append]
          have hq1 := ed_foldl_closed r h
            ((List.replicate m (expand x)).flatten) q hq
          have hq2 := ed_foldl_closed r h
            ((List.replicate m (expand x)).flatten) _ hq1
          rw [ha _ hq2]

private theorem ed_compile_apply (r : Reader) (h : EdClosed r) (x : Rope) (q : Nat)
    (hq : q < r.states) :
    applyT (compile r x) q = (expand x).foldl r.step q := by
  induction x generalizing q with
  | lit s =>
      simp only [compile, applyT, List.getElem?_map,
        List.getElem?_range (by simpa using hq), Option.map_some, Option.getD_some]
      rfl
  | cat a b iha ihb =>
      rw [compile, ed_applyT_chain _ _ q (by simpa [ed_compile_length] using hq),
        iha q hq]
      rw [ihb _ (ed_foldl_closed r h (expand a) q hq)]
      simp [expand, List.foldl_append]
  | rep k a ih =>
      rw [compile]
      exact ed_power_apply_of r h a ih k q hq

private theorem ed_dictionaryMask_of_closed (r : Reader) (h : EdClosed r) :
    dictionaryMask r := by
  intro d ids q hq
  unfold rows matched
  apply List.map_congr_left
  intro i hi
  simp only [List.getElem?_map]
  cases hd : d[i]? with
  | none => rfl
  | some x => simp [ed_compile_apply r h x q hq]

/-! The EndsInDigit step ignores the incoming state and always returns 0 or 1,
so the reader stays inside its two-state space. -/

private theorem ed_closed : EdClosed reader := by
  intro q b hq
  change (decide (48 ≤ b ∧ b ≤ 57)).toNat < 2
  by_cases h : 48 ≤ b ∧ b ≤ 57 <;> simp [h]

theorem sound_DictionaryMask : EndsInDigit.DictionaryMask.Goal := by
  exact ed_dictionaryMask_of_closed reader ed_closed

end TinylambOpt.ResearchProofs.StorageExecution.Goal_GrammarScans_EndsInDigit_DictionaryMask
