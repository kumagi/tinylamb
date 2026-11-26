import TinylambOpt.ResearchGoals.StorageExecution.GrammarScans

/-!
A compressed string scan uses grammar or chunk summaries to evaluate string predicates
without always reconstructing the full value.

Proved

- `PercentEscape.DictionaryMask` — Cache acceptance once per phrase; a missing local
  dictionary code is rejected.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_GrammarScans_PercentEscape_DictionaryMask

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.GrammarScans
open TinylambOpt.ResearchGoals.StorageExecution.GrammarScans.PercentEscape

/-! ## Truth check on small dictionaries.

Every scanned state satisfies `q < reader.states = 4`; `sampleIds` also has an
out-of-range code so the `getD false` branch is exercised. -/

private def samples : List Rope :=
  [ .lit [], .lit [37], .lit [37, 52], .lit [37, 52, 70], .lit [37, 52, 71],
    .lit [65], .lit [37, 52, 70, 37, 57, 65], .lit [37, 52, 70, 65],
    .cat (.lit [37, 52]) (.lit [70]), .rep 0 (.lit [37, 52, 70]),
    .rep 2 (.lit [37, 52, 70]), .rep 3 (.lit [37, 52]),
    .cat (.rep 2 (.lit [37, 52])) (.lit [70]) ]

private def sampleIds : List Nat := [0, 1, 2, 9]

private def check : Bool :=
  (List.range reader.states).all fun q =>
    rows reader samples sampleIds q =
      sampleIds.map (fun i =>
        (samples[i]?.map (fun x => reader.accept ((expand x).foldl reader.step q))).getD false)

#eval check

example : check := by native_decide

/-! ## Generic compiled-table facts (valid for any closed reader). -/

private theorem pe_power_length (n : Nat) (t : Table) :
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

private theorem pe_compile_length (r : Reader) (x : Rope) :
    (compile r x).length = r.states := by
  induction x with
  | lit s => simp [compile]
  | cat a b iha ihb => simp [compile, chain, iha, ihb]
  | rep k a ih => simp [compile, pe_power_length, ih]

private theorem pe_applyT_chain (a b : Table) (q : Nat) (hq : q < a.length) :
    applyT (chain a b) q = applyT b (applyT a q) := by
  simp [applyT, chain, List.getElem?_map, List.getElem?_eq_getElem hq]

private def PeClosed (r : Reader) : Prop :=
  ∀ q b, q < r.states → r.step q b < r.states

private theorem pe_foldl_closed (r : Reader) (h : PeClosed r) (s : Bytes) (q : Nat)
    (hq : q < r.states) : s.foldl r.step q < r.states := by
  induction s generalizing q with
  | nil => exact hq
  | cons b bs ih => exact ih (r.step q b) (h q b hq)

private theorem pe_flatten_replicate_add (a b : Nat) (s : Bytes) :
    (List.replicate (a + b) s).flatten =
      (List.replicate a s).flatten ++ (List.replicate b s).flatten := by
  induction a with
  | zero => simp
  | succ a ih =>
      rw [Nat.succ_add, List.replicate_succ, List.flatten_cons, ih,
        List.replicate_succ, List.flatten_cons]
      simp [List.append_assoc]

/-! The compiled table really computes the fold of the expanded phrase;
this is all the dictionary-mask goal needs. -/

private theorem pe_power_apply_of (r : Reader) (h : PeClosed r) (x : Rope)
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
        rw [pe_compile_length]
        change (List.range r.states)[q]?.getD q = q
        simp [applyT, List.getElem?_range hq]
      · rename_i hn
        let m := n / 2
        let ht := power m (compile r x)
        have hm : m < n := Nat.div_lt_self (by omega) (by decide)
        have ih' := ih m hm
        have hlen : ht.length = r.states := by
          simp [ht, pe_power_length, pe_compile_length]
        split
        · rename_i heven
          change applyT (chain ht ht) q =
            ((List.replicate n (expand x)).flatten).foldl r.step q
          have hmod0 : n % 2 = 0 := by simpa using heven
          have heq : n = m + m := by
            change n = n / 2 + n / 2
            have hd := Nat.div_add_mod n 2
            omega
          rw [heq, pe_flatten_replicate_add, List.foldl_append]
          rw [pe_applyT_chain ht ht q (by rw [hlen]; exact hq), ih' q hq]
          exact ih' _ (pe_foldl_closed r h
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
          simp only [List.flatten_cons, List.flatten_nil, List.nil_append]
          rw [List.foldl_append, List.foldl_append]
          rw [pe_applyT_chain (chain ht ht) (compile r x) q
            (by simp only [chain, List.length_map]; rw [hlen]; exact hq)]
          rw [pe_applyT_chain ht ht q (by rw [hlen]; exact hq), ih' q hq]
          rw [ih' _ (pe_foldl_closed r h
            ((List.replicate m (expand x)).flatten) q hq)]
          simp only [List.foldl_nil]
          rw [pe_flatten_replicate_add, List.foldl_append]
          have hq1 := pe_foldl_closed r h
            ((List.replicate m (expand x)).flatten) q hq
          have hq2 := pe_foldl_closed r h
            ((List.replicate m (expand x)).flatten) _ hq1
          rw [ha _ hq2]

private theorem pe_compile_apply (r : Reader) (h : PeClosed r) (x : Rope) (q : Nat)
    (hq : q < r.states) :
    applyT (compile r x) q = (expand x).foldl r.step q := by
  induction x generalizing q with
  | lit s =>
      simp only [compile, applyT, List.getElem?_map,
        List.getElem?_range (by simpa using hq), Option.map_some, Option.getD_some]
      rfl
  | cat a b iha ihb =>
      rw [compile, pe_applyT_chain _ _ q (by simpa [pe_compile_length] using hq),
        iha q hq]
      rw [ihb _ (pe_foldl_closed r h (expand a) q hq)]
      simp [expand, List.foldl_append]
  | rep k a ih =>
      rw [compile]
      exact pe_power_apply_of r h a ih k q hq

/-! The percent-escape reader's step function stays inside its four states. -/

private theorem pe_reader_closed : PeClosed reader := by
  intro q b hq
  have hq4 : q < 4 := by simpa [reader] using hq
  by_cases h37 : b == 37
  · have hb : b = 37 := by simpa using h37
    simp [reader, hb]
  · have hb : b ≠ 37 := by simpa using h37
    by_cases hhex : (48 ≤ b && b ≤ 57) || (65 ≤ b && b ≤ 70) || (97 ≤ b && b ≤ 102)
    · have hhex' : ((48 ≤ b ∧ b ≤ 57) ∨ (65 ≤ b ∧ b ≤ 70)) ∨ (97 ≤ b ∧ b ≤ 102) := by
        simpa using hhex
      by_cases h12 : q == 1 || q == 2
      · have hq12 : q = 1 ∨ q = 2 := by simpa using h12
        simp [reader, hb, hhex', hq12]
        rcases hq12 with rfl | rfl <;> omega
      · have hnq12 : ¬(q = 1 ∨ q = 2) := by simpa using h12
        simp [reader, hb, hhex', hnq12]
    · have hhex' : ¬(((48 ≤ b ∧ b ≤ 57) ∨ (65 ≤ b ∧ b ≤ 70)) ∨ (97 ≤ b ∧ b ≤ 102)) := by
        simpa using hhex
      simp [reader, hb, hhex']

private theorem pe_matched_fold (x : Rope) (q : Nat) (hq : q < reader.states) :
    matched reader x q = reader.accept ((expand x).foldl reader.step q) := by
  unfold matched
  rw [pe_compile_apply reader pe_reader_closed x q hq]

/-! ## The goal. -/

theorem sound_DictionaryMask : PercentEscape.DictionaryMask.Goal := by
  intro d ids q hq
  unfold rows matched
  apply List.map_congr_left
  intro i hi
  simp only [List.getElem?_map]
  cases hd : d[i]? with
  | none => rfl
  | some x => simpa [hd, matched] using pe_matched_fold x q hq

end TinylambOpt.ResearchProofs.StorageExecution.Goal_GrammarScans_PercentEscape_DictionaryMask
