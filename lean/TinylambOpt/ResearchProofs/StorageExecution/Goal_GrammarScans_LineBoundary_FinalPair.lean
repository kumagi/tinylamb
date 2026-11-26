import TinylambOpt.ResearchGoals.StorageExecution.GrammarScans

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_GrammarScans_LineBoundary_FinalPair

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.GrammarScans
open TinylambOpt.ResearchGoals.StorageExecution.GrammarScans.LineBoundary

/-! ## Small-input truth check -/

private def alphabet : List Nat := [10, 13, 65]

private def wordLen : Nat → List Bytes
  | 0 => [[]]
  | n + 1 => (wordLen n).flatMap (fun s => alphabet.map (fun b => s ++ [b]))

private def byteSamples : List Bytes :=
  (List.range 6).flatMap wordLen

private def truthCheck : Bool :=
  byteSamples.all fun s =>
    decide ((s.foldl reader.step 0 == 2) == ([13, 10].isSuffixOf s))

#eval truthCheck

private def ropeSamples : List Rope :=
  [ .lit [], .lit [13], .lit [10], .lit [13, 10], .lit [65, 13, 10],
    .lit [13, 10, 65], .cat (.lit [65, 13]) (.lit [10]),
    .cat (.lit [13]) (.lit [10, 65]), .rep 0 (.lit [13, 10]),
    .rep 2 (.lit [13, 10]), .rep 3 (.lit [65]),
    .cat (.rep 2 (.lit [13])) (.lit [10]),
    .cat (.lit [13, 10]) (.rep 2 (.lit [65])) ]

private def ropeCheck : Bool :=
  ropeSamples.all fun x => decide (matched reader x 0 == [13, 10].isSuffixOf (expand x))

#eval ropeCheck

/-! ## Bool suffix / Prop suffix bridge -/

private theorem fp_isPrefixOf_true_iff (a b : List Nat) :
    a.isPrefixOf b = true ↔ a <+: b := by
  induction a with
  | nil => simp [List.isPrefixOf]
  | cons x xs ih =>
      cases b with
      | nil => simp [List.isPrefixOf]
      | cons y ys =>
          simp [List.isPrefixOf, List.isPrefixOf._f, ih, List.prefix_cons_iff]
          constructor
          · intro h
            exact ⟨xs, ⟨h.1, rfl⟩, h.2⟩
          · intro h
            obtain ⟨t, ⟨hxy, hxt⟩, ht⟩ := h
            exact ⟨hxy, by simpa [hxt] using ht⟩

private theorem fp_isSuffixOf_true_iff (a b : List Nat) :
    a.isSuffixOf b = true ↔ a <:+ b := by
  unfold List.isSuffixOf
  rw [fp_isPrefixOf_true_iff, List.reverse_prefix]

/-! ## The byte-level CRLF state invariant -/

private theorem fp_step0_13 : reader.step 0 13 = 1 := by decide
private theorem fp_step1_13 : reader.step 1 13 = 1 := by decide
private theorem fp_step2_13 : reader.step 2 13 = 1 := by decide
private theorem fp_step0_10 : reader.step 0 10 = 0 := by decide
private theorem fp_step1_10 : reader.step 1 10 = 2 := by decide
private theorem fp_step2_10 : reader.step 2 10 = 0 := by decide
private theorem fp_step0_other (b : Nat) (hb : b ≠ 13) : reader.step 0 b = 0 := by
  simp [reader, hb]
private theorem fp_step1_other (b : Nat) (hb : b ≠ 13) (h10 : b ≠ 10) :
    reader.step 1 b = 0 := by
  simp [reader, hb, h10]
private theorem fp_step2_other (b : Nat) (hb : b ≠ 13) : reader.step 2 b = 0 := by
  simp [reader, hb]

private theorem fp_or_eq_comm (a b : List Nat) (P : Prop) :
    (a = b ∨ P) ↔ (b = a ∨ P) := by
  constructor <;> intro h <;> rcases h with h | h
  · exact Or.inl h.symm
  · exact Or.inr h
  · exact Or.inl h.symm
  · exact Or.inr h

private theorem fp_fold_spec (s : List Nat) :
    (s.foldl reader.step 0 = 2 ↔ [13, 10] <:+ s) ∧
    (s.foldl reader.step 1 = 2 ↔ s = [10] ∨ [13, 10] <:+ s) ∧
    (s.foldl reader.step 2 = 2 ↔ s = [] ∨ [13, 10] <:+ s) := by
  induction s with
  | nil => simp [List.suffix_cons_iff]
  | cons b t ih =>
      obtain ⟨h0, h1, h2⟩ := ih
      by_cases hb : b = 13
      · subst b
        constructor
        · change (13 :: t).foldl reader.step 0 = 2 ↔ [13, 10] <:+ (13 :: t)
          rw [List.foldl_cons, fp_step0_13, h1]
          simp [List.suffix_cons_iff]
          exact fp_or_eq_comm _ _ _
        · constructor
          · change (13 :: t).foldl reader.step 1 = 2 ↔ (13 :: t) = [10] ∨ [13, 10] <:+ (13 :: t)
            rw [List.foldl_cons, fp_step1_13, h1]
            simp [List.suffix_cons_iff]
            exact fp_or_eq_comm _ _ _
          · change (13 :: t).foldl reader.step 2 = 2 ↔ (13 :: t) = [] ∨ [13, 10] <:+ (13 :: t)
            rw [List.foldl_cons, fp_step2_13, h1]
            simp [List.suffix_cons_iff]
            exact fp_or_eq_comm _ _ _
      · by_cases h10 : b = 10
        · subst b
          constructor
          · change (10 :: t).foldl reader.step 0 = 2 ↔ [13, 10] <:+ (10 :: t)
            rw [List.foldl_cons, fp_step0_10, h0]
            simp [List.suffix_cons_iff]
          · constructor
            · change (10 :: t).foldl reader.step 1 = 2 ↔ (10 :: t) = [10] ∨ [13, 10] <:+ (10 :: t)
              rw [List.foldl_cons, fp_step1_10, h2]
              simp [List.suffix_cons_iff]
            · change (10 :: t).foldl reader.step 2 = 2 ↔ (10 :: t) = [] ∨ [13, 10] <:+ (10 :: t)
              rw [List.foldl_cons, fp_step2_10, h0]
              simp [List.suffix_cons_iff]
        · have hb' : 13 ≠ b := by omega
          constructor
          · change (b :: t).foldl reader.step 0 = 2 ↔ [13, 10] <:+ (b :: t)
            rw [List.foldl_cons, fp_step0_other b hb, h0]
            simp [List.suffix_cons_iff, hb']
          · constructor
            · change (b :: t).foldl reader.step 1 = 2 ↔ (b :: t) = [10] ∨ [13, 10] <:+ (b :: t)
              rw [List.foldl_cons, fp_step1_other b hb h10, h0]
              simp [List.suffix_cons_iff, hb, h10, hb']
            · change (b :: t).foldl reader.step 2 = 2 ↔ (b :: t) = [] ∨ [13, 10] <:+ (b :: t)
              rw [List.foldl_cons, fp_step2_other b hb, h0]
              simp [List.suffix_cons_iff, hb']

/-! ## Self-contained transition-table / compilation facts -/

private theorem fp_sum_replicate_nat (n m : Nat) :
    (List.replicate n m).sum = n * m := by
  induction n with
  | zero => simp
  | succ n ih => simp [List.replicate_succ, ih, Nat.succ_mul, Nat.add_comm]

private theorem fp_length_expand (x : Rope) : extent x = (expand x).length := by
  induction x with
  | lit s => rfl
  | cat a b iha ihb => simp [extent, expand, iha, ihb]
  | rep k a iha =>
      simp only [extent, expand, List.length_flatten, List.map_replicate,
        fp_sum_replicate_nat, iha]

private theorem fp_power_length (n : Nat) (t : Table) :
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

private theorem fp_compile_length (r : Reader) (x : Rope) :
    (compile r x).length = r.states := by
  induction x with
  | lit s => simp [compile]
  | cat a b iha ihb => simp [compile, chain, iha, ihb]
  | rep k a ih => simp [compile, fp_power_length, ih]

private theorem fp_applyT_chain (a b : Table) (q : Nat) (hq : q < a.length) :
    applyT (chain a b) q = applyT b (applyT a q) := by
  simp [applyT, chain, List.getElem?_map, List.getElem?_eq_getElem hq]

private def fp_Closed (r : Reader) : Prop :=
  ∀ q b, q < r.states → r.step q b < r.states

private theorem fp_foldl_closed (r : Reader) (h : fp_Closed r) (s : Bytes) (q : Nat)
    (hq : q < r.states) : s.foldl r.step q < r.states := by
  induction s generalizing q with
  | nil => exact hq
  | cons b bs ih => exact ih (r.step q b) (h q b hq)

private theorem fp_flatten_replicate_add (a b : Nat) (s : Bytes) :
    (List.replicate (a + b) s).flatten =
      (List.replicate a s).flatten ++ (List.replicate b s).flatten := by
  induction a with
  | zero => simp
  | succ a ih =>
      rw [Nat.succ_add, List.replicate_succ, List.flatten_cons, ih,
        List.replicate_succ, List.flatten_cons]
      simp [List.append_assoc]

private theorem fp_power_apply_of (r : Reader) (h : fp_Closed r) (x : Rope)
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
        rw [fp_compile_length]
        change (List.range r.states)[q]?.getD q = q
        simp [applyT, List.getElem?_range hq]
      · rename_i hn
        let m := n / 2
        let ht := power m (compile r x)
        have hm : m < n := Nat.div_lt_self (by omega) (by decide)
        have ih' := ih m hm
        have hlen : ht.length = r.states := by
          simp [ht, fp_power_length, fp_compile_length]
        split
        · rename_i heven
          change applyT (chain ht ht) q =
            ((List.replicate n (expand x)).flatten).foldl r.step q
          have heq : n = m + m := by
            change n = n / 2 + n / 2
            have hd := Nat.div_add_mod n 2
            have hmod0 : n % 2 = 0 := by simpa using heven
            omega
          rw [heq, fp_flatten_replicate_add, List.foldl_append]
          rw [fp_applyT_chain ht ht q (by rw [hlen]; exact hq), ih' q hq]
          exact ih' _ (fp_foldl_closed r h
            ((List.replicate m (expand x)).flatten) q hq)
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
          rw [fp_applyT_chain (chain ht ht) (compile r x) q
            (by simp only [chain, List.length_map]; rw [hlen]; exact hq)]
          rw [fp_applyT_chain ht ht q (by rw [hlen]; exact hq), ih' q hq]
          rw [ih' _ (fp_foldl_closed r h
            ((List.replicate m (expand x)).flatten) q hq)]
          simp only [List.foldl_nil]
          rw [fp_flatten_replicate_add, List.foldl_append]
          have hq1 := fp_foldl_closed r h
            ((List.replicate m (expand x)).flatten) q hq
          have hq2 := fp_foldl_closed r h
            ((List.replicate m (expand x)).flatten) _ hq1
          rw [ha _ hq2]

private theorem fp_compile_apply (r : Reader) (h : fp_Closed r) (x : Rope) (q : Nat)
    (hq : q < r.states) :
    applyT (compile r x) q = (expand x).foldl r.step q := by
  induction x generalizing q with
  | lit s =>
      simp only [compile, applyT, List.getElem?_map,
        List.getElem?_range (by simpa using hq), Option.map_some, Option.getD_some]
      rfl
  | cat a b iha ihb =>
      rw [compile, fp_applyT_chain _ _ q (by simpa [fp_compile_length] using hq),
        iha q hq]
      rw [ihb _ (fp_foldl_closed r h (expand a) q hq)]
      simp [expand, List.foldl_append]
  | rep k a ih =>
      rw [compile]
      exact fp_power_apply_of r h a ih k q hq

private theorem fp_line_closed : fp_Closed reader := by
  intro q b hq
  change (if b == 13 then 1 else if q == 1 && b == 10 then 2 else 0) < 3
  split
  · omega
  · split
    · omega
    · omega

/-! ## The goal -/

theorem sound_FinalPair : LineBoundary.FinalPair.Goal := by
  intro x
  unfold matched
  rw [fp_compile_apply reader fp_line_closed x 0 (by decide : 0 < reader.states)]
  change ((expand x).foldl reader.step 0 == 2) = [13, 10].isSuffixOf (expand x)
  apply Bool.eq_iff_iff.mpr
  rw [fp_isSuffixOf_true_iff]
  have hs := (fp_fold_spec (expand x)).1
  simp [hs]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_GrammarScans_LineBoundary_FinalPair
