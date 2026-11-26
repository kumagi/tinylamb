import TinylambOpt.ResearchGoals.StorageExecution.GrammarScans
import TinylambOpt.ResearchProofs.Compression.Wire

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.StorageExecution.GrammarScansA

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.GrammarScans

/-! Shared transition-table and rope facts. -/

private theorem gs_sum_replicate_nat (n m : Nat) :
    (List.replicate n m).sum = n * m := by
  induction n with
  | zero => simp
  | succ n ih => simp [List.replicate_succ, ih, Nat.succ_mul, Nat.add_comm]

private theorem gs_length_expand (x : Rope) : extent x = (expand x).length := by
  induction x with
  | lit s => rfl
  | cat a b iha ihb => simp [extent, expand, iha, ihb]
  | rep k a iha =>
      simp only [extent, expand, List.length_flatten, List.map_replicate,
        gs_sum_replicate_nat, iha]

private theorem gs_power_length (n : Nat) (t : Table) :
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

private theorem gs_compile_length (r : Reader) (x : Rope) :
    (compile r x).length = r.states := by
  induction x with
  | lit s => simp [compile]
  | cat a b iha ihb => simp [compile, chain, iha, ihb]
  | rep k a ih => simp [compile, gs_power_length, ih]

private theorem gs_applyT_chain (a b : Table) (q : Nat) (hq : q < a.length) :
    applyT (chain a b) q = applyT b (applyT a q) := by
  simp [applyT, chain, List.getElem?_map, List.getElem?_eq_getElem hq]

private def GsClosed (r : Reader) : Prop :=
  ∀ q b, q < r.states → r.step q b < r.states

private theorem gs_foldl_closed (r : Reader) (h : GsClosed r) (s : Bytes) (q : Nat)
    (hq : q < r.states) : s.foldl r.step q < r.states := by
  induction s generalizing q with
  | nil => exact hq
  | cons b bs ih => exact ih (r.step q b) (h q b hq)

private theorem gs_chain_bound (r : Reader) (h : GsClosed r) (a b : Table)
    (ha : ∀ q ∈ a, q < r.states) (hb : ∀ q ∈ b, q < r.states) :
    ∀ q ∈ chain a b, q < r.states := by
  intro q hq
  rw [chain, List.mem_map] at hq
  obtain ⟨i, hi, heq⟩ := hq
  rw [← heq]
  unfold applyT
  by_cases hib : i < b.length
  · rw [List.getElem?_eq_getElem hib]
    exact hb b[i] (List.getElem_mem hib)
  · rw [List.getElem?_eq_none (by omega)]
    exact ha i hi

private theorem gs_compile_bound (r : Reader) (h : GsClosed r) (x : Rope) :
    ∀ q ∈ compile r x, q < r.states := by
  induction x with
  | lit s =>
      intro q hq
      simp only [compile, List.mem_map] at hq
      obtain ⟨i, hi, rfl⟩ := hq
      exact gs_foldl_closed r h s i (List.mem_range.mp hi)
  | cat a b iha ihb =>
      exact gs_chain_bound r h (compile r a) (compile r b) iha ihb
  | rep k a ih =>
      change ∀ q, q ∈ power k (compile r a) → q < r.states
      induction k using Nat.strongRecOn with
      | ind k ihk =>
          unfold power
          split
          · intro q hq
            exact List.mem_range.mp (by simpa [gs_compile_length] using hq)
          · rename_i hk
            split
            · exact gs_chain_bound r h _ _
                (ihk (k / 2) (Nat.div_lt_self (by omega) (by decide)))
                (ihk (k / 2) (Nat.div_lt_self (by omega) (by decide)))
            · exact gs_chain_bound r h (chain (power (k / 2) (compile r a))
                  (power (k / 2) (compile r a))) (compile r a)
                (gs_chain_bound r h _ _
                  (ihk (k / 2) (Nat.div_lt_self (by omega) (by decide)))
                  (ihk (k / 2) (Nat.div_lt_self (by omega) (by decide)))) ih

private theorem gs_flatten_replicate_add (a b : Nat) (s : Bytes) :
    (List.replicate (a + b) s).flatten =
      (List.replicate a s).flatten ++ (List.replicate b s).flatten := by
  induction a with
  | zero => simp
  | succ a ih =>
      rw [Nat.succ_add, List.replicate_succ, List.flatten_cons, ih,
        List.replicate_succ, List.flatten_cons]
      simp [List.append_assoc]

private theorem gs_power_apply_of (r : Reader) (h : GsClosed r) (x : Rope)
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
        rw [gs_compile_length]
        change (List.range r.states)[q]?.getD q = q
        simp [applyT, List.getElem?_range hq]
      · rename_i hn
        let m := n / 2
        let ht := power m (compile r x)
        have hm : m < n := Nat.div_lt_self (by omega) (by decide)
        have ih' := ih m hm
        have hlen : ht.length = r.states := by
          simp [ht, gs_power_length, gs_compile_length]
        split
        · rename_i heven
          change applyT (chain ht ht) q =
            ((List.replicate n (expand x)).flatten).foldl r.step q
          have hmod0 : n % 2 = 0 := by simpa using heven
          have heq : n = m + m := by
            change n = n / 2 + n / 2
            have hd := Nat.div_add_mod n 2
            omega
          rw [heq, gs_flatten_replicate_add, List.foldl_append]
          rw [gs_applyT_chain ht ht q (by rw [hlen]; exact hq), ih' q hq]
          exact ih' _ (gs_foldl_closed r h
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
          rw [gs_applyT_chain (chain ht ht) (compile r x) q
            (by simp only [chain, List.length_map]; rw [hlen]; exact hq)]
          rw [gs_applyT_chain ht ht q (by rw [hlen]; exact hq), ih' q hq]
          rw [ih' _ (gs_foldl_closed r h
            ((List.replicate m (expand x)).flatten) q hq)]
          simp only [List.foldl_nil]
          rw [gs_flatten_replicate_add, List.foldl_append]
          have hq1 := gs_foldl_closed r h
            ((List.replicate m (expand x)).flatten) q hq
          have hq2 := gs_foldl_closed r h
            ((List.replicate m (expand x)).flatten) _ hq1
          rw [ha _ hq2]

private theorem gs_compile_apply (r : Reader) (h : GsClosed r) (x : Rope) (q : Nat)
    (hq : q < r.states) :
    applyT (compile r x) q = (expand x).foldl r.step q := by
  induction x generalizing q with
  | lit s =>
      simp only [compile, applyT, List.getElem?_map,
        List.getElem?_range (by simpa using hq), Option.map_some, Option.getD_some]
      rfl
  | cat a b iha ihb =>
      rw [compile, gs_applyT_chain _ _ q (by simpa [gs_compile_length] using hq),
        iha q hq]
      rw [ihb _ (gs_foldl_closed r h (expand a) q hq)]
      simp [expand, List.foldl_append]
  | rep k a ih =>
      rw [compile]
      exact gs_power_apply_of r h a ih k q hq

private theorem gs_power_apply (r : Reader) (h : GsClosed r) (x : Rope) (n : Nat)
    (q : Nat) (hq : q < r.states) :
    applyT (power n (compile r x)) q =
      ((List.replicate n (expand x)).flatten).foldl r.step q := by
  have hv := gs_compile_apply r h (Rope.rep n x) q hq
  simpa only [compile, expand] using hv

private theorem gs_take_flatten_replicate_part (k n : Nat) (s : List Nat)
    (hs : 0 < s.length) (hlt : n < k * s.length) :
    ((List.replicate k s).flatten).take n =
      (List.replicate (n / s.length) s).flatten ++ s.take (n % s.length) := by
  induction k using Nat.strongRecOn generalizing n with
  | ind k ih =>
      cases k with
      | zero => simp at hlt
      | succ k =>
          rw [List.replicate_succ, List.flatten_cons, List.take_append]
          by_cases hn : n ≤ s.length
          · by_cases heq : n = s.length
            · subst n
              rw [Nat.sub_self, List.take_zero, List.append_nil,
                Nat.div_self hs, Nat.mod_self s.length]
              simp [List.replicate_succ, List.flatten_cons]
            · have hltn : n < s.length := by omega
              rw [Nat.sub_eq_zero_of_le hn, List.take_zero, List.append_nil,
                Nat.div_eq_of_lt hltn, Nat.mod_eq_of_lt hltn]
              simp
          · have hgt : s.length < n := by omega
            have hsub : n - s.length < k * s.length := by
              rw [Nat.succ_mul] at hlt
              omega
            have ih' := ih k (Nat.lt_succ_self k) (n - s.length) hsub
            rw [ih']
            have hrec : n = s.length + (n - s.length) := by omega
            have hdiv : n / s.length = (n - s.length) / s.length + 1 :=
              (congrArg (fun t => t / s.length) hrec).trans
                (Nat.add_div_left (n - s.length) hs)
            have hmod : n % s.length = (n - s.length) % s.length :=
              (congrArg (fun t => t % s.length) hrec).trans
                (Nat.add_mod_left s.length (n - s.length))
            rw [hdiv, hmod, List.replicate_succ, List.flatten_cons]
            simp [List.append_assoc, List.take_of_length_le (Nat.le_of_lt hgt)]

private theorem gs_prefixSeek_of_closed (r : Reader) (h : GsClosed r) :
    prefixSeek r := by
  intro x q n hq
  induction x generalizing q n with
  | lit s => rfl
  | cat a b iha ihb =>
      rw [partialScan]
      by_cases hn : n ≤ extent a
      · rw [if_pos hn, iha q n hq]
        rw [expand, List.take_append_of_le_length (by simpa [gs_length_expand] using hn)]
      · rw [if_neg hn, gs_compile_apply r h a q hq]
        rw [expand, List.take_append,
          List.take_of_length_le (by simpa [gs_length_expand] using Nat.le_of_not_ge hn),
          List.foldl_append]
        simpa [gs_length_expand] using
          ihb _ (n - extent a) (gs_foldl_closed r h (expand a) q hq)
  | rep k a iha =>
      rw [partialScan]
      by_cases hs : extent a = 0
      · rw [if_pos (by simp [hs])]
        have hlen : (expand a).length = 0 := by rw [← gs_length_expand, hs]
        have hempty : expand a = [] := List.length_eq_zero_iff.mp hlen
        change q = (((List.replicate k (expand a)).flatten).take n).foldl r.step q
        rw [hempty]
        simp
      · rw [if_neg (by simp [hs])]
        have hs0 : 0 < extent a := Nat.pos_of_ne_zero hs
        by_cases hfull : min k (n / extent a) = k
        · rw [if_pos (by simpa using hfull), hfull]
          have hkdiv : k ≤ n / extent a := by
            cases Nat.le_total k (n / extent a) with
            | inl h => exact h
            | inr h =>
                have hmin := Nat.min_eq_right h
                omega
          have hnge : k * extent a ≤ n := by
            rw [Nat.mul_comm k (extent a)]
            have hmul := Nat.mul_le_mul_left (extent a) hkdiv
            have hdivle := Nat.mul_div_le n (extent a)
            omega
          rw [gs_power_apply r h a k q hq]
          rw [List.take_of_length_le (by
            rw [← gs_length_expand (Rope.rep k a)]
            simp only [extent, List.length_flatten, List.map_replicate,
              gs_sum_replicate_nat]
            exact hnge)]
          rfl
        · rw [if_neg (by simpa using hfull)]
          have hnk : n / extent a < k := by
            cases Nat.le_total (n / extent a) k with
            | inl h =>
                have hmin := Nat.min_eq_right h
                omega
            | inr h =>
                have hmin := Nat.min_eq_left h
                exact False.elim (hfull hmin)
          have hnlt : n < k * extent a := by
            have hd := Nat.div_add_mod n (extent a)
            have hmod := Nat.mod_lt n hs0
            have hsucc := Nat.succ_le_of_lt hnk
            have hmul := Nat.mul_le_mul_left (extent a) hsucc
            calc n = extent a * (n / extent a) + n % extent a := hd.symm
              _ < extent a * (n / extent a) + extent a :=
                Nat.add_lt_add_left hmod _
              _ = extent a * (n / extent a).succ := by rw [Nat.mul_succ]
              _ ≤ extent a * k := hmul
              _ = k * extent a := Nat.mul_comm _ _
          rw [Nat.min_eq_right (Nat.le_of_lt hnk)]
          change partialScan r a (n % extent a)
            (applyT (power (n / extent a) (compile r a)) q) = _
          rw [gs_power_apply r h a (n / extent a) q hq, expand]
          rw [gs_take_flatten_replicate_part k n (expand a)
            (by simpa [gs_length_expand] using hs0) (by simpa [gs_length_expand] using hnlt)]
          rw [← gs_length_expand a, List.foldl_append]
          exact iha _ (n % extent a) (gs_foldl_closed r h
            ((List.replicate (n / extent a) (expand a)).flatten) q hq)

private theorem gs_repeatedState_of_closed (r : Reader) (h : GsClosed r) :
    repeatedState r := by
  intro x n q hq
  exact gs_power_apply r h x n q hq

private theorem gs_checkpoint_of_prefixSeek (r : Reader) (hp : prefixSeek r) :
    checkpoint r := by
  intro x q a b hq
  rw [hp x q (a + b) hq, hp x q a hq, ← List.foldl_append, List.take_add]

private theorem gs_expand_reverse (x : Rope) :
    expand (reverse x) = (expand x).reverse := by
  induction x with
  | lit s => simp [reverse, expand]
  | cat a b iha ihb => simp [reverse, expand, iha, ihb, List.reverse_append]
  | rep k a iha =>
      simp only [reverse, expand, iha, List.reverse_flatten]
      simp

private theorem gs_reverseScan_of_closed (r : Reader) (h : GsClosed r) :
    reverseScan r := by
  intro x q hq
  unfold matched
  rw [gs_compile_apply r h (reverse x) q hq, gs_expand_reverse]
  rfl

private theorem gs_dictionaryMask_of_closed (r : Reader) (h : GsClosed r) :
    dictionaryMask r := by
  intro d ids q hq
  unfold rows matched
  apply List.map_congr_left
  intro i hi
  simp only [List.getElem?_map]
  cases hd : d[i]? with
  | none => rfl
  | some x => simp [hd, gs_compile_apply r h x q hq]

private theorem gs_contextReuse_of_closed (r : Reader) (h : GsClosed r) :
    contextReuse r := by
  intro a b x y q hq hxy
  rw [← gs_compile_apply r h (Rope.cat a (Rope.cat x b)) q hq,
    ← gs_compile_apply r h (Rope.cat a (Rope.cat y b)) q hq]
  congr 1
  simp only [compile, chain, hxy]

private theorem gs_flatten_replicate_comm (a b : Nat) (s : List Nat) :
    (List.replicate a s).flatten ++ (List.replicate b s).flatten =
      (List.replicate b s).flatten ++ (List.replicate a s).flatten := by
  calc (List.replicate a s).flatten ++ (List.replicate b s).flatten
      = (List.replicate a s ++ List.replicate b s).flatten :=
        List.flatten_append.symm
    _ = (List.replicate b s ++ List.replicate a s).flatten := by
      rw [List.replicate_append_replicate, List.replicate_append_replicate,
        Nat.add_comm a b]
    _ = (List.replicate b s).flatten ++ (List.replicate a s).flatten :=
      List.flatten_append

private theorem gs_flatten_replicate_mul (k p : Nat) (s : List Nat) :
    (List.replicate (k * p) s).flatten =
      (List.replicate k ((List.replicate p s).flatten)).flatten := by
  induction k with
  | zero => simp
  | succ k ih =>
      rw [Nat.succ_mul, gs_flatten_replicate_add, ih, List.replicate_succ,
        List.flatten_cons]
      exact gs_flatten_replicate_comm k p s

private theorem gs_foldl_replicate_flatten_fixed (f : Nat → Nat → Nat)
    (s : List Nat) (p q k : Nat)
    (hblock : ((List.replicate p s).flatten).foldl f q = q) :
    ((List.replicate k ((List.replicate p s).flatten)).flatten).foldl f q = q := by
  induction k with
  | zero => simp
  | succ k ih =>
      simp only [List.replicate_succ, List.flatten_cons, List.foldl_cons]
      rw [List.foldl_append, hblock]
      exact ih

private theorem gs_periodJump_of_closed (r : Reader) (h : GsClosed r) :
    periodJump r := by
  intro x q period n hq hp hcycle
  unfold matched
  change r.accept (applyT (power n (compile r x)) q) =
    r.accept (applyT (power (n % period) (compile r x)) q)
  rw [gs_power_apply r h x n q hq,
    gs_power_apply r h x (n % period) q hq]
  have hblocks :
      ((List.replicate (period * (n / period)) (expand x)).flatten).foldl r.step q = q := by
    rw [Nat.mul_comm period (n / period), gs_flatten_replicate_mul]
    apply gs_foldl_replicate_flatten_fixed r.step
      ((List.replicate period (expand x)).flatten) 1 q (n / period)
    exact (gs_compile_apply r h (Rope.rep period x) q hq).symm.trans hcycle
  have hdec : n = period * (n / period) + n % period :=
    (Nat.div_add_mod n period).symm
  have hscan :
      ((List.replicate n (expand x)).flatten).foldl r.step q =
        ((List.replicate (n % period) (expand x)).flatten).foldl r.step q := by
    calc ((List.replicate n (expand x)).flatten).foldl r.step q
        = ((List.replicate (period * (n / period) + n % period) (expand x)).flatten).foldl r.step q :=
          congrArg (fun k => ((List.replicate k (expand x)).flatten).foldl r.step q) hdec
      _ = ((List.replicate (n % period) (expand x)).flatten).foldl r.step q := by
        rw [gs_flatten_replicate_add, List.foldl_append, hblocks]
  rw [hscan]

private theorem gs_lt_two_pow_width (n : Nat) : n < 2 ^ width n := by
  unfold width
  by_cases hn : n == 0
  · simp at hn
    subst hn
    decide
  · simp [hn]
    exact Nat.lt_log2_self

private theorem gs_tableBytes_of_closed (r : Reader) (h : GsClosed r) :
    tableBytes r := by
  intro x
  dsimp only
  let t := compile r x
  have htlen : t.length = r.states := gs_compile_length r x
  have hbound : ∀ v ∈ t, v < 2 ^ width (r.states - 1) := by
    intro v hv
    rw [List.mem_iff_getElem] at hv
    obtain ⟨i, hi, rfl⟩ := hv
    have hi' : i < r.states := by simpa [htlen] using hi
    have hval : t[i] = applyT t i := by simp [applyT, List.getElem?_eq_getElem hi]
    rw [hval, gs_compile_apply r h x i hi']
    have hbnd := gs_foldl_closed r h (expand x) i hi'
    have hle := Nat.le_pred_of_lt hbnd
    exact Nat.lt_of_le_of_lt hle (gs_lt_two_pow_width _)
  have hpi := TinylambOpt.ResearchProofs.Compression.Wire.sound_PackedInverse
    (width (r.states - 1)) t hbound
  simpa [htlen] using hpi

/-! ## ContainsSlash -/

private theorem gs_slash_closed : GsClosed ContainsSlash.reader := by
  intro q b hq
  change q < 2 at hq
  change (if b == 47 then 1 else q) < 2
  split <;> omega

private theorem gs_slash_fold (s : Bytes) (q : Nat) :
    s.foldl (fun q b => if b == 47 then 1 else q) q =
      if s.contains 47 then 1 else q := by
  induction s generalizing q with
  | nil => rfl
  | cons b bs ih =>
      rw [List.foldl_cons, List.contains_cons, ih]
      by_cases hb : b = 47
      · subst b
        simp
      · by_cases hm : 47 ∈ bs <;> simp [hb, hm] <;> omega

private theorem gs_slash_accept (s : Bytes) :
    (s.foldl (fun q b => if b == 47 then 1 else q) 0 == 1) = s.contains 47 := by
  rw [gs_slash_fold]
  by_cases hs : s.contains 47
  · rw [if_pos hs, hs]
    rfl
  · rw [if_neg hs]
    simpa [List.contains_iff_mem] using hs

private theorem gs_contains_flatten_replicate (s : Bytes) (k : Nat) (hk : 0 < k) :
    (List.replicate k s).flatten.contains 47 = s.contains 47 := by
  by_cases hs : s.contains 47
  · have hmem : 47 ∈ s := by simpa [List.contains_iff_mem] using hs
    cases k with
    | zero => omega
    | succ k =>
        rw [List.replicate_succ, List.flatten_cons]
        simp [List.contains_append, List.contains_iff_mem, hmem, hs]
  · have hnone : 47 ∉ s := by simpa [List.contains_iff_mem] using hs
    have hnflat : 47 ∉ (List.replicate k s).flatten := by
      intro hm
      obtain ⟨t, ht, hmem⟩ := List.mem_flatten.mp hm
      obtain ⟨_, rfl⟩ := List.mem_replicate.mp ht
      exact hnone hmem
    simp [hs, List.contains_iff_mem, hnflat]

theorem sound_ContainsSlash_SearchOnce :
    TinylambOpt.ResearchGoals.StorageExecution.GrammarScans.ContainsSlash.SearchOnce.Goal := by
  intro a k hk
  rw [sound_ContainsSlash_SlashWitness, sound_ContainsSlash_SlashWitness]
  change (expand (Rope.rep k a)).contains 47 = (expand a).contains 47
  rw [expand]
  exact gs_contains_flatten_replicate (expand a) k hk

theorem sound_ContainsSlash_RepeatedState :
    TinylambOpt.ResearchGoals.StorageExecution.GrammarScans.ContainsSlash.RepeatedState.Goal :=
  gs_repeatedState_of_closed ContainsSlash.reader gs_slash_closed

theorem sound_ContainsSlash_PrefixSeek :
    TinylambOpt.ResearchGoals.StorageExecution.GrammarScans.ContainsSlash.PrefixSeek.Goal :=
  gs_prefixSeek_of_closed ContainsSlash.reader gs_slash_closed

theorem sound_ContainsSlash_Checkpoint :
    TinylambOpt.ResearchGoals.StorageExecution.GrammarScans.ContainsSlash.Checkpoint.Goal :=
  gs_checkpoint_of_prefixSeek ContainsSlash.reader
    (gs_prefixSeek_of_closed ContainsSlash.reader gs_slash_closed)

theorem sound_ContainsSlash_ReverseScan :
    TinylambOpt.ResearchGoals.StorageExecution.GrammarScans.ContainsSlash.ReverseScan.Goal :=
  gs_reverseScan_of_closed ContainsSlash.reader gs_slash_closed

theorem sound_ContainsSlash_DictionaryMask :
    TinylambOpt.ResearchGoals.StorageExecution.GrammarScans.ContainsSlash.DictionaryMask.Goal :=
  gs_dictionaryMask_of_closed ContainsSlash.reader gs_slash_closed

theorem sound_ContainsSlash_ContextReuse :
    TinylambOpt.ResearchGoals.StorageExecution.GrammarScans.ContainsSlash.ContextReuse.Goal :=
  gs_contextReuse_of_closed ContainsSlash.reader gs_slash_closed

theorem sound_ContainsSlash_PeriodJump :
    TinylambOpt.ResearchGoals.StorageExecution.GrammarScans.ContainsSlash.PeriodJump.Goal :=
  gs_periodJump_of_closed ContainsSlash.reader gs_slash_closed

theorem sound_ContainsSlash_TableBytes :
    TinylambOpt.ResearchGoals.StorageExecution.GrammarScans.ContainsSlash.TableBytes.Goal :=
  gs_tableBytes_of_closed ContainsSlash.reader gs_slash_closed

theorem sound_ContainsSlash_SlashWitness :
    TinylambOpt.ResearchGoals.StorageExecution.GrammarScans.ContainsSlash.SlashWitness.Goal := by
  intro x
  unfold matched
  rw [gs_compile_apply ContainsSlash.reader gs_slash_closed x 0 (by decide)]
  change ((expand x).foldl (fun q b => if b == 47 then 1 else q) 0 == 1) =
    (expand x).contains 47
  exact gs_slash_accept (expand x)

#guard matched ContainsSlash.reader (Rope.cat (Rope.lit [47]) (Rope.lit [65])) 0
#guard matched ContainsSlash.reader (Rope.rep 100000 (Rope.lit [65])) 0 = false

/-! ## EndsInDigit -/

private theorem gs_digit_closed : GsClosed EndsInDigit.reader := by
  intro q b hq
  change (decide (48 ≤ b ∧ b ≤ 57)).toNat < 2
  by_cases h1 : 48 ≤ b <;> by_cases h2 : b ≤ 57 <;> simp [h1, h2]

private theorem gs_digit_bool_eq (b : Nat) :
    decide (48 ≤ b ∧ b ≤ 57) = (48 ≤ b && b ≤ 57) := by
  by_cases h1 : 48 ≤ b <;> by_cases h2 : b ≤ 57 <;> simp [h1, h2]

private theorem gs_digit_fold (s : Bytes) (hs : s ≠ []) (q : Nat) :
    s.foldl (fun _ b => (decide (48 ≤ b ∧ b ≤ 57)).toNat) q =
      (s.getLast?.any (fun b => 48 ≤ b && b ≤ 57)).toNat := by
  cases hlast : s.getLast? with
  | none => exact False.elim (hs (List.getLast?_eq_none_iff.mp hlast))
  | some b =>
      obtain ⟨pre, rfl⟩ := List.getLast?_eq_some_iff.mp hlast
      simp only [List.foldl_append, List.foldl_cons, List.foldl_nil,
        Option.any_some, gs_digit_bool_eq]

private theorem gs_digit_accept_of_nonempty (s : Bytes) (hs : s ≠ []) (q : Nat) :
    (s.foldl (fun _ b => (decide (48 ≤ b ∧ b ≤ 57)).toNat) q == 1) =
      s.getLast?.any (fun b => 48 ≤ b && b ≤ 57) := by
  rw [gs_digit_fold s hs q]
  cases h : s.getLast?.any (fun b => 48 ≤ b && b ≤ 57) <;> simp [h]

private theorem gs_digit_accept (s : Bytes) :
    (s.foldl (fun _ b => (decide (48 ≤ b ∧ b ≤ 57)).toNat) 0 == 1) =
      s.getLast?.any (fun b => 48 ≤ b && b ≤ 57) := by
  cases s with
  | nil => rfl
  | cons b bs => exact gs_digit_accept_of_nonempty (b :: bs) (by simp) 0

theorem sound_EndsInDigit_NonemptyTail :
    TinylambOpt.ResearchGoals.StorageExecution.GrammarScans.EndsInDigit.NonemptyTail.Goal := by
  intro a b hb
  have hne : expand b ≠ [] := by
    intro he
    have hlen := gs_length_expand b
    rw [he] at hlen
    simp at hlen
    omega
  unfold matched
  rw [gs_compile_apply EndsInDigit.reader gs_digit_closed (Rope.cat a b) 0 (by decide)]
  change ((expand a ++ expand b).foldl
      (fun _ b => (decide (48 ≤ b ∧ b ≤ 57)).toNat) 0 == 1) =
    ((expand b).foldl (fun _ b => (decide (48 ≤ b ∧ b ≤ 57)).toNat) 0 == 1)
  rw [List.foldl_append]
  exact gs_digit_accept_of_nonempty (expand b) hne _

theorem sound_EndsInDigit_RepeatedState :
    TinylambOpt.ResearchGoals.StorageExecution.GrammarScans.EndsInDigit.RepeatedState.Goal :=
  gs_repeatedState_of_closed EndsInDigit.reader gs_digit_closed

theorem sound_EndsInDigit_PrefixSeek :
    TinylambOpt.ResearchGoals.StorageExecution.GrammarScans.EndsInDigit.PrefixSeek.Goal :=
  gs_prefixSeek_of_closed EndsInDigit.reader gs_digit_closed

theorem sound_EndsInDigit_Checkpoint :
    TinylambOpt.ResearchGoals.StorageExecution.GrammarScans.EndsInDigit.Checkpoint.Goal :=
  gs_checkpoint_of_prefixSeek EndsInDigit.reader
    (gs_prefixSeek_of_closed EndsInDigit.reader gs_digit_closed)

theorem sound_EndsInDigit_ReverseScan :
    TinylambOpt.ResearchGoals.StorageExecution.GrammarScans.EndsInDigit.ReverseScan.Goal :=
  gs_reverseScan_of_closed EndsInDigit.reader gs_digit_closed

theorem sound_EndsInDigit_DictionaryMask :
    TinylambOpt.ResearchGoals.StorageExecution.GrammarScans.EndsInDigit.DictionaryMask.Goal :=
  gs_dictionaryMask_of_closed EndsInDigit.reader gs_digit_closed

theorem sound_EndsInDigit_ContextReuse :
    TinylambOpt.ResearchGoals.StorageExecution.GrammarScans.EndsInDigit.ContextReuse.Goal :=
  gs_contextReuse_of_closed EndsInDigit.reader gs_digit_closed

theorem sound_EndsInDigit_PeriodJump :
    TinylambOpt.ResearchGoals.StorageExecution.GrammarScans.EndsInDigit.PeriodJump.Goal :=
  gs_periodJump_of_closed EndsInDigit.reader gs_digit_closed

theorem sound_EndsInDigit_TableBytes :
    TinylambOpt.ResearchGoals.StorageExecution.GrammarScans.EndsInDigit.TableBytes.Goal :=
  gs_tableBytes_of_closed EndsInDigit.reader gs_digit_closed

theorem sound_EndsInDigit_LastByte :
    TinylambOpt.ResearchGoals.StorageExecution.GrammarScans.EndsInDigit.LastByte.Goal := by
  intro x
  unfold matched
  rw [gs_compile_apply EndsInDigit.reader gs_digit_closed x 0 (by decide)]
  change ((expand x).foldl (fun _ b => (decide (48 ≤ b ∧ b ≤ 57)).toNat) 0 == 1) =
    (expand x).getLast?.any (fun b => 48 ≤ b && b ≤ 57)
  exact gs_digit_accept (expand x)

#guard matched EndsInDigit.reader (Rope.cat (Rope.lit [49, 50]) (Rope.lit [57])) 0
#guard matched EndsInDigit.reader (Rope.lit [57, 65]) 0 = false

/-! ## QuotedField -/

private theorem gs_quote_closed : GsClosed QuotedField.reader := by
  intro q b hq
  change (if b == 34 then 1 - q else q) < 2
  split <;> omega

private theorem gs_quote_fold (s : Bytes) (q : Nat) (hq : q ≤ 1) :
    s.foldl (fun q b => if b == 34 then 1 - q else q) q =
      if s.count 34 % 2 = 0 then q else 1 - q := by
  induction s generalizing q with
  | nil => simp
  | cons b bs ih =>
      rw [List.foldl_cons, List.count_cons]
      by_cases hb : b = 34
      · subst b
        simp only [beq_self_eq_true, ↓reduceIte]
        rw [ih (1 - q) (by omega)]
        have hmod := Nat.mod_lt (bs.count 34) (by decide : 0 < 2)
        by_cases hp : bs.count 34 % 2 = 0
        · have hnext : (bs.count 34 + 1) % 2 = 1 := by omega
          simp [hp, hnext]
          omega
        · have hnext : (bs.count 34 + 1) % 2 = 0 := by omega
          simp [hp, hnext]
          omega
      · simpa [hb] using ih q hq

private theorem gs_quote_twice (s : Bytes) (q : Nat) (hq : q ≤ 1) :
    s.foldl (fun q b => if b == 34 then 1 - q else q)
      (s.foldl (fun q b => if b == 34 then 1 - q else q) q) = q := by
  rw [gs_quote_fold s q hq]
  by_cases hp : s.count 34 % 2 = 0
  · rw [if_pos hp, gs_quote_fold s q hq, if_pos hp]
  · rw [if_neg hp, gs_quote_fold s (1 - q) (by omega), if_neg hp]
    omega

private theorem gs_quote_accept (s : Bytes) :
    (s.foldl (fun q b => if b == 34 then 1 - q else q) 0 == 1) =
      decide (s.count 34 % 2 = 1) := by
  rw [gs_quote_fold s 0 (by omega)]
  have hmod := Nat.mod_lt (s.count 34) (by decide : 0 < 2)
  by_cases hp : s.count 34 % 2 = 0
  · simp [QuotedField.reader, hp]
  · have hp1 : s.count 34 % 2 = 1 := by omega
    simp [QuotedField.reader, hp, hp1]

theorem sound_QuotedField_DoublePhrase :
    TinylambOpt.ResearchGoals.StorageExecution.GrammarScans.QuotedField.DoublePhrase.Goal := by
  intro x
  apply List.ext_getElem
  · simp [gs_compile_length, QuotedField.reader]
  · intro q hq1 hq2
    have hq : q < QuotedField.reader.states := by simpa [gs_compile_length] using hq1
    have hv := gs_compile_apply QuotedField.reader gs_quote_closed (Rope.rep 2 x) q hq
    have hrep : expand (Rope.rep 2 x) = expand x ++ expand x := by simp [expand]
    rw [hrep, List.foldl_append] at hv
    have hget : (compile QuotedField.reader (Rope.rep 2 x))[q] =
        applyT (compile QuotedField.reader (Rope.rep 2 x)) q := by
      simp [applyT, List.getElem?_eq_getElem hq1]
    rw [hget, hv]
    exact gs_quote_twice (expand x) q (by omega)

theorem sound_QuotedField_RepeatedState :
    TinylambOpt.ResearchGoals.StorageExecution.GrammarScans.QuotedField.RepeatedState.Goal :=
  gs_repeatedState_of_closed QuotedField.reader gs_quote_closed

theorem sound_QuotedField_PrefixSeek :
    TinylambOpt.ResearchGoals.StorageExecution.GrammarScans.QuotedField.PrefixSeek.Goal :=
  gs_prefixSeek_of_closed QuotedField.reader gs_quote_closed

theorem sound_QuotedField_Checkpoint :
    TinylambOpt.ResearchGoals.StorageExecution.GrammarScans.QuotedField.Checkpoint.Goal :=
  gs_checkpoint_of_prefixSeek QuotedField.reader
    (gs_prefixSeek_of_closed QuotedField.reader gs_quote_closed)

theorem sound_QuotedField_ReverseScan :
    TinylambOpt.ResearchGoals.StorageExecution.GrammarScans.QuotedField.ReverseScan.Goal :=
  gs_reverseScan_of_closed QuotedField.reader gs_quote_closed

theorem sound_QuotedField_DictionaryMask :
    TinylambOpt.ResearchGoals.StorageExecution.GrammarScans.QuotedField.DictionaryMask.Goal :=
  gs_dictionaryMask_of_closed QuotedField.reader gs_quote_closed

theorem sound_QuotedField_ContextReuse :
    TinylambOpt.ResearchGoals.StorageExecution.GrammarScans.QuotedField.ContextReuse.Goal :=
  gs_contextReuse_of_closed QuotedField.reader gs_quote_closed

theorem sound_QuotedField_PeriodJump :
    TinylambOpt.ResearchGoals.StorageExecution.GrammarScans.QuotedField.PeriodJump.Goal :=
  gs_periodJump_of_closed QuotedField.reader gs_quote_closed

theorem sound_QuotedField_TableBytes :
    TinylambOpt.ResearchGoals.StorageExecution.GrammarScans.QuotedField.TableBytes.Goal :=
  gs_tableBytes_of_closed QuotedField.reader gs_quote_closed

theorem sound_QuotedField_QuoteParity :
    TinylambOpt.ResearchGoals.StorageExecution.GrammarScans.QuotedField.QuoteParity.Goal := by
  intro x
  unfold matched
  rw [gs_compile_apply QuotedField.reader gs_quote_closed x 0 (by decide)]
  change ((expand x).foldl (fun q b => if b == 34 then 1 - q else q) 0 == 1) =
    decide ((expand x).count 34 % 2 = 1)
  exact gs_quote_accept (expand x)

#guard compile QuotedField.reader (Rope.rep 2 (Rope.lit [34, 65, 34, 66])) = [0, 1]
#guard matched QuotedField.reader (Rope.rep 10001 (Rope.lit [34])) 0

/-! ## PercentEscape -/

private theorem gs_percent_closed : GsClosed PercentEscape.reader := by
  intro q b hq
  change (if b == 37 then 1
    else if (48 ≤ b && b ≤ 57) || (65 ≤ b && b ≤ 70) || (97 ≤ b && b ≤ 102) then
      if q == 1 || q == 2 then q + 1 else 0
    else 0) < 4
  split
  · omega
  · split
    · rename_i hhex
      split
      · rename_i hq12
        have hq' : q = 1 ∨ q = 2 := by simpa using hq12
        omega
      · omega
    · omega

private theorem gs_percent_two_percent (q : Nat) :
    [37, 37].foldl PercentEscape.reader.step q = 1 := by
  simp [PercentEscape.reader]

private theorem gs_percent_escape (q : Nat) :
    [37, 52, 70].foldl PercentEscape.reader.step q = 3 := by
  simp [PercentEscape.reader]

theorem sound_PercentEscape_LoneIntroducer :
    TinylambOpt.ResearchGoals.StorageExecution.GrammarScans.PercentEscape.LoneIntroducer.Goal := by
  intro x k
  rw [gs_prefixSeek_of_closed PercentEscape.reader gs_percent_closed
    (Rope.cat x (Rope.lit [37, 37])) (k % 4) (extent x + 2)
    (Nat.mod_lt _ (by decide))]
  rw [show (expand (Rope.cat x (Rope.lit [37, 37]))).take (extent x + 2) =
      expand x ++ [37, 37] by
    simp only [expand]
    apply List.take_of_length_le
    simp [gs_length_expand]]
  rw [List.foldl_append]
  exact gs_percent_two_percent _

theorem sound_PercentEscape_RepeatedState :
    TinylambOpt.ResearchGoals.StorageExecution.GrammarScans.PercentEscape.RepeatedState.Goal :=
  gs_repeatedState_of_closed PercentEscape.reader gs_percent_closed

theorem sound_PercentEscape_PrefixSeek :
    TinylambOpt.ResearchGoals.StorageExecution.GrammarScans.PercentEscape.PrefixSeek.Goal :=
  gs_prefixSeek_of_closed PercentEscape.reader gs_percent_closed

theorem sound_PercentEscape_Checkpoint :
    TinylambOpt.ResearchGoals.StorageExecution.GrammarScans.PercentEscape.Checkpoint.Goal :=
  gs_checkpoint_of_prefixSeek PercentEscape.reader
    (gs_prefixSeek_of_closed PercentEscape.reader gs_percent_closed)

theorem sound_PercentEscape_ReverseScan :
    TinylambOpt.ResearchGoals.StorageExecution.GrammarScans.PercentEscape.ReverseScan.Goal :=
  gs_reverseScan_of_closed PercentEscape.reader gs_percent_closed

theorem sound_PercentEscape_DictionaryMask :
    TinylambOpt.ResearchGoals.StorageExecution.GrammarScans.PercentEscape.DictionaryMask.Goal :=
  gs_dictionaryMask_of_closed PercentEscape.reader gs_percent_closed

theorem sound_PercentEscape_ContextReuse :
    TinylambOpt.ResearchGoals.StorageExecution.GrammarScans.PercentEscape.ContextReuse.Goal :=
  gs_contextReuse_of_closed PercentEscape.reader gs_percent_closed

theorem sound_PercentEscape_PeriodJump :
    TinylambOpt.ResearchGoals.StorageExecution.GrammarScans.PercentEscape.PeriodJump.Goal :=
  gs_periodJump_of_closed PercentEscape.reader gs_percent_closed

theorem sound_PercentEscape_TableBytes :
    TinylambOpt.ResearchGoals.StorageExecution.GrammarScans.PercentEscape.TableBytes.Goal :=
  gs_tableBytes_of_closed PercentEscape.reader gs_percent_closed

theorem sound_PercentEscape_EscapeAcrossBoundary :
    TinylambOpt.ResearchGoals.StorageExecution.GrammarScans.PercentEscape.EscapeAcrossBoundary.Goal := by
  intro x
  unfold matched
  rw [gs_compile_apply PercentEscape.reader gs_percent_closed
    (Rope.cat x (Rope.lit [37, 52, 70])) 0 (by decide)]
  change PercentEscape.reader.accept
    ((expand x ++ [37, 52, 70]).foldl PercentEscape.reader.step 0) = true
  rw [List.foldl_append, gs_percent_escape]
  rfl

#guard partialScan PercentEscape.reader (Rope.cat (Rope.lit [65]) (Rope.lit [37, 37])) 3 2 = 1
#guard matched PercentEscape.reader (Rope.cat (Rope.lit [37]) (Rope.lit [37, 52, 70])) 0

/-! ## LineBoundary -/

private theorem gs_line_closed : GsClosed LineBoundary.reader := by
  intro q b hq
  change (if b == 13 then 1 else if q == 1 && b == 10 then 2 else 0) < 3
  split <;> try split <;> omega

private def GsLineStep (q b : Nat) : Nat :=
  if b == 13 then 1 else if q == 1 && b == 10 then 2 else 0

private theorem gs_line_step2 (q b : Nat) :
    GsLineStep (GsLineStep q b) b = if b == 13 then 1 else 0 := by
  by_cases h13 : b = 13
  · subst b
    simp [GsLineStep]
  · by_cases h10 : b = 10
    · subst b
      by_cases hq : q = 1 <;> simp [GsLineStep, hq]
    · simp [GsLineStep, h13, h10]

private theorem gs_line_repeat_after_two (b m q : Nat) :
    (List.replicate m b).foldl GsLineStep (GsLineStep (GsLineStep q b) b) ≠ 2 := by
  rw [gs_line_step2]
  by_cases h13 : b = 13
  · subst b
    induction m with
    | zero => simp [GsLineStep]
    | succ m ih =>
        rw [List.replicate_succ, List.foldl_cons]
        simpa [GsLineStep] using ih
  · rw [if_neg (by simpa using h13)]
    induction m with
    | zero => simp
    | succ m ih =>
        rw [List.replicate_succ, List.foldl_cons]
        have hs : GsLineStep 0 b = 0 := by simp [GsLineStep, h13]
        rw [hs]
        exact ih

private theorem gs_flatten_replicate_singleton (k b : Nat) :
    (List.replicate k [b]).flatten = List.replicate k b := by
  induction k with
  | zero => rfl
  | succ k ih => simp [List.replicate_succ, List.flatten_cons, ih]

private theorem gs_line_suffix_append (pre : Bytes) (a b : Nat) :
    [13, 10].isSuffixOf (pre ++ [a, b]) = decide (a = 13 ∧ b = 10) := by
  by_cases ha : a = 13 <;> by_cases hb : b = 10
  · subst a
    subst b
    simp [List.isSuffixOf, List.isPrefixOf]
  · subst a
    simp [List.isSuffixOf, List.isPrefixOf, Bool.beq_eq_decide_eq, Ne.symm hb]
    exact hb
  · subst b
    simp [List.isSuffixOf, List.isPrefixOf, Bool.beq_eq_decide_eq, Ne.symm ha]
    exact ha
  · simp [List.isSuffixOf, List.isPrefixOf, Bool.beq_eq_decide_eq, Ne.symm ha, Ne.symm hb]
    intro _
    exact hb

private theorem gs_line_fold_pair (pre : Bytes) (a b : Nat) :
    (pre ++ [a, b]).foldl GsLineStep 0 =
      if a == 13 then (if b == 10 then 2 else if b == 13 then 1 else 0)
      else (if b == 13 then 1 else 0) := by
  rw [List.foldl_append]
  by_cases ha : a = 13 <;> by_cases hb13 : b = 13 <;> by_cases hb10 : b = 10 <;>
    simp [GsLineStep, ha, hb13, hb10] <;> split <;> simp [GsLineStep]

theorem sound_LineBoundary_RunBoundaryCRLF :
    TinylambOpt.ResearchGoals.StorageExecution.GrammarScans.LineBoundary.RunBoundaryCRLF.Goal := by
  intro x k hk hx
  have hlen : (expand x).length = 1 := by simpa [gs_length_expand] using hx
  obtain ⟨b, hb⟩ := List.length_eq_one_iff.mp hlen
  unfold matched
  rw [gs_compile_apply LineBoundary.reader gs_line_closed (Rope.rep k x) 0 (by decide)]
  change (((List.replicate k (expand x)).flatten).foldl GsLineStep 0 == 2) = false
  rw [hb, gs_flatten_replicate_singleton]
  cases k with
  | zero => omega
  | succ n =>
      cases n with
      | zero => omega
      | succ m =>
          rw [List.replicate_succ, List.replicate_succ, List.foldl_cons, List.foldl_cons]
          simp only [beq_eq_false_iff_ne]
          exact gs_line_repeat_after_two b m 0

theorem sound_LineBoundary_RepeatedState :
    TinylambOpt.ResearchGoals.StorageExecution.GrammarScans.LineBoundary.RepeatedState.Goal :=
  gs_repeatedState_of_closed LineBoundary.reader gs_line_closed

theorem sound_LineBoundary_PrefixSeek :
    TinylambOpt.ResearchGoals.StorageExecution.GrammarScans.LineBoundary.PrefixSeek.Goal :=
  gs_prefixSeek_of_closed LineBoundary.reader gs_line_closed

theorem sound_LineBoundary_Checkpoint :
    TinylambOpt.ResearchGoals.StorageExecution.GrammarScans.LineBoundary.Checkpoint.Goal :=
  gs_checkpoint_of_prefixSeek LineBoundary.reader
    (gs_prefixSeek_of_closed LineBoundary.reader gs_line_closed)

theorem sound_LineBoundary_ReverseScan :
    TinylambOpt.ResearchGoals.StorageExecution.GrammarScans.LineBoundary.ReverseScan.Goal :=
  gs_reverseScan_of_closed LineBoundary.reader gs_line_closed

theorem sound_LineBoundary_DictionaryMask :
    TinylambOpt.ResearchGoals.StorageExecution.GrammarScans.LineBoundary.DictionaryMask.Goal :=
  gs_dictionaryMask_of_closed LineBoundary.reader gs_line_closed

theorem sound_LineBoundary_ContextReuse :
    TinylambOpt.ResearchGoals.StorageExecution.GrammarScans.LineBoundary.ContextReuse.Goal :=
  gs_contextReuse_of_closed LineBoundary.reader gs_line_closed

theorem sound_LineBoundary_PeriodJump :
    TinylambOpt.ResearchGoals.StorageExecution.GrammarScans.LineBoundary.PeriodJump.Goal :=
  gs_periodJump_of_closed LineBoundary.reader gs_line_closed

theorem sound_LineBoundary_TableBytes :
    TinylambOpt.ResearchGoals.StorageExecution.GrammarScans.LineBoundary.TableBytes.Goal :=
  gs_tableBytes_of_closed LineBoundary.reader gs_line_closed

theorem sound_LineBoundary_FinalPair :
    TinylambOpt.ResearchGoals.StorageExecution.GrammarScans.LineBoundary.FinalPair.Goal := by
  intro x
  unfold matched
  rw [gs_compile_apply LineBoundary.reader gs_line_closed x 0 (by decide)]
  change ((expand x).foldl GsLineStep 0 == 2) = [13, 10].isSuffixOf (expand x)
  generalize hs : expand x = s at *
  cases s with
  | nil => rfl
  | cons b bs =>
      cases bs with
      | nil => simp [GsLineStep]
      | cons c rest =>
          have hrot : b :: c :: rest = rest ++ [b, c] := by
            induction rest with
            | nil => rfl
            | cons a rest ih => simp [ih]
          rw [hrot, gs_line_fold_pair, gs_line_suffix_append]
          by_cases hc : b = 13 <;> by_cases hl : c = 10 <;> simp [GsLineStep, hc, hl]

#guard matched LineBoundary.reader (Rope.cat (Rope.lit [65, 13]) (Rope.lit [10])) 0
#guard matched LineBoundary.reader (Rope.rep 1000 (Rope.lit [13])) 0 = false
#guard matched LineBoundary.reader (Rope.rep 1000 (Rope.lit [10])) 0 = false

end TinylambOpt.ResearchProofs.StorageExecution.GrammarScansA
