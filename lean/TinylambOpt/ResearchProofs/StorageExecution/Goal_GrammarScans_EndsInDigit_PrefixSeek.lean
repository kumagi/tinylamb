import TinylambOpt.ResearchGoals.StorageExecution.GrammarScans
import TinylambOpt.ResearchProofs.Compression.Wire

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_GrammarScans_EndsInDigit_PrefixSeek

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.GrammarScans
open TinylambOpt.ResearchGoals.StorageExecution.GrammarScans.EndsInDigit

/-! ## Small-input truth check (before the proof) -/

private def samples : List (Rope × Nat × Nat) :=
  [ (.lit [], 0, 0), (.lit [48], 1, 0), (.lit [65, 49, 66], 3, 1),
    (.cat (.lit [65]) (.lit [49, 66]), 5, 1),
    (.rep 3 (.lit [65, 49]), 4, 1), (.rep 2 (.lit [65]), 100, 1),
    (.cat (.rep 2 (.lit [65, 66])) (.lit [57]), 4, 0) ]

private def check : Bool :=
  samples.all fun p =>
    p.2.2 < reader.states &&
      partialScan reader p.1 p.2.1 p.2.2 ==
        ((expand p.1).take p.2.1).foldl reader.step p.2.2

#eval check

example : check := by native_decide

/-! ## Shared transition-table and rope facts (from GrammarScansA) -/

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

/-! ## Goal -/

private theorem digit_closed : GsClosed EndsInDigit.reader := by
  intro q b hq
  change (decide (48 ≤ b ∧ b ≤ 57)).toNat < 2
  by_cases h1 : 48 ≤ b <;> by_cases h2 : b ≤ 57 <;> simp [h1, h2]

theorem sound_PrefixSeek : EndsInDigit.PrefixSeek.Goal := by
  exact gs_prefixSeek_of_closed EndsInDigit.reader digit_closed

end TinylambOpt.ResearchProofs.StorageExecution.Goal_GrammarScans_EndsInDigit_PrefixSeek
