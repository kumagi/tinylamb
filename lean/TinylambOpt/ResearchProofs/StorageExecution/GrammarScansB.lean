import TinylambOpt.ResearchGoals.StorageExecution.GrammarScans

namespace TinylambOpt.ResearchProofs.StorageExecution.GrammarScansB

open TinylambOpt.ResearchGoals.StorageExecution.GrammarScans
open TinylambOpt.ResearchGoals.Compression.Wire

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

private theorem sum_replicate_nat (n m : Nat) :
    (List.replicate n m).sum = n * m := by
  induction n with
  | zero => simp
  | succ n ih => simp [List.replicate_succ, ih, Nat.succ_mul, Nat.add_comm]

private theorem length_expand (x : Rope) : extent x = (expand x).length := by
  induction x with
  | lit s => rfl
  | cat a b iha ihb => simp [expand, extent, iha, ihb]
  | rep k a ih =>
      simp only [expand, extent, List.length_flatten, List.map_replicate,
        sum_replicate_nat, ih]

private theorem expand_reverse (x : Rope) : expand (reverse x) = (expand x).reverse := by
  induction x with
  | lit s => simp [reverse, expand]
  | cat a b iha ihb => simp [reverse, expand, iha, ihb, List.reverse_append]
  | rep k a ih =>
      simp only [reverse, expand, ih]
      induction k with
      | zero => simp
      | succ k ihk =>
          simp [List.replicate_succ, List.flatten_cons, ihk, List.flatten_append,
            List.reverse_append]

private theorem power_length (n : Nat) (t : Table) :
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

private theorem foldl_closed (r : Reader) (h : Closed r) (s : List Nat) (q : Nat)
    (hq : q < r.states) : s.foldl r.step q < r.states := by
  induction s generalizing q with
  | nil => exact hq
  | cons b bs ih =>
      exact ih (r.step q b) (h q b hq)

private theorem chain_bound (r : Reader) (h : Closed r) (a b : Table)
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

private theorem compile_bound (r : Reader) (h : Closed r) (x : Rope) :
    ∀ q ∈ compile r x, q < r.states := by
  induction x with
  | lit s =>
      intro q hq
      simp only [compile, List.mem_map] at hq
      obtain ⟨i, hi, rfl⟩ := hq
      exact foldl_closed r h s i (List.mem_range.mp hi)
  | cat a b iha ihb =>
      exact chain_bound r h (compile r a) (compile r b) iha ihb
  | rep k a ih =>
      change ∀ q, q ∈ power k (compile r a) → q < r.states
      induction k using Nat.strongRecOn with
      | ind k ihk =>
          unfold power
          split
          · intro q hq
            exact List.mem_range.mp (by simpa [compile_length] using hq)
          · rename_i hk
            split
            · exact chain_bound r h _ _
                (ihk (k / 2) (Nat.div_lt_self (by omega) (by decide)))
                (ihk (k / 2) (Nat.div_lt_self (by omega) (by decide)))
            · exact chain_bound r h (chain (power (k / 2) (compile r a))
                  (power (k / 2) (compile r a))) (compile r a)
                (chain_bound r h _ _
                  (ihk (k / 2) (Nat.div_lt_self (by omega) (by decide)))
                  (ihk (k / 2) (Nat.div_lt_self (by omega) (by decide)))) ih

private theorem flatten_replicate_add (a b : Nat) (s : List Nat) :
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
          exact ih' _ (foldl_closed r h
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
          rw [applyT_chain (chain ht ht) (compile r x) q
            (by simp only [chain, List.length_map]; rw [hlen]; exact hq)]
          rw [applyT_chain ht ht q (by rw [hlen]; exact hq), ih' q hq]
          rw [ih' _ (foldl_closed r h
            ((List.replicate m (expand x)).flatten) q hq)]
          simp only [List.foldl_nil]
          rw [flatten_replicate_add, List.foldl_append]
          have hq1 := foldl_closed r h
            ((List.replicate m (expand x)).flatten) q hq
          have hq2 := foldl_closed r h
            ((List.replicate m (expand x)).flatten) _ hq1
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
      rw [compile, applyT_chain _ _ q (by simpa [compile_length] using hq), iha q hq]
      rw [ihb _ (foldl_closed r h (expand a) q hq)]
      simp [expand, List.foldl_append]
  | rep k a ih =>
      rw [compile]
      exact power_apply_of r h a ih k q hq

private theorem power_apply (r : Reader) (h : Closed r) (x : Rope) (n : Nat)
    (q : Nat) (hq : q < r.states) :
    applyT (power n (compile r x)) q =
      ((List.replicate n (expand x)).flatten).foldl r.step q :=
  compile_apply r h (.rep n x) q hq

private theorem take_flatten_replicate_part (k n : Nat) (s : List Nat)
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

private theorem prefixSeek_of_closed (r : Reader) (h : Closed r) : prefixSeek r := by
  intro x q n hq
  induction x generalizing q n with
  | lit s => rfl
  | cat a b iha ihb =>
      rw [partialScan]
      by_cases hn : n ≤ extent a
      · rw [if_pos hn, iha q n hq]
        rw [expand, List.take_append_of_le_length (by simpa [length_expand] using hn)]
      · rw [if_neg hn, compile_apply r h a q hq]
        rw [expand, List.take_append,
          List.take_of_length_le (by simpa [length_expand] using Nat.le_of_not_ge hn),
          List.foldl_append]
        simpa [length_expand] using
          ihb _ (n - extent a) (foldl_closed r h (expand a) q hq)
  | rep k a iha =>
      rw [partialScan]
      by_cases hs : extent a = 0
      · rw [if_pos (by simp [hs])]
        have hlen : (expand a).length = 0 := by
          rw [← length_expand, hs]
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
          rw [power_apply r h a k q hq]
          rw [List.take_of_length_le (by
            rw [← length_expand (.rep k a)]
            simp only [extent, List.length_flatten, List.map_replicate,
              sum_replicate_nat]
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
          rw [power_apply r h a (n / extent a) q hq, expand]
          rw [take_flatten_replicate_part k n (expand a)
            (by simpa [length_expand] using hs0) (by simpa [length_expand] using hnlt)]
          rw [← length_expand a, List.foldl_append]
          exact iha _ (n % extent a) (foldl_closed r h
            ((List.replicate (n / extent a) (expand a)).flatten) q hq)

private theorem repeatedState_of_closed (r : Reader) (h : Closed r) :
    repeatedState r := by
  intro x n q hq
  exact power_apply r h x n q hq

private theorem checkpoint_of_prefixSeek (r : Reader) (hp : prefixSeek r) :
    checkpoint r := by
  intro x q a b hq
  rw [hp x q (a + b) hq, hp x q a hq, ← List.foldl_append, List.take_add]

private theorem dictionaryMask_of_closed (r : Reader) (h : Closed r) :
    dictionaryMask r := by
  intro d ids q hq
  unfold rows matched
  apply List.map_congr_left
  intro i hi
  simp only [List.getElem?_map]
  cases hd : d[i]? with
  | none => rfl
  | some x => simp [hd, compile_apply r h x q hq]

private theorem contextReuse_of_closed (r : Reader) (h : Closed r) :
    contextReuse r := by
  intro a b x y q hq hxy
  rw [← compile_apply r h (.cat a (.cat x b)) q hq,
    ← compile_apply r h (.cat a (.cat y b)) q hq]
  congr 1
  simp only [compile, chain, hxy]

private theorem flatten_replicate_comm (a b : Nat) (s : List Nat) :
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

private theorem flatten_replicate_mul (k p : Nat) (s : List Nat) :
    (List.replicate (k * p) s).flatten =
      (List.replicate k ((List.replicate p s).flatten)).flatten := by
  induction k with
  | zero => simp
  | succ k ih =>
      rw [Nat.succ_mul, flatten_replicate_add, ih, List.replicate_succ,
        List.flatten_cons]
      simpa using flatten_replicate_comm k 1 ((List.replicate p s).flatten)

private theorem foldl_replicate_flatten_fixed (f : Nat → Nat → Nat)
    (s : List Nat) (p q k : Nat)
    (hblock : ((List.replicate p s).flatten).foldl f q = q) :
    ((List.replicate k ((List.replicate p s).flatten)).flatten).foldl f q = q := by
  induction k with
  | zero => simp
  | succ k ih =>
      simp only [List.replicate_succ, List.flatten_cons, List.foldl_cons]
      rw [List.foldl_append, hblock]
      exact ih

private theorem periodJump_of_closed (r : Reader) (h : Closed r) : periodJump r := by
  intro x q period n hq hp hcycle
  unfold matched
  change r.accept (applyT (power n (compile r x)) q) =
    r.accept (applyT (power (n % period) (compile r x)) q)
  rw [power_apply r h x n q hq, power_apply r h x (n % period) q hq]
  have hblocks :
      ((List.replicate (period * (n / period)) (expand x)).flatten).foldl r.step q = q := by
    rw [Nat.mul_comm period (n / period), flatten_replicate_mul]
    have hh := foldl_replicate_flatten_fixed r.step
      ((List.replicate period (expand x)).flatten) 1 q (n / period)
      (by simpa only [List.replicate_one, List.flatten_cons, List.flatten_nil,
        List.nil_append, List.append_nil] using (power_apply r h x period q hq).symm.trans hcycle)
    simpa only [List.replicate_one, List.flatten_cons, List.flatten_nil,
      List.nil_append, List.append_nil] using hh
  have hdec : n = period * (n / period) + n % period :=
    (Nat.div_add_mod n period).symm
  have hscan :
      ((List.replicate n (expand x)).flatten).foldl r.step q =
        ((List.replicate (n % period) (expand x)).flatten).foldl r.step q := by
    calc ((List.replicate n (expand x)).flatten).foldl r.step q
        = ((List.replicate (period * (n / period) + n % period) (expand x)).flatten).foldl r.step q :=
          congrArg (fun k => ((List.replicate k (expand x)).flatten).foldl r.step q) hdec
      _ = ((List.replicate (n % period) (expand x)).flatten).foldl r.step q := by
        rw [flatten_replicate_add, List.foldl_append, hblocks]
  rw [hscan]

/-! ## Packed transition tables -/

private theorem bits_le_byteCount (bits : Nat) : bits ≤ 8 * byteCount bits := by
  unfold byteCount
  have hm := Nat.mod_lt (bits + 7) (by decide : 0 < 8)
  have hd := Nat.mod_add_div (bits + 7) 8
  omega

private theorem fromLittle_append_singleton (l : Bytes) (b : Nat) :
    fromLittle (l ++ [b]) = fromLittle l + 256 ^ l.length * b := by
  induction l with
  | nil => simp [fromLittle]
  | cons x xs ih =>
      simp only [List.cons_append, fromLittle]
      rw [ih]
      simp only [List.length_cons, Nat.pow_succ]
      rw [Nat.mul_comm (256 ^ xs.length) 256, Nat.mul_assoc]
      omega

private theorem mod_mul_256 (v m : Nat) (hm : 0 < m) :
    v % (m * 256) = v % m + m * ((v / m) % 256) := by
  let r := v % m
  let q := v / m
  let s := q % 256
  let t := q / 256
  have hr : r < m := by simpa [r] using Nat.mod_lt v hm
  have hs : s < 256 := by simpa [s] using Nat.mod_lt q (by decide : 0 < 256)
  have hq : q = s + 256 * t := by
    dsimp [s, t]
    exact (Nat.mod_add_div q 256).symm
  have hv : v = r + m * q := by
    dsimp [r, q]
    exact (Nat.mod_add_div v m).symm
  have hs_le : s ≤ 255 := Nat.lt_succ_iff.mp (by simpa using hs)
  have hsum_lt : r + m * s < m * 256 := by
    have hm_le : m * s ≤ m * 255 := Nat.mul_le_mul_left m hs_le
    omega
  have hrewrite : v = (r + m * s) + (m * 256) * t := by
    rw [hv, hq]
    rw [Nat.mul_add, Nat.mul_assoc]
    omega
  have hleft : v % (m * 256) = r + m * s := by
    calc
      v % (m * 256) = ((r + m * s) + (m * 256) * t) % (m * 256) := by rw [hrewrite]
      _ = (r + m * s) % (m * 256) := by rw [Nat.add_mul_mod_self_left]
      _ = r + m * s := Nat.mod_eq_of_lt hsum_lt
  have hright : v % m + m * ((v / m) % 256) = r + m * s := rfl
  exact hleft.trans hright.symm

private theorem fromLittle_little (v k : Nat) :
    fromLittle ((List.range k).map (fun j => (v / 256 ^ j) % 256)) = v % 256 ^ k := by
  induction k with
  | zero => simp [fromLittle, Nat.mod_one]
  | succ k ih =>
      rw [List.range_succ, List.map_append]
      simp only [List.map_cons, List.map_nil]
      rw [fromLittle_append_singleton]
      simp only [List.length_map, List.length_range]
      rw [ih, Nat.pow_succ]
      exact (mod_mul_256 v (256 ^ k) (Nat.pow_pos (by decide : 0 < 256))).symm

private theorem word_lt_pow (w : Nat) (xs : List Nat) : word w xs < 2 ^ (xs.length * w) := by
  induction xs with
  | nil => simp [word]
  | cons x xs ih =>
      have hxmod : x % 2 ^ w < 2 ^ w := Nat.mod_lt x (Nat.two_pow_pos w)
      have hsum : x % 2 ^ w + 2 ^ w * word w xs < 2 ^ w * (word w xs + 1) := by
        rw [Nat.mul_succ, Nat.add_comm (2 ^ w * word w xs) (2 ^ w)]
        exact Nat.add_lt_add_right hxmod _
      have hle : 2 ^ w * (word w xs + 1) ≤ 2 ^ w * 2 ^ (xs.length * w) :=
        Nat.mul_le_mul_left (2 ^ w) (Nat.succ_le_of_lt ih)
      have hp_exp : (xs.length + 1) * w = w + xs.length * w := by
        rw [Nat.add_mul, Nat.one_mul]
        omega
      have hp : 2 ^ ((xs.length + 1) * w) = 2 ^ w * 2 ^ (xs.length * w) := by
        rw [hp_exp, Nat.pow_add]
      rw [word]
      change x % 2 ^ w + 2 ^ w * word w xs < 2 ^ ((xs.length + 1) * w)
      rw [hp]
      exact Nat.lt_of_lt_of_le hsum hle

private theorem fromLittle_pack (w : Nat) (xs : List Nat) :
    fromLittle (pack w xs) = word w xs := by
  unfold pack little
  rw [fromLittle_little]
  have hlt := word_lt_pow w xs
  have hb := bits_le_byteCount (xs.length * w)
  have hpow : 256 ^ byteCount (xs.length * w) = 2 ^ (8 * byteCount (xs.length * w)) := by
    have h256 : (256 : Nat) = 2 ^ 8 := by decide
    rw [h256, Nat.pow_mul]
  apply Nat.mod_eq_of_lt
  rw [hpow]
  exact Nat.lt_of_lt_of_le hlt (Nat.pow_le_pow_right (by decide : 0 < 2) hb)

private theorem word_shift (w : Nat) (xs : List Nat) (h : ∀ x ∈ xs, x < 2 ^ w) :
    word w xs / 2 ^ w = word w (List.drop 1 xs) := by
  cases xs with
  | nil => simp [word]
  | cons x xs =>
      have hx : x < 2 ^ w := h x (by simp)
      have hxmod : x % 2 ^ w = x := Nat.mod_eq_of_lt hx
      have hxdiv : x / 2 ^ w = 0 := Nat.div_eq_of_lt hx
      rw [word, hxmod]
      rw [Nat.add_comm, Nat.mul_add_div (Nat.two_pow_pos w) (word w xs) x, hxdiv, Nat.add_zero]
      simp

private theorem word_digit (w i : Nat) (xs : List Nat) (h : ∀ x ∈ xs, x < 2 ^ w)
    (hi : i < xs.length) : (word w xs / 2 ^ (i * w)) % 2 ^ w = xs[i] := by
  induction i generalizing xs with
  | zero =>
      simp only [Nat.zero_mul, Nat.pow_zero, Nat.div_one]
      cases xs with
      | nil => simp at hi
      | cons x xs =>
          have hx : x < 2 ^ w := h x (by simp)
          simp [word, Nat.mod_eq_of_lt hx]
  | succ i ih =>
      cases xs with
      | nil => simp at hi
      | cons x xs =>
          have ht : ∀ y ∈ xs, y < 2 ^ w := by
            intro y hy
            exact h y (by simp [hy])
          have hp_exp : (i + 1) * w = w + i * w := by
            rw [Nat.add_mul, Nat.one_mul]
            omega
          have hp : 2 ^ ((i + 1) * w) = 2 ^ w * 2 ^ (i * w) := by
            rw [hp_exp, Nat.pow_add]
          rw [hp, ← Nat.div_div_eq_div_mul, word_shift w (x :: xs) h]
          exact ih xs ht (Nat.lt_of_succ_lt_succ (by simpa using hi))

private theorem packedInverse (w : Nat) (xs : List Nat) (h : ∀ x ∈ xs, x < 2 ^ w) :
    (List.range xs.length).map (get (pack w xs) w) = xs := by
  have hfl : fromLittle (pack w xs) = word w xs := fromLittle_pack w xs
  apply List.ext_getElem?
  intro i
  rw [List.getElem?_map]
  by_cases hi : i < xs.length
  · rw [List.getElem?_range hi, List.getElem?_eq_getElem hi]
    simp only [Option.map_some, Option.some.injEq]
    change (fromLittle (pack w xs) / 2 ^ (i * w)) % 2 ^ w = xs[i]
    rw [hfl]
    exact word_digit w i xs h hi
  · have hle : xs.length ≤ i := by omega
    have hl : (List.range xs.length).length ≤ i := by simpa using hle
    rw [List.getElem?_eq_none hl, List.getElem?_eq_none hle]
    simp

private theorem lt_two_pow_width (n : Nat) : n < 2 ^ width n := by
  by_cases hn : n = 0
  · subst n
    simp [width]
  · rw [show width n = n.log2 + 1 by unfold width; simp [hn]]
    have hlog : n.log2 < n.log2 + 1 := Nat.lt_succ_self _
    exact (Nat.log2_lt hn).mp hlog

private theorem states_le_pow_width_sub (r : Reader) :
    r.states ≤ 2 ^ width (r.states - 1) := by
  have h := lt_two_pow_width (r.states - 1)
  omega

private theorem tableBytes_of_closed (r : Reader) (h : Closed r) : tableBytes r := by
  intro x
  simp only [tableBytes]
  let t := compile r x
  have htlen : t.length = r.states := compile_length r x
  have hbound : ∀ v ∈ t, v < 2 ^ width (r.states - 1) := by
    intro v hv
    rw [List.mem_iff_getElem] at hv
    obtain ⟨i, hi, rfl⟩ := hv
    have hi' : i < r.states := by simpa [htlen] using hi
    have hval : t[i] = applyT t i := by simp [applyT, List.getElem?_eq_getElem hi]
    rw [hval, compile_apply r h x i hi']
    have hbnd := foldl_closed r h (expand x) i hi'
    have hle := states_le_pow_width_sub r
    omega
  simpa [htlen] using packedInverse (width (r.states - 1)) t hbound

private theorem matched_eq_foldl (r : Reader) (h : Closed r) (x : Rope) (q : Nat)
    (hq : q < r.states) : matched r x q = r.accept ((expand x).foldl r.step q) := by
  simp [matched, compile_apply r h x q hq]

/-! ## AsciiOnly -/

private theorem AsciiOnly_closed : Closed AsciiOnly.reader := by
  intro q b hq
  change q < 2 at hq
  change (if b < 128 then q else 1) < 2
  split <;> omega

theorem sound_AsciiOnly_RepeatedState : AsciiOnly.RepeatedState.Goal :=
  repeatedState_of_closed AsciiOnly.reader AsciiOnly_closed
theorem sound_AsciiOnly_PrefixSeek : AsciiOnly.PrefixSeek.Goal :=
  prefixSeek_of_closed AsciiOnly.reader AsciiOnly_closed
theorem sound_AsciiOnly_Checkpoint : AsciiOnly.Checkpoint.Goal :=
  checkpoint_of_prefixSeek AsciiOnly.reader
    (prefixSeek_of_closed AsciiOnly.reader AsciiOnly_closed)
theorem sound_AsciiOnly_ReverseScan : AsciiOnly.ReverseScan.Goal := by
  intro x q hq
  simp only [reverseScan, matched]
  rw [compile_apply AsciiOnly.reader AsciiOnly_closed (reverse x) q hq, expand_reverse]
theorem sound_AsciiOnly_DictionaryMask : AsciiOnly.DictionaryMask.Goal :=
  dictionaryMask_of_closed AsciiOnly.reader AsciiOnly_closed
theorem sound_AsciiOnly_ContextReuse : AsciiOnly.ContextReuse.Goal :=
  contextReuse_of_closed AsciiOnly.reader AsciiOnly_closed
theorem sound_AsciiOnly_PeriodJump : AsciiOnly.PeriodJump.Goal :=
  periodJump_of_closed AsciiOnly.reader AsciiOnly_closed
theorem sound_AsciiOnly_TableBytes : AsciiOnly.TableBytes.Goal :=
  tableBytes_of_closed AsciiOnly.reader AsciiOnly_closed

private theorem AsciiOnly_step1 (b : Nat) : AsciiOnly.reader.step 1 b = 1 := by
  simp [AsciiOnly.reader]

private theorem AsciiOnly_foldl1 (xs : Bytes) : xs.foldl AsciiOnly.reader.step 1 = 1 := by
  induction xs with
  | nil => simp
  | cons b bs ih => simp [AsciiOnly_step1, ih]

private theorem AsciiOnly_foldl0 (xs : Bytes) :
    xs.foldl AsciiOnly.reader.step 0 = if xs.all (· < 128) then 0 else 1 := by
  induction xs with
  | nil => simp
  | cons b bs ih =>
      simp only [List.foldl_cons, List.all_cons]
      by_cases hb : b < 128
      · simp [AsciiOnly.reader, hb, ih]
      · simpa only [AsciiOnly.reader, hb, ↓reduceIte] using AsciiOnly_foldl1 bs

theorem sound_AsciiOnly_RejectEarly : AsciiOnly.RejectEarly.Goal := by
  intro a b hma
  rw [matched_eq_foldl AsciiOnly.reader AsciiOnly_closed (.cat a b) 0 (by decide)]
  rw [matched_eq_foldl AsciiOnly.reader AsciiOnly_closed a 0 (by decide)] at hma
  simp only [AsciiOnly.reader] at hma ⊢
  rw [AsciiOnly_foldl0] at hma
  cases ha : (expand a).all (· < 128) with
  | true => simp [ha] at hma
  | false => simp [expand, List.foldl_append, AsciiOnly_foldl0, ha, AsciiOnly_foldl1]

theorem sound_AsciiOnly_SevenBitFastPath : AsciiOnly.SevenBitFastPath.Goal := by
  intro x
  rw [matched_eq_foldl AsciiOnly.reader AsciiOnly_closed x 0 (by decide)]
  rw [AsciiOnly_foldl0]
  simp only [AsciiOnly.reader]
  cases h : (expand x).all (· < 128) <;> simp [h]

#guard matched AsciiOnly.reader (.lit [65, 127]) 0
#guard !matched AsciiOnly.reader (.lit [65, 128]) 0

end TinylambOpt.ResearchProofs.StorageExecution.GrammarScansB
