import TinylambOpt.ResearchGoals.StorageExecution.GrammarScans

/-!
A compressed string scan uses grammar or chunk summaries to evaluate string predicates
without always reconstructing the full value.

Proved

- `ContainsSlash.PrefixSeek` — Only a partial final phrase needs to be interpreted to
  obtain the state at an offset.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_GrammarScans_ContainsSlash_PrefixSeek

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.GrammarScans
open TinylambOpt.ResearchGoals.StorageExecution.GrammarScans.ContainsSlash

/-! ## Truth check on small inputs (only inputs satisfying the hypothesis) -/

private def samples : List (Rope × Nat × Nat) :=
  [ (.lit [], 0, 0), (.lit [47], 1, 0), (.lit [65, 47, 66], 3, 1),
    (.cat (.lit [65]) (.lit [47, 66]), 5, 1),
    (.rep 3 (.lit [65, 47]), 4, 1), (.rep 2 (.lit [65]), 100, 1),
    (.cat (.rep 2 (.lit [65, 66])) (.lit [47]), 4, 0) ]

private def check : Bool :=
  samples.all fun p =>
    p.2.2 < reader.states &&
      partialScan reader p.1 p.2.1 p.2.2 ==
        ((expand p.1).take p.2.1).foldl reader.step p.2.2

#eval check

example : check := by native_decide

/-! ## Table facts -/

private theorem chain_length (a b : Table) : (chain a b).length = a.length := by
  simp [chain]

private theorem applyT_chain (a b : Table) (q : Nat) (hq : q < a.length) :
    applyT (chain a b) q = applyT b (applyT a q) := by
  have hq' : q < (chain a b).length := by simpa [chain_length] using hq
  simp [applyT, chain, List.getElem?_map, List.getElem?_eq_getElem hq]

private theorem power_length (n : Nat) (t : Table) : (power n t).length = t.length := by
  induction n using Nat.strongRecOn with
  | _ n ih =>
      unfold power
      split
      · simp
      · rename_i hzero
        have ihh := ih (n / 2) (Nat.div_lt_self (by omega) (by decide))
        simp only
        split
        · rw [chain_length, ihh]
        · rw [chain_length, chain_length, ihh]

private theorem power_zero (t : Table) : power 0 t = List.range t.length := by
  unfold power
  simp

private theorem applyT_power_zero (t : Table) (q : Nat) (hq : q < t.length) :
    applyT (power 0 t) q = q := by
  simp [power_zero, applyT, List.getElem?_range hq]

private theorem compile_length (r : Reader) (x : Rope) : (compile r x).length = r.states := by
  induction x generalizing r with
  | lit s => simp [compile]
  | cat a b ihA ihB => simp [compile, chain_length, ihA]
  | rep n a ihA => simp [compile, power_length, ihA]

/-! ## Byte-list facts -/

private theorem cs_foldl_bounded (xs : Bytes) {q : Nat} (hq : q < reader.states) :
    xs.foldl reader.step q < reader.states := by
  induction xs generalizing q with
  | nil => simpa
  | cons b bs ih =>
      apply ih
      by_cases h : b == 47
      · have hb : b = 47 := beq_iff_eq.mp h
        simp [reader, hb]
      · have hb : b ≠ 47 := by intro hb'; exact h (beq_iff_eq.mpr hb')
        simpa [reader, hb] using hq

private theorem foldl_flatten_replicate_succ (r : Reader) (n : Nat) (xs : Bytes) (q : Nat) :
    ((List.replicate (n + 1) xs).flatten).foldl r.step q =
      ((List.replicate n xs).flatten).foldl r.step (xs.foldl r.step q) := by
  rw [List.replicate_succ, List.flatten_cons, List.foldl_append]

private theorem flatten_replicate_succ_append (k : Nat) (xs : Bytes) :
    (List.replicate (k + 1) xs).flatten = (List.replicate k xs).flatten ++ xs := by
  induction k with
  | zero => simp
  | succ k ih =>
      calc
        (List.replicate (k + 1 + 1) xs).flatten
            = xs ++ (List.replicate (k + 1) xs).flatten := by
                rw [show k + 1 + 1 = (k + 1) + 1 by omega, List.replicate_succ, List.flatten_cons]
        _ = xs ++ ((List.replicate k xs).flatten ++ xs) := by rw [ih]
        _ = (xs ++ (List.replicate k xs).flatten) ++ xs := by rw [List.append_assoc]
        _ = (List.replicate (k + 1) xs).flatten ++ xs := by
              rw [List.replicate_succ, List.flatten_cons]

private theorem foldl_F_m_one (r : Reader) (m : Nat) (xs : Bytes) (q : Nat) :
    ((List.replicate m xs).flatten).foldl r.step (xs.foldl r.step q) =
      xs.foldl r.step (((List.replicate m xs).flatten).foldl r.step q) := by
  calc
    ((List.replicate m xs).flatten).foldl r.step (xs.foldl r.step q)
        = (xs ++ (List.replicate m xs).flatten).foldl r.step q := by rw [List.foldl_append]
    _ = ((List.replicate (m + 1) xs).flatten).foldl r.step q := by
          rw [← List.flatten_cons, ← List.replicate_succ]
    _ = ((List.replicate m xs).flatten ++ xs).foldl r.step q := by rw [flatten_replicate_succ_append]
    _ = xs.foldl r.step (((List.replicate m xs).flatten).foldl r.step q) := by rw [List.foldl_append]

private theorem foldl_flatten_replicate_add (r : Reader) (m n : Nat) (xs : Bytes) (q : Nat) :
    ((List.replicate (m + n) xs).flatten).foldl r.step q =
      ((List.replicate n xs).flatten).foldl r.step
        (((List.replicate m xs).flatten).foldl r.step q) := by
  induction n generalizing q with
  | zero => simp
  | succ n ih =>
      rw [show m + (n + 1) = (m + n) + 1 by omega]
      rw [foldl_flatten_replicate_succ]
      rw [ih]
      rw [foldl_F_m_one]
      rw [foldl_flatten_replicate_succ]

private theorem replicate_append_self (m : Nat) (xs : Bytes) :
    List.replicate m xs ++ [xs] = xs :: List.replicate m xs := by
  induction m with
  | zero => simp
  | succ m ih => simp [List.replicate_succ, ih]

private theorem replicate_add_eq_append (m n : Nat) (xs : Bytes) :
    List.replicate (m + n) xs = List.replicate m xs ++ List.replicate n xs := by
  induction n with
  | zero => simp
  | succ n ih =>
      calc
        List.replicate (m + (n + 1)) xs
            = List.replicate ((m + n) + 1) xs := by rw [show m + (n + 1) = (m + n) + 1 by omega]
        _ = xs :: List.replicate (m + n) xs := by rw [List.replicate_succ]
        _ = xs :: (List.replicate m xs ++ List.replicate n xs) := by rw [ih]
        _ = (xs :: List.replicate m xs) ++ List.replicate n xs := rfl
        _ = (List.replicate m xs ++ [xs]) ++ List.replicate n xs := by rw [replicate_append_self]
        _ = List.replicate m xs ++ (xs :: List.replicate n xs) := by simp [List.append_assoc]
        _ = List.replicate m xs ++ List.replicate (n + 1) xs := by rw [List.replicate_succ]

/-! ## Compile correctness for `reader` -/

private theorem applyT_power_of_compile (xs : Bytes) (t : Table) (hlen : t.length = reader.states)
    (ht : ∀ {q : Nat}, q < reader.states → applyT t q = xs.foldl reader.step q) :
    ∀ m q : Nat, q < reader.states →
      applyT (power m t) q = ((List.replicate m xs).flatten).foldl reader.step q := by
  intro m
  induction m using Nat.strongRecOn with
  | _ m ihM =>
      intro q hq
      by_cases hm : m = 0
      · subst m
        rw [applyT_power_zero _ q (by simpa [hlen] using hq)]
        simp
      · have hdiv : m / 2 < m := Nat.div_lt_self (Nat.pos_of_ne_zero hm) (by decide)
        have ihH := ihM (m / 2) hdiv
        have hlenP : (power (m / 2) t).length = reader.states := by rw [power_length, hlen]
        unfold power
        simp [hm]
        by_cases heven : m % 2 == 0
        · have heven' : m % 2 = 0 := beq_iff_eq.mp heven
          simp [heven']
          rw [applyT_chain _ _ q (by simpa [hlenP] using hq)]
          rw [ihH q hq]
          have hqH : ((List.replicate (m / 2) xs).flatten).foldl reader.step q < reader.states := cs_foldl_bounded _ hq
          rw [ihH _ hqH]
          have hm2 : m / 2 + m / 2 = m := by omega
          simpa [hm2] using (foldl_flatten_replicate_add reader (m / 2) (m / 2) xs q).symm
        · have hodd : m % 2 ≠ 0 := by
            intro h0
            exact heven (beq_iff_eq.mpr h0)
          simp [hodd]
          have hlenSq : (chain (power (m / 2) t) (power (m / 2) t)).length = reader.states := by
            rw [chain_length, hlenP]
          rw [applyT_chain _ _ q (by simpa [hlenSq] using hq)]
          rw [applyT_chain _ _ q (by simpa [hlenP] using hq)]
          rw [ihH q hq]
          have hqH : ((List.replicate (m / 2) xs).flatten).foldl reader.step q < reader.states := cs_foldl_bounded _ hq
          rw [ihH _ hqH]
          have hsqBound : ((List.replicate (m / 2 + m / 2) xs).flatten).foldl reader.step q < reader.states := cs_foldl_bounded _ hq
          rw [← foldl_flatten_replicate_add reader (m / 2) (m / 2) xs q]
          rw [ht hsqBound]
          have hm2 : m / 2 + m / 2 + 1 = m := by omega
          simpa [hm2] using (foldl_flatten_replicate_add reader (m / 2 + m / 2) 1 xs q).symm

private theorem applyT_compile_cs (x : Rope) {q : Nat} (hq : q < reader.states) :
    applyT (compile reader x) q = (expand x).foldl reader.step q := by
  induction x generalizing q with
  | lit s =>
      simp [compile, expand, applyT, List.getElem?_map, List.getElem?_range hq]
  | cat a b ihA ihB =>
      have hqa_len : q < (compile reader a).length := by simpa [compile_length] using hq
      rw [show compile reader (.cat a b) = chain (compile reader a) (compile reader b) by rfl]
      rw [applyT_chain _ _ q hqa_len]
      rw [ihA hq]
      have hqa : (expand a).foldl reader.step q < reader.states := cs_foldl_bounded _ hq
      rw [ihB hqa]
      simp [expand, List.foldl_append]
  | rep n a ihA =>
      have hp := applyT_power_of_compile (expand a) (compile reader a) (compile_length reader a)
        (fun {q} hq => ihA hq) n q hq
      simpa [compile, expand] using hp

/-! ## PrefixSeek -/

private theorem expand_length (x : Rope) : (expand x).length = extent x := by
  induction x with
  | lit s => rfl
  | cat a b ihA ihB => simp [expand, extent, ihA, ihB]
  | rep k a ihA =>
      simp [expand, extent, ihA]

theorem sound_PrefixSeek : ContainsSlash.PrefixSeek.Goal := by
  intro x q n hq
  induction x generalizing q n with
  | lit s =>
      simp [partialScan, expand]
  | cat a b ihA ihB =>
      simp only [partialScan]
      by_cases hn : n ≤ extent a
      · rw [ite_eq_left hn]
        have ht : (expand a ++ expand b).take n = (expand a).take n := by
          apply List.take_append_of_le_length
          simpa [expand_length] using hn
        simp [expand, ht, ihA q n hq]
      · rw [ite_eq_right hn]
        have hgt : extent a < n := Nat.lt_of_not_ge hn
        have hqa : applyT (compile reader a) q < reader.states := by
          rw [applyT_compile_cs a hq]
          exact cs_foldl_bounded _ hq
        have ht : (expand a ++ expand b).take n =
            expand a ++ (expand b).take (n - (expand a).length) := by
          rw [List.take_append, List.take_of_length_le (by
            simpa [expand_length] using Nat.le_of_lt hgt)]
        rw [ihB _ _ hqa]
        simp [expand, ht, List.foldl_append, applyT_compile_cs a hq, expand_length]
  | rep k a ihA =>
      simp only [partialScan]
      by_cases hsize : extent a = 0
      · have hexp : expand a = [] := by
          apply List.eq_nil_of_length_eq_zero
          simpa [expand_length] using hsize
        have hzero : (List.replicate k (expand a)).flatten = [] := by simp [hexp]
        simp [hsize, expand, hexp]
      · simp [hsize]
        have hsizePos : 0 < extent a := Nat.pos_of_ne_zero hsize
        have hfull_le_n : (min k (n / extent a)) * extent a ≤ n := by
          have h1 : min k (n / extent a) ≤ n / extent a := Nat.min_le_right _ _
          have h2 : (min k (n / extent a)) * extent a ≤ (n / extent a) * extent a := Nat.mul_le_mul_right _ h1
          have h3 : (n / extent a) * extent a ≤ n := Nat.div_mul_le_self n (extent a)
          omega
        have hnext : applyT (power (min k (n / extent a)) (compile reader a)) q =
            ((List.replicate (min k (n / extent a)) (expand a)).flatten).foldl reader.step q :=
          applyT_power_of_compile (expand a) (compile reader a) (compile_length reader a)
            (fun {q} hq => applyT_compile_cs a hq) (min k (n / extent a)) q hq
        have hnextBound : ((List.replicate (min k (n / extent a)) (expand a)).flatten).foldl reader.step q < reader.states :=
          cs_foldl_bounded _ hq
        by_cases hfk : min k (n / extent a) = k
        · have hkn : k * extent a ≤ n := by simpa [hfk] using hfull_le_n
          have hlen : ((List.replicate k (expand a)).flatten).length = k * extent a := by
            have := expand_length (.rep k a)
            simpa [expand, extent] using this
          rw [show expand (.rep k a) = (List.replicate k (expand a)).flatten by rfl]
          rw [List.take_of_length_le (by rw [hlen]; exact hkn)]
          simpa [hfk] using hnext
        · simp [hfk, hnext]
          have htake : ((List.replicate k (expand a)).flatten).take n =
              (List.replicate (min k (n / extent a)) (expand a)).flatten ++
                ((expand a).take (n % extent a)) := by
            have hrep : List.replicate k (expand a) =
                List.replicate (min k (n / extent a)) (expand a) ++
                  List.replicate (k - min k (n / extent a)) (expand a) := by
              rw [← replicate_add_eq_append]
              congr 1
              have hmin_le : min k (n / extent a) ≤ k := Nat.min_le_left _ _
              omega
            rw [hrep, List.flatten_append, List.take_append]
            have hlenFull : ((List.replicate (min k (n / extent a)) (expand a)).flatten).length =
                (min k (n / extent a)) * extent a := by
              have := expand_length (.rep (min k (n / extent a)) a)
              simpa [expand, extent] using this
            have hnotle : ¬ k ≤ n / extent a := by
              intro hk_le
              exact hfk (Nat.min_eq_left hk_le)
            have hm_le_k : n / extent a ≤ k := Nat.le_of_lt (Nat.lt_of_not_ge hnotle)
            have hfull_eq : min k (n / extent a) = n / extent a := Nat.min_eq_right hm_le_k
            have hn' : n - ((List.replicate (min k (n / extent a)) (expand a)).flatten).length =
                n % extent a := by
              rw [hlenFull, hfull_eq]
              have hdivmod : (n / extent a) * extent a + n % extent a = n := by
                rw [Nat.mul_comm]
                exact Nat.div_add_mod n (extent a)
              omega
            have hleN : ((List.replicate (min k (n / extent a)) (expand a)).flatten).length ≤ n := by
              rw [hlenFull]
              exact hfull_le_n
            have hkfull : 0 < k - min k (n / extent a) := by omega
            rw [List.take_of_length_le hleN, hn']
            cases hkm : k - min k (n / extent a) with
            | zero => omega
            | succ m =>
                rw [List.replicate_succ, List.flatten_cons]
                rw [List.take_append_of_le_length (by
                  rw [expand_length]
                  exact Nat.le_of_lt (Nat.mod_lt n (Nat.pos_of_ne_zero hsize)))]
          rw [show expand (.rep k a) = (List.replicate k (expand a)).flatten by rfl]
          rw [htake, List.foldl_append]
          rw [ihA _ (n % extent a) hnextBound]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_GrammarScans_ContainsSlash_PrefixSeek
