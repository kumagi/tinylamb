import TinylambOpt.ResearchGoals.StorageExecution.GrammarScans

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_GrammarScans_SlashDepth_RepeatedDelimiter

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.GrammarScans
open TinylambOpt.ResearchGoals.StorageExecution.GrammarScans.SlashDepth

/-! Small-input truth check (`2 ≤ n` inputs only). -/
private def samples : List Rope :=
  [ .lit [], .lit [47], .lit [65, 66], .lit [47, 47], .lit [47, 65, 47],
    .cat (.lit [65]) (.lit [47]), .cat (.lit [47]) (.lit [65]),
    .rep 0 (.lit [47]), .rep 1 (.lit [47]), .rep 2 (.lit [65]),
    .rep 2 (.lit [47]), .rep 3 (.lit [65, 47]), .rep 2 (.lit []),
    .cat (.rep 2 (.lit [65])) (.lit [47]) ]

private def check : Bool :=
  samples.all fun x => (List.range 6).all fun n =>
    (n < 2) || (matched reader (.rep n x) 0 == (expand x).contains 47)

#eval check

example : check := by native_decide

/-! Every compiled table is one of three maps `q ↦ min 2 (q + c)`, and
    `chain`/`power` correspond to addition/multiplication of the parameter. -/
private def tab (c : Nat) : Table := (List.range 3).map (fun q => min 2 (q + c))

private theorem tab_length (c : Nat) : (tab c).length = 3 := by simp [tab]

private theorem tab_get? (c i : Nat) :
    (tab c)[i]? = if i < 3 then some (min 2 (i + c)) else none := by
  by_cases hi : i < 3
  · rw [ite_eq_left hi, List.getElem?_eq_getElem (by simpa [tab_length] using hi)]
    simp [tab]
  · rw [ite_eq_right hi, List.getElem?_eq_none]
    simpa [tab_length] using hi

private theorem applyT_tab (c q : Nat) (hq : q < 3) : applyT (tab c) q = min 2 (q + c) := by
  rw [applyT, List.getElem?_eq_getElem (by simpa [tab_length] using hq)]
  simp [tab]

private theorem min2_add (a b : Nat) : min 2 (min 2 a + b) = min 2 (a + b) := by
  by_cases h : 2 ≤ a <;> omega

private theorem min2_add3 (i a b : Nat) : min 2 (i + a + b) = min 2 (i + min 2 (a + b)) := by
  by_cases h : 2 ≤ a + b <;> omega

private theorem min2_add_both (a b : Nat) : min 2 (min 2 a + min 2 b) = min 2 (a + b) := by
  by_cases ha : 2 ≤ a <;> by_cases hb : 2 ≤ b <;> omega

private theorem min2_min2 (a : Nat) : min 2 (min 2 a) = min 2 a := by
  by_cases h : 2 ≤ a <;> omega

private theorem min2_mul_min2 (k c : Nat) : min 2 (k * min 2 c) = min 2 (k * c) := by
  by_cases hc : 2 ≤ c
  · rw [Nat.min_eq_left hc]
    by_cases hk : k = 0
    · subst k
      simp
    · have h1 : 2 ≤ k * 2 := by omega
      have h2 : 2 ≤ k * c := Nat.mul_le_mul (by omega : 1 ≤ k) hc
      rw [Nat.min_eq_left h1, Nat.min_eq_left h2]
  · rw [Nat.min_eq_right (by omega : c ≤ 2)]

private theorem chain_tab (a b : Nat) : chain (tab a) (tab b) = tab (min 2 (a + b)) := by
  apply List.ext_getElem?
  intro i
  by_cases hi : i < 3
  · have hia : (tab a)[i]? = some (min 2 (i + a)) := by simpa [hi] using tab_get? a i
    rw [show (chain (tab a) (tab b))[i]? = some (applyT (tab b) (min 2 (i + a))) by
      simp [chain, List.getElem?_map, hia]]
    rw [tab_get?, ite_eq_left hi]
    rw [applyT_tab b (min 2 (i + a)) (by omega)]
    rw [min2_add, min2_add3 i a b]
  · rw [tab_get?, ite_eq_right hi]
    have hchainlen : (chain (tab a) (tab b)).length = 3 := by simp [chain, tab_length]
    rw [List.getElem?_eq_none (by simpa [hchainlen] using hi)]

private theorem power_tab : ∀ n c : Nat, power n (tab c) = tab (min 2 (n * c)) := by
  intro n
  induction n using Nat.strongRecOn with
  | ind n ih =>
      intro c
      by_cases hn : n = 0
      · subst n
        rw [show power 0 (tab c) = List.range (tab c).length by simp [power]]
        rw [tab_length]
        apply List.ext_getElem?
        intro i
        have hrange : (List.range 3)[i]? = if i < 3 then some i else none := by
          by_cases hi : i < 3
          · rw [ite_eq_left hi, List.getElem?_eq_getElem (by simp [hi])]
            simp
          · rw [ite_eq_right hi, List.getElem?_eq_none]
            simp
            omega
        rw [hrange, tab_get?]
        by_cases hi : i < 3
        · simp [hi]
          congr 1
          omega
        · simp [hi]
      · have hdiv : n / 2 < n := Nat.div_lt_self (Nat.pos_of_ne_zero hn) (by decide)
        have htabc := ih (n / 2) hdiv c
        unfold power
        simp only [hn, dite_false]
        let m := (n / 2) * c
        rw [htabc]
        change (if (n % 2 == 0) = true then chain (tab (min 2 m)) (tab (min 2 m))
                else chain (chain (tab (min 2 m)) (tab (min 2 m))) (tab c)) =
          tab (min 2 (n * c))
        by_cases heven : n % 2 == 0
        · simp [heven]
          rw [chain_tab]
          have hmod : n % 2 = 0 := by simpa using heven
          have hnform : n = 2 * (n / 2) := by
            have hmd := Nat.mod_add_div n 2
            omega
          have hmul : n * c = 2 * m := by
            rw [hnform, Nat.mul_assoc]
          have hmin : min 2 (min 2 m + min 2 m) = min 2 (2 * m) := by
            by_cases hm2 : 2 ≤ m <;> omega
          rw [hmul, hmin]
        · simp [heven]
          rw [chain_tab, chain_tab]
          have hmod : n % 2 = 1 := by
            have hmodne : n % 2 ≠ 0 := by
              intro h
              apply heven
              simp [h]
            have hlt : n % 2 < 2 := Nat.mod_lt n (by decide)
            omega
          have hnform : n = 2 * (n / 2) + 1 := by
            have hmd := Nat.mod_add_div n 2
            omega
          have hmul : n * c = 2 * m + c := by
            rw [hnform, Nat.add_mul, Nat.one_mul, Nat.mul_assoc]
          have hmin : min 2 (min 2 m + min 2 m + c) = min 2 (2 * m + c) := by
            by_cases hm2 : 2 ≤ m <;> omega
          rw [min2_add, hmul, hmin]

/-! Folding the reader counts slashes, saturating at two. -/
private theorem step_fold (s : Bytes) (q : Nat) (hq : q ≤ 2) :
    s.foldl reader.step q = min 2 (q + (s.count 47)) := by
  induction s generalizing q with
  | nil =>
      simp
      exact (Nat.min_eq_right hq).symm
  | cons b bs ih =>
      rw [List.foldl_cons]
      by_cases hb : b = 47
      · subst b
        have hstep : reader.step q 47 = min 2 (q + 1) := by simp [reader]
        rw [hstep, ih (min 2 (q + 1)) (by omega)]
        simp
        omega
      · have hstep : reader.step q b = q := by simp [reader, hb]
        rw [hstep, ih q hq]
        simp [hb]

private theorem count_flatten_replicate (k : Nat) (s : Bytes) :
    ((List.replicate k s).flatten.count 47) = k * (s.count 47) := by
  induction k with
  | zero => simp
  | succ k ih => simp [List.replicate_succ, ih, Nat.succ_mul, Nat.add_comm]

private theorem compile_table (x : Rope) :
    compile reader x = tab (min 2 ((expand x).count 47)) := by
  induction x with
  | lit s =>
      change (List.range 3).map (fun q => s.foldl reader.step q) =
        tab (min 2 ((s.count 47)))
      simp [tab]
      intro a ha
      rw [step_fold s a (by omega)]
      simpa using (min2_add3 a (s.count 47) 0)
  | cat a b iha ihb =>
      change chain (compile reader a) (compile reader b) =
        tab (min 2 (((expand a) ++ (expand b)).count 47))
      rw [iha, ihb, List.count_append, chain_tab, min2_add_both]
  | rep k a ih =>
      rw [compile, ih]
      change power k (tab (min 2 ((expand a).count 47))) =
        tab (min 2 (((List.replicate k (expand a)).flatten.count 47)))
      rw [count_flatten_replicate, power_tab, min2_mul_min2]

theorem sound_RepeatedDelimiter : SlashDepth.RepeatedDelimiter.Goal := by
  intro x n hn
  rw [matched, compile_table]
  change reader.accept (applyT (tab (min 2 ((expand (.rep n x)).count 47))) 0) =
    (expand x).contains 47
  rw [show expand (.rep n x) = (List.replicate n (expand x)).flatten by rfl]
  rw [count_flatten_replicate]
  rw [applyT_tab (min 2 (n * ((expand x).count 47))) 0 (by decide)]
  simp only [Nat.zero_add]
  rw [min2_min2]
  by_cases hz : (expand x).count 47 = 0
  · have hmulzero : n * ((expand x).count 47) = 0 := by simp [hz]
    rw [hmulzero]
    have hnotmem : 47 ∉ expand x := (List.count_eq_zero).mp hz
    simpa [reader] using hnotmem
  · have hpos : 0 < (expand x).count 47 := Nat.pos_of_ne_zero hz
    have htwo : 2 ≤ n * ((expand x).count 47) := by
      calc
        2 = 2 * 1 := by omega
        _ ≤ n * ((expand x).count 47) := Nat.mul_le_mul hn hpos
    rw [Nat.min_eq_left htwo]
    have hmem : 47 ∈ expand x := (List.count_pos_iff).mp hpos
    simpa [reader] using hmem

end TinylambOpt.ResearchProofs.StorageExecution.Goal_GrammarScans_SlashDepth_RepeatedDelimiter
