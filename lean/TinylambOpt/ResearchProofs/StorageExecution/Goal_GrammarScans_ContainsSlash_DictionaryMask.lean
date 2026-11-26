import TinylambOpt.ResearchGoals.StorageExecution.GrammarScans

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_GrammarScans_ContainsSlash_DictionaryMask

open TinylambOpt.ResearchGoals.StorageExecution.GrammarScans
open TinylambOpt.ResearchGoals.StorageExecution.GrammarScans.ContainsSlash

/-! Truth check on small dictionaries.  Every scanned state satisfies the
hypothesis `q < reader.states`; `sampleIds` also contains out-of-range codes to
exercise the `getD false` path. -/

private def samples : List Rope :=
  [ .lit [], .lit [47], .lit [65], .lit [47, 65], .lit [65, 47],
    .cat (.lit [65]) (.lit [47]), .rep 0 (.lit [47]), .rep 2 (.lit [47]),
    .rep 3 (.lit [65, 66]), .cat (.rep 2 (.lit [65])) (.lit [47]) ]

private def sampleIds : List Nat := [0, 1, 2, 9]

private def check : Bool :=
  (List.range reader.states).all fun q =>
    rows reader samples sampleIds q =
      sampleIds.map (fun i =>
        (samples[i]?.map (fun x => reader.accept ((expand x).foldl reader.step q))).getD false)

#eval check

example : check := by native_decide

/-! The slash reader has an absorbing accept state: from either of its two
states, folding a byte string ends in 1 exactly when a slash occurs. -/

private theorem step_one (b : Nat) : reader.step 1 b = 1 := by
  simp [reader]

private theorem step_slash (q b : Nat) (hb : b = 47) : reader.step q b = 1 := by
  simp [reader, hb]

private theorem step_nonslash (q b : Nat) (hb : b ≠ 47) : reader.step q b = q := by
  simp [reader, hb]

private theorem foldl_reader_one (xs : List Nat) : xs.foldl reader.step 1 = 1 := by
  induction xs with
  | nil => simp
  | cons b bs ih =>
      rw [List.foldl_cons, step_one, ih]

private theorem foldl_reader (xs : List Nat) (q : Nat) (hq : q < 2) :
    xs.foldl reader.step q = if xs.contains 47 then 1 else q := by
  induction xs generalizing q with
  | nil => simp
  | cons b bs ih =>
      rw [List.foldl_cons]
      by_cases hb : b == 47
      · have hb' : b = 47 := by simpa using hb
        rw [step_slash q b hb', foldl_reader_one]
        simp [hb']
      · have hb' : b ≠ 47 := by simpa using hb
        rw [step_nonslash q b hb', ih q hq]
        simp [hb', eq_comm]

/-! For this reader the compiled table has a particularly simple shape:
index 0 is the "contains slash" bit and index 1 is the absorbing state. -/

private theorem chain_same (v : Nat) (hv : v = 0 ∨ v = 1) :
    chain [v, 1] [v, 1] = [v, 1] := by
  rcases hv with rfl | rfl <;> simp [chain, applyT]

private theorem range_two : List.range 2 = [0, 1] := by decide

private theorem power_slash (v n : Nat) (hv : v = 0 ∨ v = 1) :
    power n [v, 1] = if n = 0 then [0, 1] else [v, 1] := by
  induction n using Nat.strongRecOn with
  | _ n ih =>
      by_cases hn : n = 0
      · subst n
        unfold power
        simp [range_two]
      · have hpos : 0 < n := Nat.pos_of_ne_zero hn
        have hdiv : n / 2 < n := Nat.div_lt_self hpos (by decide)
        by_cases hhalf : n / 2 = 0
        · have hn1 : n = 1 := by omega
          subst n
          simp [power, chain, applyT, range_two]
        · have ihalf : power (n / 2) [v, 1] = [v, 1] := by
            have h := ih (n / 2) hdiv
            simpa [hhalf] using h
          unfold power
          simp [hn, ihalf, chain_same v hv]

private theorem compile_shape (x : Rope) :
    compile reader x = [if (expand x).contains 47 then 1 else 0, 1] := by
  induction x with
  | lit s =>
      change (List.range reader.states).map (fun q => s.foldl reader.step q) =
        [if s.contains 47 then 1 else 0, 1]
      rw [show reader.states = 2 by decide, range_two]
      simp only [List.map_cons, List.map_nil]
      rw [foldl_reader s 0 (by decide), foldl_reader_one]
  | cat a b iha ihb =>
      simp only [compile, expand]
      rw [iha, ihb]
      by_cases ha : 47 ∈ expand a <;> by_cases hb : 47 ∈ expand b <;>
        simp [ha, hb, List.contains_iff_mem, chain, applyT]
  | rep n a iha =>
      change power n (compile reader a) =
        [if (List.replicate n (expand a)).flatten.contains 47 then 1 else 0, 1]
      rw [iha]
      by_cases ha : 47 ∈ expand a
      · have hca : (expand a).contains 47 = true := by simpa [List.contains_iff_mem] using ha
        simp [hca, ha]
        by_cases hn : n = 0
        · subst n
          rw [power_slash 1 0 (Or.inr rfl)]
          simp
        · have hrep_mem : 47 ∈ (List.replicate n (expand a)).flatten := by
            rw [List.mem_flatten]
            refine ⟨expand a, ?_, ha⟩
            simp [hn]
          have hrep_c : (List.replicate n (expand a)).flatten.contains 47 = true := by
            simpa [List.contains_iff_mem] using hrep_mem
          rw [hrep_c]
          simp [hca, ha]
          rw [power_slash 1 n (Or.inr rfl)]
          simp [hn]
      · have hca : (expand a).contains 47 = false := by simpa [List.contains_iff_mem] using ha
        simp [hca, ha]
        have hrep_not : 47 ∉ (List.replicate n (expand a)).flatten := by
          intro hm
          rw [List.mem_flatten] at hm
          rcases hm with ⟨l, hl, h47⟩
          rw [List.mem_replicate] at hl
          exact ha (hl.2 ▸ h47)
        have hrep_c : (List.replicate n (expand a)).flatten.contains 47 = false := by
          simpa [List.contains_iff_mem] using hrep_not
        rw [hrep_c]
        simp [hca, ha]
        rw [power_slash 0 n (Or.inl rfl)]
        simp

private theorem compile_apply (x : Rope) (q : Nat) (hq : q < 2) :
    applyT (compile reader x) q = (expand x).foldl reader.step q := by
  rw [compile_shape x, foldl_reader (expand x) q hq]
  have hq01 : q = 0 ∨ q = 1 := by omega
  rcases hq01 with rfl | rfl <;> simp [applyT]

theorem sound_DictionaryMask : ContainsSlash.DictionaryMask.Goal := by
  intro d ids q hq
  unfold rows matched
  apply List.map_congr_left
  intro i hi
  simp only [List.getElem?_map]
  cases hd : d[i]? with
  | none => rfl
  | some x => simp [hd, compile_apply x q hq]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_GrammarScans_ContainsSlash_DictionaryMask
