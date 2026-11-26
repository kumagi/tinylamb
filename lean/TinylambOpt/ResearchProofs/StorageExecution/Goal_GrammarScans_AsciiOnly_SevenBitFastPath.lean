import TinylambOpt.ResearchGoals.StorageExecution.GrammarScans

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_GrammarScans_AsciiOnly_SevenBitFastPath

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.GrammarScans
open TinylambOpt.ResearchGoals.StorageExecution.GrammarScans.AsciiOnly

/-! ## Small-input truth checks -/

private def samples : List Rope :=
  [ .lit [], .lit [65], .lit [200], .lit [65, 200], .lit [200, 65],
    .cat (.lit [65]) (.lit [200]), .cat (.lit [200]) (.lit [65]),
    .rep 0 (.lit [200]), .rep 2 (.lit [65]), .rep 3 (.lit []),
    .rep 2 (.lit [65, 200]), .cat (.rep 2 (.lit [65])) (.lit [200]),
    .cat (.rep 3 (.lit [127])) (.rep 2 (.lit [128, 65])) ]

private def check : Bool :=
  samples.all fun x => matched reader x 0 == (expand x).all (fun b => b < 128)

#eval check

example : check := by native_decide

/-! The reader is sticky: a byte ≥ 128 sends it to rejecting state 1,
    which is absorbing; ASCII bytes leave the state unchanged. -/

private theorem step_fold (s : Bytes) (q : Nat) :
    s.foldl reader.step q = if (∃ b ∈ s, 128 ≤ b) then 1 else q := by
  induction s generalizing q with
  | nil => simp [reader]
  | cons b bs ih =>
      rw [List.foldl_cons, ih]
      by_cases hb : 128 ≤ b
      · have hmem : ∃ c ∈ b :: bs, 128 ≤ c := ⟨b, by simp, hb⟩
        rw [if_pos hmem]
        have hnotlt : ¬ b < 128 := by omega
        simp [reader, hnotlt]
      · have hblt : b < 128 := by omega
        by_cases hm : ∃ c ∈ bs, 128 ≤ c
        · have hmem : ∃ c ∈ b :: bs, 128 ≤ c := by
            rcases hm with ⟨c, hc, hc128⟩
            exact ⟨c, by simp [hc], hc128⟩
          rw [if_pos hmem, if_pos hm]
        · have hmem : ¬ ∃ c ∈ b :: bs, 128 ≤ c := by
            rintro ⟨c, hc, hc128⟩
            simp at hc
            rcases hc with rfl | hc
            · exact hb hc128
            · exact hm ⟨c, hc, hc128⟩
          rw [if_neg hmem, if_neg hm]
          simp [reader, hblt]

private theorem bad_append (xs ys : Bytes) :
    (∃ b ∈ xs ++ ys, 128 ≤ b) ↔
      (∃ b ∈ xs, 128 ≤ b) ∨ (∃ b ∈ ys, 128 ≤ b) := by
  constructor
  · rintro ⟨b, hbmem, hb128⟩
    rcases List.mem_append.mp hbmem with h | h
    · exact Or.inl ⟨b, h, hb128⟩
    · exact Or.inr ⟨b, h, hb128⟩
  · rintro (⟨b, hbmem, hb128⟩ | ⟨b, hbmem, hb128⟩)
    · exact ⟨b, List.mem_append.mpr (Or.inl hbmem), hb128⟩
    · exact ⟨b, List.mem_append.mpr (Or.inr hbmem), hb128⟩

private theorem exists_or_bad (xs ys : Bytes) :
    (∃ b, (b ∈ xs ∨ b ∈ ys) ∧ 128 ≤ b) ↔
      (∃ b ∈ xs, 128 ≤ b) ∨ (∃ b ∈ ys, 128 ≤ b) := by
  constructor
  · rintro ⟨b, h | h, hb⟩
    · exact Or.inl ⟨b, h, hb⟩
    · exact Or.inr ⟨b, h, hb⟩
  · rintro (⟨b, h, hb⟩ | ⟨b, h, hb⟩)
    · exact ⟨b, Or.inl h, hb⟩
    · exact ⟨b, Or.inr h, hb⟩

private theorem bad_flatten_replicate (k : Nat) (s : Bytes) :
    (∃ b ∈ (List.replicate k s).flatten, 128 ≤ b) ↔
      0 < k ∧ (∃ b ∈ s, 128 ≤ b) := by
  simp only [List.mem_flatten, List.mem_replicate]
  constructor
  · rintro ⟨b, ⟨l, ⟨hkne, hl⟩, hbl⟩, hb128⟩
    subst l
    exact ⟨Nat.pos_of_ne_zero hkne, ⟨b, hbl, hb128⟩⟩
  · rintro ⟨hkpos, ⟨b, hbmem, hb128⟩⟩
    exact ⟨b, ⟨s, ⟨Nat.ne_of_gt hkpos, rfl⟩, hbmem⟩, hb128⟩

/-! The only two transition tables this reader can compile to. -/

private theorem power_id : ∀ n : Nat, power n [0, 1] = [0, 1] := by
  intro n
  induction n using Nat.strongRecOn with
  | ind n ih =>
      unfold power
      split
      · decide
      · rename_i hn
        have hlt : n / 2 < n := Nat.div_lt_self (by omega) (by decide)
        have hh := ih (n / 2) hlt
        simp [hh, chain, applyT]

private theorem power_const : ∀ n : Nat, power n [1, 1] = if n = 0 then [0, 1] else [1, 1] := by
  intro n
  induction n using Nat.strongRecOn with
  | ind n ih =>
      unfold power
      split
      · rename_i hn
        subst n
        decide
      · rename_i hn
        have hlt : n / 2 < n := Nat.div_lt_self (by omega) (by decide)
        have hh := ih (n / 2) hlt
        by_cases hhalf : n / 2 = 0
        · have hn1 : n = 1 := by omega
          subst n
          simp [hh, chain, applyT]
        · have hh' : power (n / 2) [1, 1] = [1, 1] := by simpa [hhalf] using hh
          simp [hh', chain, applyT]

private theorem compile_table (x : Rope) :
    compile reader x = if (∃ b ∈ expand x, 128 ≤ b) then [1, 1] else [0, 1] := by
  induction x with
  | lit s =>
      change (List.range 2).map (fun q => s.foldl reader.step q) =
        if (∃ b ∈ s, 128 ≤ b) then [1, 1] else [0, 1]
      rw [show List.range 2 = [0, 1] by decide]
      by_cases hs : ∃ b ∈ s, 128 ≤ b <;> simp [step_fold, hs]
  | cat a b iha ihb =>
      rw [compile, iha, ihb, expand]
      by_cases ha : ∃ c ∈ expand a, 128 ≤ c <;>
        by_cases hb : ∃ c ∈ expand b, 128 ≤ c <;>
          simp [ha, hb, chain, applyT, exists_or_bad]
  | rep k a ih =>
      rw [compile, ih]
      change power k (if (∃ b ∈ expand a, 128 ≤ b) then [1, 1] else [0, 1]) =
        if (∃ b ∈ (List.replicate k (expand a)).flatten, 128 ≤ b) then [1, 1] else [0, 1]
      by_cases hs : ∃ b ∈ expand a, 128 ≤ b
      · rw [if_pos hs]
        by_cases hk : k = 0
        · subst k
          simp [power]
          decide
        · have hmem : ∃ b ∈ (List.replicate k (expand a)).flatten, 128 ≤ b :=
            (bad_flatten_replicate k (expand a)).2 ⟨Nat.pos_of_ne_zero hk, hs⟩
          rw [if_pos hmem]
          simp [power_const, hk]
      · rw [if_neg hs]
        have hmem : ¬ ∃ b ∈ (List.replicate k (expand a)).flatten, 128 ≤ b := by
          intro h
          exact hs ((bad_flatten_replicate k (expand a)).1 h).2
        rw [if_neg hmem]
        simp [power_id]

private theorem all_lt_iff (s : Bytes) :
    s.all (fun b => b < 128) = true ↔ ∀ b ∈ s, b < 128 := by
  induction s with
  | nil => simp
  | cons b bs ih => simp [ih]

theorem sound_SevenBitFastPath : AsciiOnly.SevenBitFastPath.Goal := by
  intro x
  rw [matched, compile_table]
  by_cases hs : ∃ b ∈ expand x, 128 ≤ b
  · rw [if_pos hs]
    have hnot : ¬ (∀ b ∈ expand x, b < 128) := by
      rcases hs with ⟨b, hbmem, hb128⟩
      intro h
      have hlt := h b hbmem
      omega
    have hallf : (expand x).all (fun b => b < 128) = false := by
      cases h : (expand x).all (fun b => b < 128) <;> simp at h ⊢
      exact hnot h
    rw [hallf]
    simp [applyT, reader]
  · rw [if_neg hs]
    have hallt : (expand x).all (fun b => b < 128) = true := by
      apply (all_lt_iff (expand x)).2
      intro b hbmem
      by_cases hb : b < 128
      · exact hb
      · exact False.elim (hs ⟨b, hbmem, by omega⟩)
    rw [hallt]
    simp [applyT, reader]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_GrammarScans_AsciiOnly_SevenBitFastPath
