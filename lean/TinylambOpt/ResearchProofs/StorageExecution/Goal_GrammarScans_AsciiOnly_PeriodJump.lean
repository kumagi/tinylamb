import TinylambOpt.ResearchGoals.StorageExecution.GrammarScans

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_GrammarScans_AsciiOnly_PeriodJump

open TinylambOpt.ResearchGoals.Compression.Wire
open TinylambOpt.ResearchGoals.StorageExecution.GrammarScans
open TinylambOpt.ResearchGoals.StorageExecution.GrammarScans.AsciiOnly

/-! Small-input truth check; inputs violating the cycle hypothesis are ignored. -/

private def samples : List Rope :=
  [ .lit [], .lit [65], .lit [200], .lit [65, 200], .lit [200, 65],
    .cat (.lit [65]) (.lit [200]), .cat (.lit [200]) (.lit [65]),
    .rep 0 (.lit [200]), .rep 2 (.lit [65]), .rep 3 (.lit []),
    .rep 2 (.lit [65, 200]), .cat (.rep 2 (.lit [65])) (.lit [200]) ]

private def check : Bool :=
  samples.all fun x => (List.range 2).all fun q =>
    (List.range 5).all fun period =>
      period == 0 ||
        (List.range 6).all fun n =>
          applyT (power period (compile reader x)) q != q ||
            matched reader (.rep n x) q == matched reader (.rep (n % period) x) q

#eval check

example : check := by native_decide

/-! The reader is sticky: a non-ASCII byte sends every state to 1, an ASCII
    byte preserves the incoming state. -/

private theorem step_fold (s : Bytes) (q : Nat) :
    s.foldl reader.step q = if (∃ b ∈ s, 128 ≤ b) then 1 else q := by
  induction s generalizing q with
  | nil => simp [reader]
  | cons b bs ih =>
      rw [List.foldl_cons, ih]
      by_cases hb : 128 ≤ b
      · have hmem : ∃ c ∈ b :: bs, 128 ≤ c := ⟨b, by simp, hb⟩
        have hnotlt : ¬ b < 128 := by omega
        have hstep : reader.step q b = 1 := by simp [reader, hnotlt]
        rw [if_pos hmem, hstep]
        simp
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
    simp only [List.mem_append] at hbmem
    rcases hbmem with h | h
    · exact Or.inl ⟨b, h, hb128⟩
    · exact Or.inr ⟨b, h, hb128⟩
  · rintro (⟨b, hb, hb128⟩ | ⟨b, hb, hb128⟩)
    · exact ⟨b, by simp [hb], hb128⟩
    · exact ⟨b, by simp [hb], hb128⟩

private theorem bad_flatten_replicate (k : Nat) (s : Bytes) :
    (∃ b ∈ (List.replicate k s).flatten, 128 ≤ b) ↔
      0 < k ∧ (∃ b ∈ s, 128 ≤ b) := by
  induction k with
  | zero => simp
  | succ k ih =>
      simp only [List.replicate_succ, List.flatten_cons, List.mem_append]
      constructor
      · rintro ⟨b, hbs | hbflat, hb128⟩
        · exact ⟨by omega, ⟨b, hbs, hb128⟩⟩
        · have hres := ih.mp ⟨b, hbflat, hb128⟩
          exact ⟨by omega, hres.2⟩
      · rintro ⟨_, ⟨b, hbs, hb128⟩⟩
        exact ⟨b, Or.inl hbs, hb128⟩

/-! The only compiled tables are the identity-like [0,1] and the
    constant-one table [1,1]. -/

private theorem power_zero (t : Table) : power 0 t = List.range t.length := by
  unfold power
  split
  · rfl
  · rename_i h
    omega

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
      by_cases ha : ∃ c ∈ expand a, 128 ≤ c
      · by_cases hb : ∃ c ∈ expand b, 128 ≤ c
        · simp only [bad_append, ha, hb, chain, applyT, ↓reduceIte]
          simp [applyT]
        · simp only [bad_append, ha, hb, chain, applyT, ↓reduceIte]
          simp [applyT]
      · by_cases hb : ∃ c ∈ expand b, 128 ≤ c
        · simp only [bad_append, ha, hb, chain, applyT, ↓reduceIte]
          simp [applyT]
        · simp only [bad_append, ha, hb, chain, applyT, ↓reduceIte]
          simp [applyT]
  | rep k a ih =>
      rw [compile, ih]
      change power k (if (∃ b ∈ expand a, 128 ≤ b) then [1, 1] else [0, 1]) =
        if (∃ b ∈ (List.replicate k (expand a)).flatten, 128 ≤ b) then [1, 1] else [0, 1]
      by_cases hs : ∃ b ∈ expand a, 128 ≤ b
      · rw [if_pos hs]
        by_cases hk : k = 0
        · subst k
          change power 0 [1, 1] = [0, 1]
          rw [power_zero]
          change List.range 2 = [0, 1]
          native_decide
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

theorem sound_PeriodJump : AsciiOnly.PeriodJump.Goal := by
  intro x q period n hq hperiod hcycle
  have hq01 : q = 0 ∨ q = 1 := by simp [reader] at hq; omega
  rcases hq01 with rfl | rfl
  · -- A cycle back to 0 with positive period forces the phrase to be clean.
    have hxnone : ¬ ∃ b ∈ expand x, 128 ≤ b := by
      intro hs
      have hpne : period ≠ 0 := Nat.ne_of_gt hperiod
      rw [compile_table x] at hcycle
      simp [hs, hpne, power_const, applyT] at hcycle
    have hbad_n : ¬ ∃ b ∈ expand (.rep n x), 128 ≤ b := by
      dsimp [expand]
      intro h
      exact hxnone ((bad_flatten_replicate n (expand x)).1 h).2
    have hbad_mod : ¬ ∃ b ∈ expand (.rep (n % period) x), 128 ≤ b := by
      dsimp [expand]
      intro h
      exact hxnone ((bad_flatten_replicate (n % period) (expand x)).1 h).2
    rw [matched, matched,
      compile_table (.rep n x), compile_table (.rep (n % period) x)]
    rw [if_neg hbad_n, if_neg hbad_mod]
  · -- State 1 is absorbing and non-accepting, so both repetitions reject.
    rw [matched, matched,
      compile_table (.rep n x), compile_table (.rep (n % period) x)]
    by_cases hn : ∃ b ∈ expand (.rep n x), 128 ≤ b <;>
      by_cases hm : ∃ b ∈ expand (.rep (n % period) x), 128 ≤ b <;>
        simp [hn, hm, applyT, reader]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_GrammarScans_AsciiOnly_PeriodJump
