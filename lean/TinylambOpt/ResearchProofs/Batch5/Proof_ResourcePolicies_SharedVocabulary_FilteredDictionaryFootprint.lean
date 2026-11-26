import TinylambOpt.ResearchGoals.ResourcePolicies.SharedVocabulary

namespace TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.SharedVocabulary.Goal_FilteredDictionaryFootprint

open TinylambOpt.ResearchGoals.ResourcePolicies.SharedVocabulary


/-! Scratch proof for the FilteredDictionaryFootprint goal. Selecting a subset
of rows keeps only a filtered subset of dictionary symbols and no more row ids,
so a SQL selection can never increase the modeled storage charge. -/

open TinylambOpt.ResearchGoals.ResourcePolicies.SharedVocabulary
open TinylambOpt.ResearchGoals.Compression.Wire

theorem nodup_eraseDups : ∀ l : List Symbol, l.eraseDups.Nodup
  | [] => by simp
  | x :: xs => by
      rw [List.eraseDups_cons]
      refine List.nodup_cons.mpr ⟨?_, ?_⟩
      · simp [List.mem_filter]
      · exact nodup_eraseDups (xs.filter fun b => !(b == x))
termination_by l => l.length
decreasing_by simpa using Nat.lt_succ_of_le (List.length_filter_le (fun b : Symbol => !(b == x)) xs)

theorem nodup_filter {p : Symbol → Bool} {l : List Symbol} (h : l.Nodup) :
    (l.filter p).Nodup := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      by_cases hp : p x = true
      · rw [List.filter_cons_of_pos hp]
        apply List.nodup_cons.mpr
        constructor
        · intro hx
          exact (List.nodup_cons.mp h).1 (List.mem_filter.mp hx).1
        · exact ih (List.nodup_cons.mp h).2
      · rw [List.filter_cons_of_neg hp]
        exact ih (List.nodup_cons.mp h).2

theorem dictSize_perm {u v : List Symbol} (p : u.Perm v) : dictSize u = dictSize v := by
  unfold dictSize
  have pm := p.map (fun s : Symbol => ubytes s.length + s.length)
  exact pm.foldr_eq' (by intro x hx y hy z; exact Nat.add_left_comm y x z) 0

theorem dictSize_filter_partition (p : Symbol → Bool) (l : List Symbol) :
    dictSize (l.filter p) + dictSize (l.filter (fun s => !p s)) = dictSize l := by
  induction l with
  | nil => simp [dictSize]
  | cons x xs ih =>
      by_cases hp : p x = true
      · have hpneg : ¬ ((fun s : Symbol => !p s) x = true) := by simp [hp]
        rw [List.filter_cons_of_pos hp]
        rw [List.filter_cons_of_neg (p := fun s : Symbol => !p s) hpneg]
        simp [dictSize] at ih ⊢
        omega
      · have hp' : p x = false := Bool.eq_false_iff.mpr hp
        have hpneg : ((fun s : Symbol => !p s) x = true) := by simp [hp']
        rw [List.filter_cons_of_neg hp]
        rw [List.filter_cons_of_pos (p := fun s : Symbol => !p s) hpneg]
        simp [dictSize] at ih ⊢
        omega

theorem log2_mono {a b : Nat} (h : a ≤ b) : a.log2 ≤ b.log2 := by
  rcases Nat.eq_zero_or_pos a with ha | ha
  · rw [ha]
    simp
  · have hb : b ≠ 0 := by omega
    have h2 := (Nat.le_log2 (by omega : a ≠ 0)).mp (Nat.le_refl (a.log2))
    exact (Nat.le_log2 hb).mpr (Nat.le_trans h2 h)

theorem width_mono {a b : Nat} (h : a ≤ b) : width a ≤ width b := by
  rcases Nat.eq_zero_or_pos a with ha | ha
  · subst a
    cases b with
    | zero => simp [width]
    | succ b => simp [width]
  · have hb : b ≠ 0 := by omega
    have ha' : ¬(a == 0) := by simpa using (by omega : a ≠ 0)
    have hb' : ¬(b == 0) := by simpa using (by omega : b ≠ 0)
    simp [width, ha', hb']
    exact log2_mono h


theorem dictSize_filter_le (p : Symbol → Bool) (l : List Symbol) :
    dictSize (l.filter p) ≤ dictSize l := by
  have h := dictSize_filter_partition p l
  omega

theorem byteCount_mono {a b : Nat} (h : a ≤ b) : byteCount a ≤ byteCount b := by
  unfold byteCount
  exact Nat.div_le_div_right (by omega)

theorem vocab_filter_perm (xs : List Symbol) (p : Symbol → Bool) :
    (vocab (xs.filter p)).Perm ((vocab xs).filter p) := by
  apply (List.perm_ext_iff_of_nodup
    (nodup_eraseDups _) (nodup_filter (nodup_eraseDups xs))).2
  intro s
  simp

theorem sound : TinylambOpt.ResearchGoals.ResourcePolicies.SharedVocabulary.FilteredDictionaryFootprint.Goal := by
  intro xs p
  have hp := vocab_filter_perm xs p
  have hdictEq : dictSize (vocab (xs.filter p)) = dictSize ((vocab xs).filter p) :=
    dictSize_perm hp
  have hdict : dictSize (vocab (xs.filter p)) ≤ dictSize (vocab xs) := by
    rw [hdictEq]
    exact dictSize_filter_le p (vocab xs)
  have hkeys : (vocab (xs.filter p)).length ≤ (vocab xs).length := by
    rw [hp.length_eq]
    exact List.length_filter_le p (vocab xs)
  have hrows : (xs.filter p).length ≤ xs.length := List.length_filter_le p xs
  have hwidth : width ((vocab (xs.filter p)).length - 1) ≤
      width ((vocab xs).length - 1) := width_mono (by omega)
  have hmul : (xs.filter p).length * width ((vocab (xs.filter p)).length - 1) ≤
      xs.length * width ((vocab xs).length - 1) := Nat.mul_le_mul hrows hwidth
  have hid : idSize (xs.filter p).length (vocab (xs.filter p)).length ≤
      idSize xs.length (vocab xs).length := by
    unfold idSize
    exact byteCount_mono hmul
  unfold charge
  omega
end TinylambOpt.ResearchProofs.Batch5.ResourcePolicies.SharedVocabulary.Goal_FilteredDictionaryFootprint
