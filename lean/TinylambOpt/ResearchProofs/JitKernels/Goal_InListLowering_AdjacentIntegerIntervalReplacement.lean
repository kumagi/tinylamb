import TinylambOpt.ResearchGoals.JitKernels.InListLowering

/-!
A sorted non-NULL build list whose distinct values fill the closed interval
`[l, h]` accepts a probe exactly when the probe lies in that interval. The
minimum and maximum give the interval bounds; the distinct count rules out any
gap inside it. These list/counting lemmas justify replacing a dense IN list by
a single interval comparison in SQL execution.
-/

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.JitKernels.InListLowering.Goal_AdjacentIntegerIntervalReplacement

open TinylambOpt.ResearchGoals.JitKernels.InListLowering

theorem eraseDups_nodup {α : Type} [BEq α] [LawfulBEq α] (as : List α) :
    as.eraseDups.Nodup := by
  cases as with
  | nil => simp
  | cons a as =>
      rw [List.eraseDups_cons]
      refine List.nodup_cons.mpr ⟨?_, ?_⟩
      · intro hm
        have hfmem : a ∈ as.filter (fun b => ! b == a) := by
          simp at hm
        have hf := List.mem_filter.mp hfmem
        have hbad : (! (a == a)) = true := hf.2
        simp at hbad
      · exact eraseDups_nodup (as.filter fun b => ! b == a)
termination_by as.length
decreasing_by
  exact Nat.lt_of_le_of_lt (List.length_filter_le _ as) (Nat.lt_succ_self as.length)

theorem not_mem_erase_self_of_nodup (d : List Int) (m : Int) (hn : d.Nodup) :
    m ∉ d.erase m := by
  intro hmem
  cases d with
  | nil => simp [List.erase] at hmem
  | cons a as =>
      have ⟨hanot, hnas⟩ := List.nodup_cons.mp hn
      by_cases heq : a = m
      · subst a
        rw [List.erase_cons_head] at hmem
        exact hanot hmem
      · have hbeq : ¬ (a == m) = true := by
          intro h
          exact heq (LawfulBEq.eq_of_beq h)
        rw [List.erase_cons_tail hbeq] at hmem
        simp only [List.mem_cons] at hmem
        rcases hmem with hma | hmem
        · exact heq hma.symm
        · exact not_mem_erase_self_of_nodup as m hnas hmem

theorem nodup_length_le_interval (d : List Int) (l h : Int)
    (hn : d.Nodup) (hlo : ∀ z ∈ d, l ≤ z) (hhi : ∀ z ∈ d, z ≤ h) :
    d.length ≤ (h - l + 1).toNat := by
  by_cases hd : d = []
  · simp [hd]
  · obtain ⟨m, hmin⟩ : ∃ m, d.min? = some m := by
      cases hm : d.min? with
      | none =>
          simp at hm
          contradiction
      | some m => exact ⟨m, rfl⟩
    have hmprops := List.min?_eq_some_iff.mp hmin
    have hminmem : m ∈ d := hmprops.1
    have hminle : ∀ z ∈ d, m ≤ z := hmprops.2
    have hmh : m ≤ h := hhi m hminmem
    have hlm : l ≤ m := hlo m hminmem
    have hlt : (d.erase m).length < d.length := by
      rw [List.length_erase_of_mem hminmem]
      have hdpos : 0 < d.length := by
        cases d with
        | nil => simp at hd
        | cons a as => simp
      omega
    have hne_of_mem : ∀ {z : Int}, z ∈ d.erase m → z ≠ m := by
      intro z hz heq
      subst z
      exact not_mem_erase_self_of_nodup d m hn hz
    have hlo' : ∀ z ∈ d.erase m, m + 1 ≤ z := by
      intro z hz
      have hne : z ≠ m := hne_of_mem hz
      have hzd : z ∈ d := (List.mem_erase_of_ne hne).mp hz
      have hmz : m ≤ z := hminle z hzd
      omega
    have hhi' : ∀ z ∈ d.erase m, z ≤ h := by
      intro z hz
      have hne : z ≠ m := hne_of_mem hz
      have hzd : z ∈ d := (List.mem_erase_of_ne hne).mp hz
      exact hhi z hzd
    have ih := nodup_length_le_interval (d.erase m) (m + 1) h (hn.erase m) hlo' hhi'
    have hto' : (h - (m + 1) + 1).toNat = (h - m).toNat := by
      congr 1
      omega
    rw [hto'] at ih
    have hnonneg : 0 ≤ h - l := by omega
    have hto : (h - l + 1).toNat = (h - l).toNat + 1 := by
      rw [show h - l + 1 = (h - l) + 1 by omega]
      rw [Int.toNat_add hnonneg (by decide : 0 ≤ (1 : Int))]
      simp
    have hmono : (h - m).toNat ≤ (h - l).toNat := Int.toNat_le_toNat (by omega)
    have hdlen : d.length = (d.erase m).length + 1 := by
      rw [List.length_erase_of_mem hminmem]
      have hdpos : 0 < d.length := by
        cases d with
        | nil => simp at hd
        | cons a as => simp
      omega
    rw [hdlen, hto]
    omega
termination_by d.length
decreasing_by exact hlt

theorem missing_bound (d : List Int) (l h x : Int) (hn : d.Nodup)
    (hlo : ∀ z ∈ d, l ≤ z) (hhi : ∀ z ∈ d, z ≤ h)
    (hxl : l ≤ x) (hxh : x ≤ h) (hxnot : x ∉ d) :
    d.length ≤ (h - l).toNat := by
  have hn2 : (x :: d).Nodup := by simp [hn, hxnot]
  have hlo2 : ∀ z ∈ x :: d, l ≤ z := by
    intro z hz
    simp only [List.mem_cons] at hz
    rcases hz with rfl | hz
    · exact hxl
    · exact hlo z hz
  have hhi2 : ∀ z ∈ x :: d, z ≤ h := by
    intro z hz
    simp only [List.mem_cons] at hz
    rcases hz with rfl | hz
    · exact hxh
    · exact hhi z hz
  have hb := nodup_length_le_interval (x :: d) l h hn2 hlo2 hhi2
  simp only [List.length_cons] at hb
  have hnonneg : 0 ≤ h - l := by omega
  have hto : (h - l + 1).toNat = (h - l).toNat + 1 := by
    rw [show h - l + 1 = (h - l) + 1 by omega]
    rw [Int.toNat_add hnonneg (by decide : 0 ≤ (1 : Int))]
    simp
  rw [hto] at hb
  omega

theorem test_some (xs : Keys) (x : Int) :
    test xs (some x) =
      if some x ∈ xs then some true else if none ∈ xs then none else some false := by
  unfold test
  by_cases h : xs = []
  · simp [h]
  · simp [h]

theorem sound : AdjacentIntegerIntervalReplacement.Goal := by
  intro vs l h x hmin hmax hlen
  have hminp := List.min?_eq_some_iff.mp hmin
  have hmaxp := List.max?_eq_some_iff.mp hmax
  have hlo : ∀ z ∈ vs, l ≤ z := hminp.2
  have hhi : ∀ z ∈ vs, z ≤ h := hmaxp.2
  unfold keep
  rw [test_some]
  by_cases hxmem : x ∈ vs
  · have hxl : l ≤ x := hlo x hxmem
    have hxh : x ≤ h := hhi x hxmem
    simp [hxmem, hxl, hxh]
  · by_cases hxrange : l ≤ x ∧ x ≤ h
    · have hallo : ∀ z ∈ vs.eraseDups, l ≤ z := by
        intro z hz
        exact hlo z (by simpa using hz)
      have hhi' : ∀ z ∈ vs.eraseDups, z ≤ h := by
        intro z hz
        exact hhi z (by simpa using hz)
      have hb := missing_bound vs.eraseDups l h x (eraseDups_nodup vs) hallo hhi'
        hxrange.1 hxrange.2 (by simpa using hxmem)
      have hbInt : (vs.eraseDups.length : Int) ≤ (h - l) := by
        have hnonneg : 0 ≤ h - l := by omega
        have hcasteq : (((h - l).toNat : Nat) : Int) = h - l := Int.toNat_of_nonneg hnonneg
        have hbcast : (vs.eraseDups.length : Int) ≤ ((h - l).toNat : Int) := by
          exact Int.ofNat_le.mpr hb
        rw [hcasteq] at hbcast
        exact hbcast
      rw [hlen] at hbInt
      omega
    · have : ¬ (l ≤ x ∧ x ≤ h) := hxrange
      simp [hxmem, this]

end TinylambOpt.ResearchProofs.JitKernels.InListLowering.Goal_AdjacentIntegerIntervalReplacement
