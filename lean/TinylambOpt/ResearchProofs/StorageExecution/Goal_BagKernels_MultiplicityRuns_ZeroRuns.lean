import TinylambOpt.ResearchGoals.StorageExecution.BagKernels

set_option maxHeartbeats 0
set_option maxRecDepth 10000

namespace TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_MultiplicityRuns_ZeroRuns

open TinylambOpt.ResearchGoals.StorageExecution.BagKernels
open TinylambOpt.ResearchGoals.StorageExecution.BagKernels.MultiplicityRuns

#eval live bag ([] : List (Nat × Nat))
#eval live bag ([(1,0),(2,3),(1,2)] : List (Nat × Nat))
#eval live bag ([(1,0),(2,3),(1,2)].filter (fun r => 0 < r.2))
#eval live bag ([(5,0),(5,0),(7,1),(5,4),(9,0)] : List (Nat × Nat))
#eval live bag ([(5,0),(5,0),(7,1),(5,4),(9,0)].filter (fun r => 0 < r.2))

example : (live bag ([(1,0),(2,3),(1,2)].filter (fun r => 0 < r.2))).Perm
    (live bag [(1,0),(2,3),(1,2)]) := by decide
example : (live bag ([(5,0),(5,0),(7,1),(5,4),(9,0)].filter (fun r => 0 < r.2))).Perm
    (live bag [(5,0),(5,0),(7,1),(5,4),(9,0)]) := by decide

private theorem count_eq_one_of_mem_nodup {α : Type} [BEq α] [LawfulBEq α]
    {l : List α} {a : α} (hn : l.Nodup) (ha : a ∈ l) : l.count a = 1 := by
  induction l with
  | nil => cases ha
  | cons b bs ih =>
      rw [List.nodup_cons] at hn
      simp only [List.mem_cons] at ha
      rw [List.count_cons]
      by_cases hba : b = a
      · subst b
        rw [List.count_eq_zero_of_not_mem hn.1]
        simp
      · have ha' : a ∈ bs := by cases ha <;> simp_all
        have hbeq : (b == a) = false := by simp [hba]
        simp [hbeq, ih hn.2 ha']

private theorem nodup_filter_eraseDups {α : Type} [BEq α] [LawfulBEq α]
    (l : List α) (p : α → Bool) : (l.filter p).eraseDups.Nodup := by
  induction l generalizing p with
  | nil => simp
  | cons b bs ih =>
      by_cases hb : p b = true
      · simp only [List.filter_cons, hb, ite_true]
        rw [List.eraseDups_cons, List.nodup_cons]
        constructor
        · simp [List.mem_eraseDups]
        · simpa [List.filter_filter] using ih (fun x => (! x == b) && p x)
      · simp only [List.filter_cons, hb]
        exact ih p

private theorem filter_const_true {α : Type} (l : List α) : l.filter (fun _ => true) = l := by
  induction l with
  | nil => simp
  | cons a as ih => simp [ih]

private theorem nodup_eraseDups {α : Type} [BEq α] [LawfulBEq α] (l : List α) :
    l.eraseDups.Nodup := by
  rw [← filter_const_true l]
  exact nodup_filter_eraseDups l (fun _ => true)

private theorem mem_keys_of_mass_pos (rs : List (Nat × Nat)) (k : Nat)
    (h : 0 < bag.mass rs k) : k ∈ bag.keys rs := by
  induction rs with
  | nil => simp [bag] at h
  | cons r rs ih =>
      obtain ⟨x, n⟩ := r
      simp only [bag, List.map_cons, List.mem_cons]
      by_cases hx : x == k
      · exact Or.inl (eq_of_beq hx).symm
      · right
        apply ih
        simpa [bag, hx] using h

private theorem mem_live_iff_mass_pos (rs : List (Nat × Nat)) (k : Nat) :
    k ∈ live bag rs ↔ 0 < bag.mass rs k := by
  unfold live
  simp only [List.mem_filter, List.mem_eraseDups]
  constructor
  · intro h
    exact by simpa using h.2
  · intro h
    exact ⟨mem_keys_of_mass_pos rs k h, by simpa using h⟩

private theorem nodup_live (rs : List (Nat × Nat)) : (live bag rs).Nodup := by
  unfold live
  exact List.Pairwise.filter _ (nodup_eraseDups (bag.keys rs))

private theorem mass_cons (x n k : Nat) (rs : List (Nat × Nat)) :
    bag.mass ((x,n)::rs) k = (if x == k then n else 0) + bag.mass rs k := by
  by_cases h : x == k <;> simp [bag, h]

private theorem mass_filter_pos (rs : List (Nat × Nat)) (k : Nat) :
    bag.mass (rs.filter (fun r => 0 < r.2)) k = bag.mass rs k := by
  induction rs with
  | nil => simp [bag]
  | cons r rs ih =>
      obtain ⟨x, n⟩ := r
      by_cases hn : 0 < n
      · simp [hn, mass_cons, ih]
      · have hn0 : n = 0 := by omega
        subst n
        simp [mass_cons, ih]

theorem sound_ZeroRuns : ZeroRuns.Goal := by
  intro rs
  rw [List.perm_iff_count]
  intro k
  by_cases h : 0 < bag.mass rs k
  · have h' : 0 < bag.mass (rs.filter (fun r => 0 < r.2)) k := by
      simpa [mass_filter_pos] using h
    have hl : (live bag (rs.filter (fun r => 0 < r.2))).count k = 1 :=
      count_eq_one_of_mem_nodup (nodup_live (rs.filter (fun r => 0 < r.2)))
        ((mem_live_iff_mass_pos (rs.filter (fun r => 0 < r.2)) k).2 h')
    have hr : (live bag rs).count k = 1 :=
      count_eq_one_of_mem_nodup (nodup_live rs) ((mem_live_iff_mass_pos rs k).2 h)
    rw [hl, hr]
  · have h' : ¬ 0 < bag.mass (rs.filter (fun r => 0 < r.2)) k := by
      intro hk
      exact h (by simpa [mass_filter_pos] using hk)
    have hl : (live bag (rs.filter (fun r => 0 < r.2))).count k = 0 :=
      List.count_eq_zero_of_not_mem (by
        intro hk
        exact h' ((mem_live_iff_mass_pos (rs.filter (fun r => 0 < r.2)) k).1 hk))
    have hr : (live bag rs).count k = 0 :=
      List.count_eq_zero_of_not_mem (by
        intro hk
        exact h ((mem_live_iff_mass_pos rs k).1 hk))
    rw [hl, hr]

end TinylambOpt.ResearchProofs.StorageExecution.Goal_BagKernels_MultiplicityRuns_ZeroRuns
