import TinylambOpt.ResearchGoals.RestartRaces.Concurrency
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
namespace TinylambOpt.ResearchProofs.RestartRaces.Concurrency_CycleInstant

open TinylambOpt.ResearchGoals.RestartRaces.Concurrency.CycleInstant

private theorem maxD_eq_foldl (l : List Nat) : l.max?.getD 0 = l.foldl max 0 := by
  cases l with
  | nil => simp [List.max?]
  | cons a as =>
      simp [List.max?, Nat.max_eq_right (Nat.zero_le a)]

private theorem init_le_foldl_max (l : List Nat) (c : Nat) : c ≤ l.foldl max c := by
  induction l generalizing c with
  | nil => simp
  | cons a l ih =>
      simp only [List.foldl_cons]
      exact Nat.le_trans (Nat.le_max_left c a) (ih (max c a))

private theorem mem_le_foldl_max (l : List Nat) (c x : Nat) (h : x ∈ l) :
    x ≤ l.foldl max c := by
  induction l generalizing c x with
  | nil => simp at h
  | cons a l ih =>
      simp only [List.mem_cons] at h
      simp only [List.foldl_cons]
      rcases h with hxa | hx
      · subst x
        exact Nat.le_trans (Nat.le_max_right c a) (init_le_foldl_max l (max c a))
      · exact ih (max c a) x hx

private theorem foldl_max_le (l : List Nat) (c t : Nat) (h : ∀ x ∈ l, x ≤ t) (hc : c ≤ t) :
    l.foldl max c ≤ t := by
  induction l generalizing c with
  | nil => exact hc
  | cons a l ih =>
      simp only [List.foldl_cons]
      exact ih (max c a) (fun x hx => h x (by simp [hx]))
        (Nat.max_le.mpr ⟨hc, h a (by simp)⟩)

private theorem foldl_min_le_init (l : List Nat) (c : Nat) : l.foldl min c ≤ c := by
  induction l generalizing c with
  | nil => simp
  | cons a l ih =>
      simp only [List.foldl_cons]
      exact Nat.le_trans (ih (min c a)) (Nat.min_le_left c a)

private theorem foldl_min_le_of_mem (l : List Nat) (c x : Nat) (h : x ∈ l) :
    l.foldl min c ≤ x := by
  induction l generalizing c x with
  | nil => simp at h
  | cons a l ih =>
      simp only [List.mem_cons] at h
      simp only [List.foldl_cons]
      rcases h with hxa | hx
      · subst x
        exact Nat.le_trans (foldl_min_le_init l (min c a)) (Nat.min_le_right c a)
      · exact ih (min c a) x hx

private theorem le_foldl_min (l : List Nat) (c t : Nat) (hc : t ≤ c) (h : ∀ x ∈ l, t ≤ x) :
    t ≤ l.foldl min c := by
  induction l generalizing c with
  | nil => exact hc
  | cons a l ih =>
      simp only [List.foldl_cons]
      exact ih (min c a) (Nat.le_min.mpr ⟨hc, h a (by simp)⟩)
        (fun x hx => h x (by simp [hx]))

private theorem minD_cons (a : Nat) (as : List Nat) : (a :: as).min?.getD 0 = as.foldl min a := by
  simp [List.min?]

private theorem minD_mem (l : List Nat) (x : Nat) (h : x ∈ l) : l.min?.getD 0 ≤ x := by
  cases l with
  | nil => simp at h
  | cons a as =>
      rw [minD_cons]
      simp only [List.mem_cons] at h
      rcases h with hxa | hx
      · subst x
        exact foldl_min_le_init as a
      · exact foldl_min_le_of_mem as a x hx

private theorem le_minD (l : List Nat) (t : Nat) (hne : l ≠ []) (h : ∀ x ∈ l, t ≤ x) :
    t ≤ l.min?.getD 0 := by
  cases l with
  | nil => contradiction
  | cons a as =>
      rw [minD_cons]
      exact le_foldl_min as a t (h a (by simp)) (fun x hx => h x (by simp [hx]))

theorem sound : Goal := by
  intro ws hne
  constructor
  · intro hle live hcert
    let t : Nat := (ws.map Prod.fst).max?.getD 0
    refine ⟨t, ?_⟩
    intro i hi
    have hlt : i < ws.length := by simpa [List.mem_range] using hi
    have hsome : ws[i]? = some ws[i] := List.getElem?_eq_getElem hlt
    have hwmem : ws[i] ∈ ws := List.getElem_mem hlt
    have hstart : (ws[i]).1 ≤ t := by
      dsimp [t]
      rw [maxD_eq_foldl]
      exact mem_le_foldl_max (ws.map Prod.fst) 0 (ws[i]).1 (List.mem_map_of_mem hwmem)
    have hend : t ≤ (ws[i]).2 := by
      have hm' : (ws.map Prod.snd).min?.getD 0 ≤ (ws[i]).2 := by
        apply minD_mem
        exact List.mem_map_of_mem hwmem
      exact Nat.le_trans hle hm'
    exact hcert i (ws[i]) hsome t ⟨hstart, hend⟩
  · intro H
    by_cases hle' : (ws.map Prod.fst).max?.getD 0 ≤ (ws.map Prod.snd).min?.getD 0
    · exact hle'
    · exfalso
      let live : Nat → Nat → Prop := fun i t => match ws[i]? with
        | some w => covers w t
        | none => True
      have hcert : ∀ i w, ws[i]? = some w → ∀ t, covers w t → live i t := by
        intro i w hi t ht
        dsimp [live]
        rw [hi]
        exact ht
      obtain ⟨t, ht⟩ := H live hcert
      have hcovers : ∀ w ∈ ws, covers w t := by
        intro w hw
        obtain ⟨i, hi⟩ := List.getElem?_of_mem hw
        have hi' : i ∈ List.range ws.length := by
          rw [List.mem_range]
          exact (List.getElem?_eq_some_iff.mp hi).1
        have hti := ht i hi'
        dsimp [live] at hti
        rw [hi] at hti
        exact hti
      have hstart_le : (ws.map Prod.fst).max?.getD 0 ≤ t := by
        rw [maxD_eq_foldl]
        apply foldl_max_le
        · intro x hx
          obtain ⟨w, hw, rfl⟩ := List.mem_map.mp hx
          exact (hcovers w hw).1
        · exact Nat.zero_le t
      have hle_end : t ≤ (ws.map Prod.snd).min?.getD 0 := by
        apply le_minD
        · intro hnil
          cases ws with
          | nil => exact hne rfl
          | cons w wr => simp at hnil
        · intro x hx
          obtain ⟨w, hw, rfl⟩ := List.mem_map.mp hx
          exact (hcovers w hw).2
      exact hle' (Nat.le_trans hstart_le hle_end)

end TinylambOpt.ResearchProofs.RestartRaces.Concurrency_CycleInstant
