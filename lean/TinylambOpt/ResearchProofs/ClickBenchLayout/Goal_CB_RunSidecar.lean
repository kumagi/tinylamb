import TinylambOpt.ResearchGoals.ClickBenchLayout.Storage

set_option maxHeartbeats 0
set_option linter.unusedVariables false
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.ClickBenchLayout.Goal_CB_RunSidecar

open TinylambOpt.ResearchGoals.ClickBenchLayout.Storage
open RunSidecar

def desc : List Nat → Nat
  | x :: y :: t => (if x < y then 0 else 1) + desc (y :: t)
  | _ => 0

def Disj (rs : List (List Nat)) : Prop := rs.Pairwise (fun a b => ∀ x ∈ a, x ∉ b)

theorem runs_cons (x : Nat) (xs : List Nat) :
    runs (x :: xs) = match runs xs with
      | (y :: ys) :: rest => if x < y then (x :: y :: ys) :: rest else [x] :: (y :: ys) :: rest
      | rs => [x] :: rs := rfl

theorem runs_inv : ∀ xs : List Nat,
    (runs xs).flatten = xs ∧
    (∀ r ∈ runs xs, r ≠ [] ∧ r.Pairwise (· < ·)) ∧
    (∀ x t, xs = x :: t → ∃ ys rest, runs xs = (x :: ys) :: rest) ∧
    (xs.Nodup → Disj (runs xs)) ∧
    (∀ x t, xs = x :: t → (runs xs).length = desc xs + 1) := by
  intro xs
  induction xs with
  | nil => simp [runs, Disj]
  | cons x xs ih =>
    obtain ⟨hfl, hrun, hhead, hdis, hlen⟩ := ih
    cases hr : runs xs with
    | nil =>
      have hxs : xs = [] := by rw [← hfl, hr]; rfl
      subst hxs
      have e : runs [x] = [[x]] := by simp [runs]
      rw [e]
      refine ⟨by simp, ?_, ?_, ?_, ?_⟩
      · intro r hr; simp at hr; subst hr; simp
      · intro x' t h; simp at h; obtain ⟨rfl, rfl⟩ := h; exact ⟨[], [], rfl⟩
      · intro _; simp [Disj]
      · intro x' t h; simp [desc]
    | cons r0 rest0 =>
      cases r0 with
      | nil => exfalso; exact (hrun [] (by rw [hr]; simp)).1 rfl
      | cons y ys =>
        have hfl' : (y :: ys) ++ rest0.flatten = xs := by simpa [hr] using hfl
        have hr0 : (y :: ys) ≠ [] ∧ (y :: ys).Pairwise (· < ·) := hrun _ (by rw [hr]; simp)
        have hrest : ∀ r ∈ rest0, r ≠ [] ∧ r.Pairwise (· < ·) := fun r h => hrun r (by rw [hr]; simp [h])
        have hxs : xs = y :: (ys ++ rest0.flatten) := by rw [← hfl']; rfl
        have hlen' : (runs xs).length = desc xs + 1 := hlen y _ hxs
        rw [hr] at hlen'
        have hsub : ∀ b ∈ (y :: ys) :: rest0, ∀ z ∈ b, z ∈ xs := by
          intro b hb z hz
          rw [← hfl, hr]
          exact List.mem_flatten.mpr ⟨b, hb, hz⟩
        by_cases hxy : x < y
        · have e : runs (x :: xs) = (x :: y :: ys) :: rest0 := by
            rw [runs_cons, hr]; simp [hxy]
          rw [e]
          refine ⟨?_, ?_, ?_, ?_, ?_⟩
          · show x :: ((y :: ys) ++ rest0.flatten) = x :: xs
            rw [hfl']
          · intro r hr'
            rcases List.mem_cons.mp hr' with rfl | h
            · refine ⟨by simp, ?_⟩
              rw [List.pairwise_cons]
              refine ⟨?_, hr0.2⟩
              intro a ha
              rcases List.mem_cons.mp ha with rfl | ha
              · exact hxy
              · have := (List.pairwise_cons.mp hr0.2).1 a ha
                omega
            · exact hrest r h
          · intro x' t h; simp at h; obtain ⟨rfl, rfl⟩ := h; exact ⟨y :: ys, rest0, rfl⟩
          · intro hnd
            have hxn : x ∉ xs := (List.nodup_cons.mp hnd).1
            have hD := hdis (List.nodup_cons.mp hnd).2
            rw [hr] at hD
            unfold Disj at hD ⊢
            rw [List.pairwise_cons] at hD ⊢
            refine ⟨?_, hD.2⟩
            intro b hb z hz hzb
            rcases List.mem_cons.mp hz with rfl | hz
            · exact hxn (hsub b (List.mem_cons_of_mem _ hb) z hzb)
            · exact hD.1 b hb z hz hzb
          · intro x' t h
            simp at h; obtain ⟨rfl, rfl⟩ := h
            simp only [List.length_cons] at hlen' ⊢
            rw [hxs, desc]
            simp [hxy]
            rw [← hxs]; omega
        · have e : runs (x :: xs) = [x] :: (y :: ys) :: rest0 := by
            rw [runs_cons, hr]; simp [hxy]
          rw [e]
          refine ⟨?_, ?_, ?_, ?_, ?_⟩
          · show [x] ++ ((y :: ys) ++ rest0.flatten) = x :: xs
            rw [hfl']; rfl
          · intro r hr'
            rcases List.mem_cons.mp hr' with rfl | h
            · exact ⟨by simp, by simp⟩
            · rcases List.mem_cons.mp h with rfl | h
              · exact hr0
              · exact hrest r h
          · intro x' t h; simp at h; obtain ⟨rfl, rfl⟩ := h; exact ⟨[], (y :: ys) :: rest0, rfl⟩
          · intro hnd
            have hxn : x ∉ xs := (List.nodup_cons.mp hnd).1
            have hD := hdis (List.nodup_cons.mp hnd).2
            rw [hr] at hD
            unfold Disj at hD ⊢
            rw [List.pairwise_cons]
            refine ⟨?_, hD⟩
            intro b hb z hz hzb
            have : z = x := by simpa using hz
            subst this
            exact hxn (hsub b hb _ hzb)
          · intro x' t h
            simp at h; obtain ⟨rfl, rfl⟩ := h
            simp only [List.length_cons] at hlen' ⊢
            rw [hxs, desc]
            simp [hxy]
            rw [← hxs]; omega


theorem desc_append (a b : List Nat) : desc (a ++ b) ≤ desc a + desc b + 1 := by
  induction a with
  | nil => simp [desc]
  | cons x a ih =>
    cases a with
    | nil =>
      cases b with
      | nil => simp [desc]
      | cons z b' =>
        simp only [List.cons_append, List.nil_append, desc]
        split <;> omega
    | cons y t =>
      have h := ih
      simp only [List.cons_append, desc] at h ⊢
      generalize (if x < y then 0 else 1) = c
      omega

theorem desc_asc : ∀ l : List Nat, l.Pairwise (· < ·) → desc l = 0 := by
  intro l
  induction l with
  | nil => intro _; rfl
  | cons x t ih =>
    intro h
    cases t with
    | nil => rfl
    | cons y t' =>
      have h1 := List.pairwise_cons.mp h
      have hxy : x < y := h1.1 y (by simp)
      have := ih h1.2
      simp [desc, hxy, this]

theorem desc_flat : ∀ other : List (List Nat), (∀ l ∈ other, l.Pairwise (· < ·)) →
    desc other.flatten ≤ other.length - 1 := by
  intro other
  induction other with
  | nil => intro _; simp [desc]
  | cons l t ih =>
    intro h
    have hl : desc l = 0 := desc_asc l (h l (by simp))
    by_cases ht : t = []
    · subst ht; simp [hl]
    · have h1 := desc_append l t.flatten
      have h2 := ih (fun l' hl' => h l' (List.mem_cons_of_mem _ hl'))
      have h3 : 0 < t.length := by cases t <;> simp_all
      simp only [List.flatten_cons, List.length_cons]
      omega

theorem findIdx_key (rs : List (List Nat)) (hd : Disj rs) (r : Nat) (hr : r < rs.length) (i : Nat) :
    rs.findIdx (fun r => r.contains i) = r ↔ i ∈ rs[r] := by
  constructor
  · intro h
    have := (List.findIdx_eq hr).mp h
    simpa [List.contains_iff_mem] using this.1
  · intro h
    apply (List.findIdx_eq hr).mpr
    refine ⟨by simpa [List.contains_iff_mem] using h, ?_⟩
    intro j hj
    have hp := List.pairwise_iff_getElem.mp hd j r (by omega) hr hj
    have hn : ¬ i ∈ rs[j] := fun hm => hp i hm h
    simpa [List.contains_iff_mem] using hn

theorem zip_filter (f : Nat → Nat) (r : Nat) : ∀ (n off : Nat),
    (((List.range' off n).map f).zipIdx off).filterMap
      (fun (p : Nat × Nat) => if p.1 == r then some p.2 else none)
      = (List.range' off n).filter (fun j => f j == r) := by
  intro n
  induction n with
  | zero => intro off; simp
  | succ n ih =>
    intro off
    rw [List.range'_succ, List.map_cons, List.zipIdx_cons, List.filterMap_cons, List.filter_cons, ih]
    by_cases h : f off == r <;> simp [h]

theorem mapgetD (rs : List (List Nat)) :
    (List.range rs.length).map (fun r => rs.getD r []) = rs := by
  apply List.ext_getElem
  · simp
  · intro i h1 h2
    simp at h1
    simp [List.getD_eq_getElem?_getD, h1]



theorem sound : RunSidecar.Goal := by
  intro ids hperm
  dsimp only
  obtain ⟨hfl, hrun, hhead, hdis, hlen⟩ := runs_inv ids
  have hnd : ids.Nodup := hperm.nodup_iff.mpr (List.nodup_range)
  have hD := hdis hnd
  generalize hrs : runs ids = rs at *
  refine ⟨?_, ?_⟩
  · unfold recover labels
    -- tags as a map over range' 0 n
    have hz : ∀ r : Nat, ((List.range ids.length).map (fun i => rs.findIdx (fun r => r.contains i))).zipIdx.filterMap
        (fun (p : Nat × Nat) => if p.1 == r then some p.2 else none)
        = (List.range ids.length).filter (fun j => rs.findIdx (fun r => r.contains j) == r) := by
      intro r
      have := zip_filter (fun i => rs.findIdx (fun r => r.contains i)) r ids.length 0
      simpa [List.range_eq_range'] using this
    have hz' : ∀ r ∈ List.range rs.length,
        ((List.range ids.length).map (fun i => rs.findIdx (fun r => r.contains i))).zipIdx.filterMap
          (fun (p : Nat × Nat) => if p.1 == r then some p.2 else none) = rs.getD r [] := by
      intro r hr
      have hr' : r < rs.length := List.mem_range.mp hr
      rw [hz r]
      have hmem : ∀ i, i ∈ rs[r] → i < ids.length := by
        intro i hi
        have : i ∈ ids := by
          rw [← hfl]; exact List.mem_flatten.mpr ⟨rs[r], List.getElem_mem hr', hi⟩
        have := hperm.mem_iff.mp this
        simpa using this
      have hsorted : (rs[r]).Pairwise (· < ·) := (hrun _ (List.getElem_mem hr')).2
      have hg : rs.getD r [] = rs[r] := by simp [List.getD_eq_getElem?_getD, hr']
      have hpr : (List.range ids.length).Pairwise (· < ·) := List.pairwise_lt_range
      have hpf := hpr.filter (fun j => rs.findIdx (fun r => r.contains j) == r)
      rw [hg]
      symm
      apply List.Perm.eq_of_pairwise (le := (· < ·))
      · intro a _ b _ h1 h2; omega
      · exact hsorted
      · exact hpf
      · rw [List.perm_ext_iff_of_nodup (hsorted.imp Nat.ne_of_lt) (hpf.imp Nat.ne_of_lt)]
        intro i
        rw [List.mem_filter, List.mem_range]
        constructor
        · intro hi
          refine ⟨hmem i hi, ?_⟩
          simp only [beq_iff_eq]
          exact (findIdx_key rs hD r hr' i).mpr hi
        · rintro ⟨_, h⟩
          simp only [beq_iff_eq] at h
          exact (findIdx_key rs hD r hr' i).mp h
    have hm : (List.range rs.length).map (fun r =>
        ((List.range ids.length).map (fun i => rs.findIdx (fun r => r.contains i))).zipIdx.filterMap
          (fun (p : Nat × Nat) => if p.1 == r then some p.2 else none))
        = (List.range rs.length).map (fun r => rs.getD r []) :=
      List.map_congr_left hz'
    show ((List.range rs.length).map (fun r =>
        ((List.range ids.length).map (fun i => rs.findIdx (fun r => r.contains i))).zipIdx.filterMap
          (fun (p : Nat × Nat) => if p.1 == r then some p.2 else none))).flatten = ids
    rw [hm, mapgetD]; exact hfl
  · intro other hother hasc
    have hb := desc_flat other hasc
    rw [hother] at hb
    by_cases hne : ids = []
    · rw [← hrs, hne]; simp [runs]
    · obtain ⟨x, t, hi⟩ := List.exists_cons_of_ne_nil hne
      have hl := hlen x t hi
      have : 0 < other.length := by
        cases other with
        | nil => exact absurd (by simpa using hother.symm) hne
        | cons _ _ => simp
      omega

end TinylambOpt.ResearchProofs.ClickBenchLayout.Goal_CB_RunSidecar

#print axioms TinylambOpt.ResearchProofs.ClickBenchLayout.Goal_CB_RunSidecar.sound
