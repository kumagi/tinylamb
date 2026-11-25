import TinylambOpt.ResearchGoals.Atlas.TextExecution

set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedVariables false
set_option linter.unusedSimpArgs false

namespace TinylambOpt.ResearchProofs.Atlas.Goal_EditFilter_GramLowerBound

open TinylambOpt.ResearchGoals.Atlas.TextExecution
open TinylambOpt.ResearchGoals.Atlas.TextExecution.EditFilter

abbrev Word := List Nat
def grams (q : Nat) (s : Word) : List Word :=
  (List.range (s.length+1-q)).map (fun i => (s.drop i).take q)
def shared (a b : List Word) : Nat :=
  (a.eraseDups.map (fun g => min (a.count g) (b.count g))).sum

private theorem nodupEraseDups : ∀ xs : List Word, xs.eraseDups.Nodup := by
  intro xs
  induction h : xs.length using Nat.strongRecOn generalizing xs with
  | ind n ih =>
      cases xs with
      | nil => simp
      | cons a xs =>
          rw [List.eraseDups_cons]
          refine List.nodup_cons.mpr ⟨?_, ?_⟩
          · simp
          · apply ih ((xs.filter (fun b => ! b == a)).length)
            · have hle := List.length_filter_le (l := xs) (p := fun b => ! b == a)
              have hn : n = xs.length + 1 := by simpa using h.symm
              omega
            · rfl

private theorem sum_map_le_map {α : Type} (l : List α) (f g : α → Nat)
    (h : ∀ x ∈ l, f x ≤ g x) : (l.map f).sum ≤ (l.map g).sum := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      simp only [List.map_cons, List.sum_cons]
      exact Nat.add_le_add (h x (by simp)) (ih (fun y hy => h y (by simp [hy])))

private theorem sum_map_add {α : Type} (l : List α) (f g : α → Nat) :
    (l.map (fun x => f x + g x)).sum = (l.map f).sum + (l.map g).sum := by
  induction l with
  | nil => simp
  | cons x xs ih => simp [ih, Nat.add_assoc, Nat.add_left_comm]

private theorem sum_map_zero {α : Type} (l : List α) (f : α → Nat)
    (h : ∀ x ∈ l, f x = 0) : (l.map f).sum = 0 := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      simp only [List.map_cons, List.sum_cons]
      rw [h x (by simp)]
      simpa using ih (fun y hy => h y (by simp [hy]))

private theorem sum_map_filter_eq {α : Type} (l : List α) (f : α → Nat) (p : α → Bool)
    (hz : ∀ x ∈ l, p x = false → f x = 0) :
    (l.map f).sum = ((l.filter p).map f).sum := by
  induction l with
  | nil => simp
  | cons x xs ih =>
      simp only [List.map_cons, List.sum_cons, List.filter_cons]
      by_cases h : p x = true
      · simp [h]
        rw [ih (by intro y hy hpy; exact hz y (by simp [hy]) hpy)]
      · have hf : p x = false := Bool.eq_false_of_ne_true h
        have hx := hz x (by simp) hf
        simp [h, hx]
        exact ih (by intro y hy hpy; exact hz y (by simp [hy]) hpy)

private theorem shared_le_of_counts (A X Y : List Word) (f : Word → Nat) (q : Nat)
    (hcnt : ∀ g, X.count g ≤ Y.count g + f g)
    (hsum : (A.eraseDups.map f).sum ≤ q) :
    shared A X ≤ shared A Y + q := by
  unfold shared
  calc
    (A.eraseDups.map (fun g => min (A.count g) (X.count g))).sum
        ≤ (A.eraseDups.map (fun g => min (A.count g) (Y.count g) + f g)).sum := by
            apply sum_map_le_map
            intro g hg
            have hg' := hcnt g
            omega
    _ = (A.eraseDups.map (fun g => min (A.count g) (Y.count g))).sum +
          (A.eraseDups.map f).sum := by rw [sum_map_add]
    _ ≤ (A.eraseDups.map (fun g => min (A.count g) (Y.count g))).sum + q := by omega

private theorem sum_indicator_le_one (l : List Word) (hnd : l.Nodup) (g : Word) :
    (l.map (fun h => if g == h then 1 else 0)).sum ≤ 1 := by
  induction l with
  | nil => simp
  | cons a as ih =>
      have hnd' : as.Nodup := (List.nodup_cons.mp hnd).2
      by_cases hga : (g == a) = true
      · have hnot : ∀ x ∈ as, ¬ (g == x) = true := by
          intro x hx hxeq
          have hgx : g = x := by simpa using hxeq
          have hga' : g = a := by simpa using hga
          have hxeqa : x = a := hgx.symm.trans hga'
          exact (List.nodup_cons.mp hnd).1 (by simpa [hxeqa] using hx)
        have hz : (as.map (fun h => if g == h then 1 else 0)).sum = 0 := by
          apply sum_map_zero
          intro x hx
          simp [hnot x hx]
        simp only [List.map_cons, List.sum_cons]
        rw [if_pos hga, hz]
        omega
      · simp only [List.map_cons, List.sum_cons]
        rw [if_neg hga]
        simpa using ih hnd'

private theorem shared_erase_one_le (A X Y : List Word) (g : Word) :
    shared A (X ++ g :: Y) ≤ shared A (X ++ Y) + 1 := by
  apply shared_le_of_counts A (X ++ g :: Y) (X ++ Y) (fun h => if g == h then 1 else 0) 1
  · intro h
    simp [List.count_append, List.count_cons]
    omega
  · exact sum_indicator_le_one A.eraseDups (nodupEraseDups A) g

private theorem shared_mono_sublist (A X Y : List Word) (h : X.Sublist Y) :
    shared A X ≤ shared A Y := by
  apply shared_le_of_counts A X Y (fun _ => 0) 0
  · intro g
    exact h.count_le g
  · rw [sum_map_zero A.eraseDups (fun _ => 0) (by simp)]
    omega

private theorem shared_append_insert_le (A X Y Z : List Word) :
    shared A (X ++ Y ++ Z) ≤ shared A (X ++ Z) + Y.length := by
  induction Y generalizing X with
  | nil => simp
  | cons g gs ih =>
      have hrewrite : X ++ (g :: gs) ++ Z = (X ++ [g]) ++ gs ++ Z := by
        simp [List.append_assoc]
      rw [hrewrite]
      have h2 := ih (X := X ++ [g])
      have h3 : shared A ((X ++ [g]) ++ Z) ≤ shared A (X ++ Z) + 1 := by
        simpa [List.append_assoc] using shared_erase_one_le A X Z g
      have hlen : (g :: gs).length = gs.length + 1 := by simp
      omega

private theorem sum_indicator_eq_one_of_mem (K : List Word) (hnd : K.Nodup) {g : Word}
    (hg : g ∈ K) : (K.map (fun h => if g == h then 1 else 0)).sum = 1 := by
  induction K with
  | nil => cases hg
  | cons a as ih =>
      have hnd' : as.Nodup := (List.nodup_cons.mp hnd).2
      rcases List.mem_cons.mp hg with hga | hgas
      · subst a
        have hnot : ∀ x ∈ as, ¬ (g == x) = true := by
          intro x hx hxeq
          have hgx : g = x := by simpa using hxeq
          exact (List.nodup_cons.mp hnd).1 (by simpa [hgx] using hx)
        have hz : (as.map (fun h => if g == h then 1 else 0)).sum = 0 := by
          apply sum_map_zero
          intro x hx
          simp [hnot x hx]
        simp only [List.map_cons, List.sum_cons]
        rw [if_pos (by simp : (g == g) = true), hz]
      · have hne : ¬ (g == a) = true := by
          intro hga'
          have hg_eq_a : g = a := by simpa using hga'
          exact (List.nodup_cons.mp hnd).1 (by simpa [hg_eq_a] using hgas)
        simp only [List.map_cons, List.sum_cons]
        rw [if_neg hne]
        simpa using ih hnd' hgas

private theorem sum_count_eq_length_of_nodup (K A : List Word) (hnd : K.Nodup)
    (hsub : ∀ x ∈ A, x ∈ K) : (K.map (fun g => A.count g)).sum = A.length := by
  induction A with
  | nil =>
      exact sum_map_zero K (fun _ => 0) (by simp)
  | cons x xs ih =>
      have hsub' : ∀ y ∈ xs, y ∈ K := by
        intro y hy
        exact hsub y (by simp [hy])
      have hi := ih hsub'
      have hxK : x ∈ K := hsub x (by simp)
      have hmap : K.map (fun g => (x :: xs).count g) =
          K.map (fun g => xs.count g + (if x == g then 1 else 0)) := by
        apply List.map_congr_left
        intro g hg
        simp [List.count_cons]
      rw [hmap, sum_map_add, hi, sum_indicator_eq_one_of_mem K hnd hxK]
      simp

private theorem shared_self (A : List Word) : shared A A = A.length := by
  unfold shared
  have h := sum_count_eq_length_of_nodup A.eraseDups A (nodupEraseDups A) (by
    intro x hx
    rw [List.mem_eraseDups]
    exact hx)
  simpa using h
private theorem nodup_decomp {l : List Word} (hnd : l.Nodup) {a : Word} (ha : a ∈ l) :
    ∃ l', List.Perm l (a :: l') ∧ a ∉ l' ∧ l'.Nodup := by
  induction l with
  | nil => cases ha
  | cons b bs ih =>
      by_cases hba : b = a
      · subst b
        exact ⟨bs, List.Perm.refl _, (List.nodup_cons.mp hnd).1, (List.nodup_cons.mp hnd).2⟩
      · have habs : a ∈ bs := by
          rcases List.mem_cons.mp ha with h | h
          · exact False.elim (hba h.symm)
          · exact h
        rcases ih (List.nodup_cons.mp hnd).2 habs with ⟨l', hp, hnot, hnd'⟩
        refine ⟨b :: l', (List.Perm.cons b hp).trans (List.Perm.swap a b l'), ?_, ?_⟩
        · have hba' : a ≠ b := by intro h; exact hba h.symm
          simp [hba', hnot]
        · refine List.nodup_cons.mpr ⟨?_, hnd'⟩
          intro hbin
          exact (List.nodup_cons.mp hnd).1 ((hp.mem_iff).mpr (by simp [hbin]))

private theorem sum_eraseDups_eq_of_mem (f : Word → Nat) (l₁ l₂ : List Word)
    (hnd₁ : l₁.Nodup) (hnd₂ : l₂.Nodup)
    (hmem : ∀ d, d ∈ l₁ ↔ d ∈ l₂) : (l₁.map f).sum = (l₂.map f).sum := by
  induction l₁ generalizing l₂ with
  | nil =>
      have hnil : l₂ = [] := by
        apply List.eq_nil_iff_forall_not_mem.mpr
        intro d hd
        cases ((hmem d).mpr (by simpa using hd))
      subst l₂
      simp
  | cons a l₁ ih =>
      have ha2 : a ∈ l₂ := (hmem a).mp (by simp)
      rcases nodup_decomp hnd₂ ha2 with ⟨l₂', hp, hnot, hnd₂'⟩
      have hmem' : ∀ d, d ∈ l₁ ↔ d ∈ l₂' := by
        intro d
        constructor
        · intro hd
          have hd2 : d ∈ l₂ := (hmem d).mp (by simp [hd])
          have hda : d ∈ a :: l₂' := hp.mem_iff.mp hd2
          rcases List.mem_cons.mp hda with h | h
          · subst h
            exact False.elim ((List.nodup_cons.mp hnd₁).1 hd)
          · exact h
        · intro hd
          have hd2 : d ∈ a :: l₂' := by simp [hd]
          have hd1 : d ∈ a :: l₁ := (hmem d).mpr (hp.mem_iff.mpr hd2)
          rcases List.mem_cons.mp hd1 with h | h
          · subst h
            exact False.elim (hnot hd)
          · exact h
      have ihres := ih l₂' (List.nodup_cons.mp hnd₁).2 hnd₂' hmem'
      have hperm2 : (l₂.map f).sum = ((a :: l₂').map f).sum := List.Perm.sum_nat (hp.map f)
      simp only [List.map_cons, List.sum_cons]
      rw [hperm2]
      simp only [List.map_cons, List.sum_cons]
      rw [ihres]

private theorem sum_eraseDups_eq_of_mem_zero (f : Word → Nat) (l₁ l₂ : List Word)
    (hnd₁ : l₁.Nodup) (hnd₂ : l₂.Nodup)
    (hsub : ∀ d ∈ l₁, d ∈ l₂) (hzero : ∀ d ∈ l₂, d ∉ l₁ → f d = 0) :
    (l₁.map f).sum = (l₂.map f).sum := by
  let l₂' := l₂.filter (fun d => d ∈ l₁)
  have hnd' : l₂'.Nodup := by
    dsimp [l₂']
    exact hnd₂.filter _
  have hmem : ∀ d, d ∈ l₁ ↔ d ∈ l₂' := by
    intro d
    constructor
    · intro hd
      exact List.mem_filter.mpr ⟨hsub d hd, by simp [hd]⟩
    · intro hd
      exact of_decide_eq_true (List.mem_filter.mp hd).2
  have heq1 : (l₁.map f).sum = (l₂'.map f).sum := sum_eraseDups_eq_of_mem f l₁ l₂' hnd₁ hnd' hmem
  have heq2 : (l₂.map f).sum = (l₂'.map f).sum := by
    dsimp [l₂']
    apply sum_map_filter_eq
    intro d hd hfalse
    have hnot : d ∉ l₁ := of_decide_eq_false hfalse
    exact hzero d hd hnot
  omega

private theorem shared_eq_sum_keys (X Y K : List Word) (hnd : K.Nodup)
    (hsubX : ∀ g ∈ X, g ∈ K) (hsubY : ∀ g ∈ Y, g ∈ K) :
    shared X Y = (K.map (fun g => min (X.count g) (Y.count g))).sum := by
  unfold shared
  apply sum_eraseDups_eq_of_mem_zero
  · exact nodupEraseDups _
  · exact hnd
  · intro d hd
    have hdX : d ∈ X := by simpa using hd
    exact hsubX d hdX
  · intro d hdK hdnot
    have hdXnot : d ∉ X := by intro h; exact hdnot (by simpa using h)
    rw [List.count_eq_zero_of_not_mem hdXnot]
    simp

private theorem shared_symm (X Y : List Word) : shared X Y = shared Y X := by
  let K := (X ++ Y).eraseDups
  have hnd : K.Nodup := nodupEraseDups _
  rw [shared_eq_sum_keys X Y K hnd (by intro g hg; rw [List.mem_eraseDups]; simp [K, hg])
      (by intro g hg; rw [List.mem_eraseDups]; simp [K, hg])]
  rw [shared_eq_sum_keys Y X K hnd (by intro g hg; rw [List.mem_eraseDups]; simp [K, hg])
      (by intro g hg; rw [List.mem_eraseDups]; simp [K, hg])]
  congr 1
  apply List.map_congr_left
  intro g hg
  rw [Nat.min_comm]

private theorem shared_mono_both (X X' Y Y' : List Word) (hX : X.Sublist X') (hY : Y.Sublist Y') :
    shared X Y ≤ shared X' Y' := by
  have h1' : shared Y X ≤ shared Y X' := by
    apply shared_le_of_counts Y X X' (fun _ => 0) 0
    · intro g
      exact hX.count_le g
    · rw [sum_map_zero Y.eraseDups (fun _ => 0) (by simp)]
      omega
  have h1 : shared X Y ≤ shared X' Y := by
    rw [shared_symm X Y, shared_symm X' Y]
    exact h1'
  have h2' : shared X' Y ≤ shared X' Y' := by
    apply shared_le_of_counts X' Y Y' (fun _ => 0) 0
    · intro g
      exact hY.count_le g
    · rw [sum_map_zero X'.eraseDups (fun _ => 0) (by simp)]
      omega
  have h2 : shared X' Y ≤ shared X' Y' := h2'
  omega

private theorem shared_cons_cons_ge_add_one (h : Word) (X Y : List Word) :
    shared X Y + 1 ≤ shared (h :: X) (h :: Y) := by
  let K := (X ++ Y ++ [h]).eraseDups
  have hnd : K.Nodup := nodupEraseDups _
  rw [shared_eq_sum_keys X Y K hnd
      (by intro g hg; rw [List.mem_eraseDups]; simp [K, hg])
      (by intro g hg; rw [List.mem_eraseDups]; simp [K, hg])]
  rw [shared_eq_sum_keys (h::X) (h::Y) K hnd
      (by intro g hg; rw [List.mem_eraseDups];
          rcases List.mem_cons.mp hg with hgh | hgX
          · subst g; simp
          · simp [hgX])
      (by intro g hg; rw [List.mem_eraseDups];
          rcases List.mem_cons.mp hg with hgh | hgX
          · subst g; simp
          · simp [hgX])]
  have hmemh : h ∈ K := by simp [K, List.mem_eraseDups]
  have hsum_ge : (K.map (fun g => min ((h::X).count g) ((h::Y).count g))).sum ≥
      (K.map (fun g => min (X.count g) (Y.count g))).sum + 1 := by
    -- prove by induction on K with nodup and hmemh? Use sum_map_le_map style
    have hpoint : ∀ g ∈ K, min (X.count g) (Y.count g) + (if h == g then 1 else 0) ≤
        min ((h::X).count g) ((h::Y).count g) := by
      intro g hg
      simp [List.count_cons]
    have hstep : (K.map (fun g => min (X.count g) (Y.count g) + (if h == g then 1 else 0))).sum ≤
        (K.map (fun g => min ((h::X).count g) ((h::Y).count g))).sum := by
      apply sum_map_le_map
      intro g hg
      exact hpoint g hg
    rw [sum_map_add] at hstep
    have hind := sum_indicator_eq_one_of_mem K hnd hmemh
    omega
  exact hsum_ge

private theorem shared_le_length_left (X Y : List Word) : shared X Y ≤ X.length := by
  unfold shared
  have h := sum_count_eq_length_of_nodup X.eraseDups X (nodupEraseDups X) (by
    intro g hg
    rw [List.mem_eraseDups]
    exact hg)
  rw [← h]
  apply sum_map_le_map
  intro g hg
  exact Nat.min_le_left _ _

private def deficit (X Y : List Word) : Nat := max X.length Y.length - shared X Y

private theorem deficit_cons_same (h : Word) (X Y : List Word) :
    deficit (h::X) (h::Y) ≤ deficit X Y := by
  unfold deficit
  simp only [List.length_cons]
  have hsh := shared_cons_cons_ge_add_one h X Y
  have hmax : max (X.length + 1) (Y.length + 1) = max X.length Y.length + 1 := by omega
  rw [hmax]
  omega

private theorem deficit_cons_diff (h1 h2 : Word) (X Y : List Word) :
    deficit (h1::X) (h2::Y) ≤ deficit X Y + 1 := by
  unfold deficit
  simp only [List.length_cons]
  have hmono := shared_mono_both X (h1::X) Y (h2::Y) (List.Sublist.cons h1 (List.Sublist.refl X)) (List.Sublist.cons h2 (List.Sublist.refl Y))
  have hmax : max (X.length + 1) (Y.length + 1) = max X.length Y.length + 1 := by omega
  rw [hmax]
  omega

private theorem deficit_cons_left (h : Word) (X Y : List Word) :
    deficit (h::X) Y ≤ deficit X Y + 1 := by
  unfold deficit
  simp only [List.length_cons]
  have hmono := shared_mono_both X (h::X) Y Y (List.Sublist.cons h (List.Sublist.refl X)) (List.Sublist.refl Y)
  by_cases hle : X.length + 1 ≤ Y.length
  · have hmax1 : max (X.length + 1) Y.length = Y.length := by omega
    have hmax2 : max X.length Y.length = Y.length := by omega
    rw [hmax1, hmax2]
    omega
  · have hmax1 : max (X.length + 1) Y.length = X.length + 1 := by omega
    rw [hmax1]
    omega

private theorem deficit_cons_right (h : Word) (X Y : List Word) :
    deficit X (h::Y) ≤ deficit X Y + 1 := by
  unfold deficit
  rw [shared_symm X (h :: Y), shared_symm X Y]
  simpa [deficit, Nat.max_comm] using deficit_cons_left h Y X

private theorem deficit_triangle (A B C : List Word) :
    deficit A C ≤ deficit A B + deficit B C := by
  let K := (A ++ B ++ C).eraseDups
  have hnd : K.Nodup := nodupEraseDups _
  have hAB := shared_eq_sum_keys A B K hnd (by intro g hg; simp [K, List.mem_eraseDups, hg]) (by intro g hg; simp [K, List.mem_eraseDups, hg])
  have hBC := shared_eq_sum_keys B C K hnd (by intro g hg; simp [K, List.mem_eraseDups, hg]) (by intro g hg; simp [K, List.mem_eraseDups, hg])
  have hAC := shared_eq_sum_keys A C K hnd (by intro g hg; simp [K, List.mem_eraseDups, hg]) (by intro g hg; simp [K, List.mem_eraseDups, hg])
  have hshared : shared A B + shared B C ≤ shared A C + B.length := by
    rw [hAB, hBC, hAC]
    have hsumlen : (K.map (fun g => B.count g)).sum = B.length :=
      sum_count_eq_length_of_nodup K B hnd (by intro g hg; simp [K, List.mem_eraseDups, hg])
    rw [← hsumlen]
    calc
      (K.map (fun g => min (A.count g) (B.count g))).sum +
          (K.map (fun g => min (B.count g) (C.count g))).sum =
          (K.map (fun g => min (A.count g) (B.count g) + min (B.count g) (C.count g))).sum := by
              rw [sum_map_add]
      _ ≤ (K.map (fun g => min (A.count g) (C.count g) + B.count g)).sum := by
              apply sum_map_le_map
              intro g hg
              omega
      _ = (K.map (fun g => min (A.count g) (C.count g))).sum +
            (K.map (fun g => B.count g)).sum := by rw [sum_map_add]
  have hmax : max A.length C.length ≤ max A.length B.length + max B.length C.length - B.length := by
    have ha : A.length ≤ max A.length B.length := Nat.le_max_left _ _
    have hc : C.length ≤ max B.length C.length := Nat.le_max_right _ _
    have hb1 : B.length ≤ max A.length B.length := Nat.le_max_right _ _
    have hb2 : B.length ≤ max B.length C.length := Nat.le_max_left _ _
    apply Nat.max_le.mpr
    constructor <;> omega
  unfold deficit
  have hsa := shared_le_length_left A B
  have hsb := shared_le_length_left B C
  have hsc := shared_le_length_left A C
  by_cases hAB : A.length ≤ B.length
  · by_cases hBC : B.length ≤ C.length
    · have hAC : A.length ≤ C.length := Nat.le_trans hAB hBC
      simp [Nat.max_eq_right hAB, Nat.max_eq_right hBC, Nat.max_eq_right hAC] at *
      omega
    · have hCB : C.length ≤ B.length := Nat.le_of_not_ge hBC
      by_cases hAC : A.length ≤ C.length
      · simp [Nat.max_eq_right hAB, Nat.max_eq_left hCB, Nat.max_eq_right hAC] at *
        omega
      · have hCA : C.length ≤ A.length := Nat.le_of_not_ge hAC
        simp [Nat.max_eq_right hAB, Nat.max_eq_left hCB, Nat.max_eq_left hCA] at *
        omega
  · have hBA : B.length ≤ A.length := Nat.le_of_not_ge hAB
    by_cases hBC : B.length ≤ C.length
    · by_cases hAC : A.length ≤ C.length
      · simp [Nat.max_eq_left hBA, Nat.max_eq_right hBC, Nat.max_eq_right hAC] at *
        omega
      · have hCA : C.length ≤ A.length := Nat.le_of_not_ge hAC
        simp [Nat.max_eq_left hBA, Nat.max_eq_right hBC, Nat.max_eq_left hCA] at *
        omega
    · have hCB : C.length ≤ B.length := Nat.le_of_not_ge hBC
      have hCA : C.length ≤ A.length := Nat.le_trans hCB hBA
      simp [Nat.max_eq_left hBA, Nat.max_eq_left hCB, Nat.max_eq_left hCA] at *
      omega

private theorem range_succ_cons (n : Nat) : List.range (n+1) = 0 :: (List.range n).map (fun i => i+1) := by
  rw [List.range_eq_range', List.range'_succ]
  simp only [Nat.zero_add]
  rw [List.range'_eq_map_range]
  congr 1
  apply List.map_congr_left
  intro i hi
  omega

private theorem grams_nil (q : Nat) (hq : 0 < q) : grams q [] = [] := by
  simp [grams]
  omega

private theorem grams_cons_of_le (q : Nat) (a : Nat) (s : Word)
    (h : q ≤ s.length + 1) :
    grams q (a::s) = (a::s).take q :: grams q s := by
  unfold grams
  simp only [List.length_cons]
  have hs : s.length + 1 + 1 - q = (s.length + 1 - q) + 1 := by omega
  rw [hs, range_succ_cons, List.map_cons]
  congr 1
  simp only [List.map_map]
  apply List.map_congr_left
  intro i hi
  simp

private theorem grams_cons_of_gt (q : Nat) (hq : 0 < q) (a : Nat) (s : Word)
    (h : s.length + 1 < q) : grams q (a::s) = [] := by
  unfold grams
  simp only [List.length_cons]
  have : s.length + 1 + 1 - q = 0 := by omega
  rw [this]
  simp

private theorem deficit_symm (X Y : List Word) : deficit X Y = deficit Y X := by
  unfold deficit
  rw [Nat.max_comm, shared_symm]

private theorem shared_cons_self_eq (h : Word) (X : List Word) : shared (h::X) X = X.length := by
  let K := (h :: X).eraseDups
  have hnd : K.Nodup := nodupEraseDups _
  rw [shared_eq_sum_keys (h::X) X K hnd
      (by intro g hg; rw [List.mem_eraseDups]; simp [hg])
      (by intro g hg; rw [List.mem_eraseDups]; simp [hg])]
  have hlen := sum_count_eq_length_of_nodup K X hnd (by
    intro g hg
    rw [List.mem_eraseDups]
    simp [hg])
  rw [← hlen]
  apply congrArg List.sum
  apply List.map_congr_left
  intro g hg
  simp [List.count_cons]

private theorem deficit_cons_self_one (h : Word) (X : List Word) : deficit (h::X) X ≤ 1 := by
  unfold deficit
  have hsh := shared_cons_self_eq h X
  rw [hsh]
  simp [List.length_cons, Nat.max_comm]

private theorem deficit_le_one_of_length_le_one {X Y : List Word}
    (hX : X.length ≤ 1) (hY : Y.length ≤ 1) : deficit X Y ≤ 1 := by
  unfold deficit
  have hm : max X.length Y.length ≤ 1 := by omega
  omega

private theorem deficit_grams_insert_len_le (q : Nat) (hq : 0 < q) (l : Word) (x : Nat) (r : Word) :
    deficit (grams q (l ++ x :: r)) (grams q (l ++ r)) ≤ l.length + 1 := by
  induction l with
  | nil =>
      simp only [List.length_nil, Nat.zero_add, List.nil_append]
      by_cases hle : q ≤ r.length + 1
      · rw [grams_cons_of_le q x r hle]
        exact Nat.le_trans (deficit_cons_self_one (List.take q (x :: r)) (grams q r)) (by omega)
      · rw [grams_cons_of_gt q hq x r (by omega)]
        have hgr : grams q r = [] := by
          unfold grams
          simp
          omega
        rw [hgr]
        simp [deficit]
  | cons a l ih =>
      simp only [List.length_cons, List.cons_append] at *
      have hlenb : (a :: (l ++ x :: r)).length = (l ++ x :: r).length + 1 := by simp
      have hlenc : (a :: (l ++ r)).length = (l ++ r).length + 1 := by simp
      have htailrel : (l ++ x :: r).length = (l ++ r).length + 1 := by simp; omega
      have hlen_lr : (l ++ r).length = l.length + r.length := by simp
      have hlen_lxr : (l ++ x :: r).length = l.length + r.length + 1 := by simp; omega
      by_cases hbig : q > (l ++ x :: r).length + 1
      · 
        rw [grams_cons_of_gt q hq a (l ++ x :: r) hbig]
        rw [grams_cons_of_gt q hq a (l ++ r) (by rw [← htailrel]; exact Nat.lt_trans (Nat.lt_succ_self _) hbig)]
        simp [deficit]
      · rw [grams_cons_of_le q a (l ++ x :: r) (by omega)]
        by_cases hbigc : q > (l ++ r).length + 1
        · have hsmall : grams q (l ++ x :: r) = [] := by
            unfold grams
            simp
            omega
          rw [hsmall]
          rw [grams_cons_of_gt q hq a (l ++ r) (by omega)]
          have hd : deficit [List.take q (a :: (l ++ x :: r))] [] ≤ 1 := by
            unfold deficit
            simp
          omega
        · rw [grams_cons_of_le q a (l ++ r) (by omega)]
          by_cases hhead : q ≤ l.length + 1
          · have hheq : List.take q (a :: (l ++ x :: r)) = List.take q (a :: (l ++ r)) := by
              change List.take q ((a :: l) ++ x :: r) = List.take q ((a :: l) ++ r)
              rw [List.take_append_of_le_length (by omega : q ≤ (a :: l).length),
                  List.take_append_of_le_length (by omega : q ≤ (a :: l).length)]
            rw [hheq]
            have hs := deficit_cons_same (List.take q (a :: (l ++ r)))
              (grams q (l ++ x :: r)) (grams q (l ++ r))
            omega
          · have hs := deficit_cons_diff (List.take q (a :: (l ++ x :: r)))
                  (List.take q (a :: (l ++ r)))
                  (grams q (l ++ x :: r)) (grams q (l ++ r))
            omega

private theorem deficit_grams_insert_q_le (q : Nat) (hq : 0 < q) (l : Word) (x : Nat) (r : Word) :
    deficit (grams q (l ++ x :: r)) (grams q (l ++ r)) ≤ q := by
  induction l with
  | nil =>
      simp only [List.length_nil, Nat.zero_add, List.nil_append]
      by_cases hle : q ≤ r.length + 1
      · rw [grams_cons_of_le q x r hle]
        exact Nat.le_trans (deficit_cons_self_one (List.take q (x :: r)) (grams q r)) hq
      · rw [grams_cons_of_gt q hq x r (by omega)]
        have hgr : grams q r = [] := by
          unfold grams
          simp
          omega
        rw [hgr]
        simp [deficit]
  | cons a l ih =>
      simp only [List.length_cons, List.cons_append] at *
      have hlenb : (a :: (l ++ x :: r)).length = (l ++ x :: r).length + 1 := by simp
      have hlenc : (a :: (l ++ r)).length = (l ++ r).length + 1 := by simp
      have htailrel : (l ++ x :: r).length = (l ++ r).length + 1 := by simp; omega
      have hlen_lr : (l ++ r).length = l.length + r.length := by simp
      have hlen_lxr : (l ++ x :: r).length = l.length + r.length + 1 := by simp; omega
      by_cases hbig : q > (l ++ x :: r).length + 1
      · 
        rw [grams_cons_of_gt q hq a (l ++ x :: r) hbig]
        rw [grams_cons_of_gt q hq a (l ++ r) (by rw [← htailrel]; exact Nat.lt_trans (Nat.lt_succ_self _) hbig)]
        simp [deficit]
      · rw [grams_cons_of_le q a (l ++ x :: r) (by omega)]
        by_cases hbigc : q > (l ++ r).length + 1
        · have hsmall : grams q (l ++ x :: r) = [] := by
            unfold grams
            simp
            omega
          rw [hsmall]
          rw [grams_cons_of_gt q hq a (l ++ r) (by omega)]
          have hd : deficit [List.take q (a :: (l ++ x :: r))] [] ≤ 1 := by
            unfold deficit
            simp
          omega
        · rw [grams_cons_of_le q a (l ++ r) (by omega)]
          by_cases hhead : q ≤ l.length + 1
          · have hheq : List.take q (a :: (l ++ x :: r)) = List.take q (a :: (l ++ r)) := by
              change List.take q ((a :: l) ++ x :: r) = List.take q ((a :: l) ++ r)
              rw [List.take_append_of_le_length (by omega : q ≤ (a :: l).length),
                  List.take_append_of_le_length (by omega : q ≤ (a :: l).length)]
            rw [hheq]
            have hs := deficit_cons_same (List.take q (a :: (l ++ r)))
              (grams q (l ++ x :: r)) (grams q (l ++ r))
            omega
          · have hs := deficit_cons_diff (List.take q (a :: (l ++ x :: r)))
                  (List.take q (a :: (l ++ r)))
                  (grams q (l ++ x :: r)) (grams q (l ++ r))
            have hlen := deficit_grams_insert_len_le q hq l x r
            omega

private theorem deficit_grams_insert_le (q : Nat) (hq : 0 < q) (l : Word) (x : Nat) (r : Word) :
    deficit (grams q (l ++ x :: r)) (grams q (l ++ r)) ≤ min q (l.length + 1) := by
  exact Nat.le_min.mpr ⟨deficit_grams_insert_q_le q hq l x r,
    deficit_grams_insert_len_le q hq l x r⟩

private theorem deficit_grams_subst_len_le (q : Nat) (hq : 0 < q) (l : Word) (x y : Nat) (r : Word) :
    deficit (grams q (l ++ x :: r)) (grams q (l ++ y :: r)) ≤ l.length + 1 := by
  induction l with
  | nil =>
      simp only [List.length_nil, Nat.zero_add, List.nil_append]
      by_cases hle : q ≤ r.length + 1
      · rw [grams_cons_of_le q x r hle, grams_cons_of_le q y r hle]
        have hs := deficit_cons_diff (List.take q (x :: r)) (List.take q (y :: r))
          (grams q r) (grams q r)
        have hself : deficit (grams q r) (grams q r) = 0 := by
          unfold deficit
          rw [shared_self]
          omega
        rw [hself] at hs
        omega
      · rw [grams_cons_of_gt q hq x r (by omega), grams_cons_of_gt q hq y r (by omega)]
        simp [deficit]
  | cons a l ih =>
      simp only [List.length_cons, List.cons_append] at *
      have hlenb : (a :: (l ++ x :: r)).length = (l ++ x :: r).length + 1 := by simp
      have hlenc : (a :: (l ++ r)).length = (l ++ r).length + 1 := by simp
      have hleny : (a :: (l ++ y :: r)).length = (l ++ y :: r).length + 1 := by simp
      have hlenyxr : (l ++ y :: r).length = (l ++ x :: r).length := by simp
      have htailrel : (l ++ x :: r).length = (l ++ r).length + 1 := by simp; omega
      have hlen_lr : (l ++ r).length = l.length + r.length := by simp
      have hlen_lxr : (l ++ x :: r).length = l.length + r.length + 1 := by simp; omega
      by_cases hbig : q > (l ++ x :: r).length + 1
      · rw [grams_cons_of_gt q hq a (l ++ x :: r) hbig]
        rw [grams_cons_of_gt q hq a (l ++ y :: r) (by rw [hlenyxr]; exact hbig)]
        simp [deficit]
      · rw [grams_cons_of_le q a (l ++ x :: r) (by omega)]
        by_cases hbigc : q > (l ++ y :: r).length + 1
        · 
          have hsmall : grams q (l ++ x :: r) = [] := by
            unfold grams
            simp
            omega
          rw [hsmall]
          rw [grams_cons_of_gt q hq a (l ++ y :: r) (by omega)]
          have hd : deficit [List.take q (a :: (l ++ x :: r))] [] ≤ 1 := by
            unfold deficit
            simp
          omega
        · rw [grams_cons_of_le q a (l ++ y :: r) (by omega)]
          by_cases hhead : q ≤ l.length + 1
          · have hxhead : List.take q (a :: (l ++ x :: r)) = List.take q (a :: l) := by
              change List.take q ((a :: l) ++ x :: r) = List.take q (a :: l)
              rw [List.take_append_of_le_length (by omega : q ≤ (a :: l).length)]
            have hyhead : List.take q (a :: (l ++ y :: r)) = List.take q (a :: l) := by
              change List.take q ((a :: l) ++ y :: r) = List.take q (a :: l)
              rw [List.take_append_of_le_length (by omega : q ≤ (a :: l).length)]
            rw [hxhead, hyhead]
            have hs := deficit_cons_same (List.take q (a :: l))
              (grams q (l ++ x :: r)) (grams q (l ++ y :: r))
            omega
          · have hs := deficit_cons_diff (List.take q (a :: (l ++ x :: r)))
                  (List.take q (a :: (l ++ y :: r)))
                  (grams q (l ++ x :: r)) (grams q (l ++ y :: r))
            omega

private theorem deficit_grams_subst_q_le (q : Nat) (hq : 0 < q) (l : Word) (x y : Nat) (r : Word) :
    deficit (grams q (l ++ x :: r)) (grams q (l ++ y :: r)) ≤ q := by
  induction l with
  | nil =>
      simp only [List.length_nil, Nat.zero_add, List.nil_append]
      by_cases hle : q ≤ r.length + 1
      · rw [grams_cons_of_le q x r hle, grams_cons_of_le q y r hle]
        have hs := deficit_cons_diff (List.take q (x :: r)) (List.take q (y :: r))
          (grams q r) (grams q r)
        have hself : deficit (grams q r) (grams q r) = 0 := by
          unfold deficit
          rw [shared_self]
          omega
        rw [hself] at hs
        omega
      · rw [grams_cons_of_gt q hq x r (by omega), grams_cons_of_gt q hq y r (by omega)]
        simp [deficit]
  | cons a l ih =>
      simp only [List.length_cons, List.cons_append] at *
      have hlenb : (a :: (l ++ x :: r)).length = (l ++ x :: r).length + 1 := by simp
      have hlenc : (a :: (l ++ r)).length = (l ++ r).length + 1 := by simp
      have hleny : (a :: (l ++ y :: r)).length = (l ++ y :: r).length + 1 := by simp
      have hlenyxr : (l ++ y :: r).length = (l ++ x :: r).length := by simp
      have htailrel : (l ++ x :: r).length = (l ++ r).length + 1 := by simp; omega
      have hlen_lr : (l ++ r).length = l.length + r.length := by simp
      have hlen_lxr : (l ++ x :: r).length = l.length + r.length + 1 := by simp; omega
      by_cases hbig : q > (l ++ x :: r).length + 1
      · rw [grams_cons_of_gt q hq a (l ++ x :: r) hbig]
        rw [grams_cons_of_gt q hq a (l ++ y :: r) (by rw [hlenyxr]; exact hbig)]
        simp [deficit]
      · rw [grams_cons_of_le q a (l ++ x :: r) (by omega)]
        by_cases hbigc : q > (l ++ y :: r).length + 1
        · 
          have hsmall : grams q (l ++ x :: r) = [] := by
            unfold grams
            simp
            omega
          rw [hsmall]
          rw [grams_cons_of_gt q hq a (l ++ y :: r) (by omega)]
          have hd : deficit [List.take q (a :: (l ++ x :: r))] [] ≤ 1 := by
            unfold deficit
            simp
          omega
        · rw [grams_cons_of_le q a (l ++ y :: r) (by omega)]
          by_cases hhead : q ≤ l.length + 1
          · have hxhead : List.take q (a :: (l ++ x :: r)) = List.take q (a :: l) := by
              change List.take q ((a :: l) ++ x :: r) = List.take q (a :: l)
              rw [List.take_append_of_le_length (by omega : q ≤ (a :: l).length)]
            have hyhead : List.take q (a :: (l ++ y :: r)) = List.take q (a :: l) := by
              change List.take q ((a :: l) ++ y :: r) = List.take q (a :: l)
              rw [List.take_append_of_le_length (by omega : q ≤ (a :: l).length)]
            rw [hxhead, hyhead]
            have hs := deficit_cons_same (List.take q (a :: l))
              (grams q (l ++ x :: r)) (grams q (l ++ y :: r))
            omega
          · have hs := deficit_cons_diff (List.take q (a :: (l ++ x :: r)))
                  (List.take q (a :: (l ++ y :: r)))
                  (grams q (l ++ x :: r)) (grams q (l ++ y :: r))
            have hlen := deficit_grams_subst_len_le q hq l x y r
            omega

private theorem deficit_grams_subst_le (q : Nat) (hq : 0 < q) (l : Word) (x y : Nat) (r : Word) :
    deficit (grams q (l ++ x :: r)) (grams q (l ++ y :: r)) ≤ min q (l.length + 1) := by
  exact Nat.le_min.mpr ⟨deficit_grams_subst_q_le q hq l x y r,
    deficit_grams_subst_len_le q hq l x y r⟩

private theorem main_goal : ∀ s t k q, 0 < q → Edits k s t →
    max (grams q s).length (grams q t).length ≤
      shared (grams q s) (grams q t) + k*q := by
  intro s t k q hq hed
  induction hed with
  | refl s =>
      have hself := shared_self (grams q s)
      rw [hself]
      simp
  | drop l r x h ih =>
      rename_i k0 a
      have hstep := deficit_grams_insert_le q hq l x r
      have hmin : min q (l.length+1) ≤ q := Nat.min_le_left _ _
      have hstepq : deficit (grams q (l ++ x :: r)) (grams q (l ++ r)) ≤ q := Nat.le_trans hstep hmin
      have htri := deficit_triangle (grams q a) (grams q (l ++ x :: r)) (grams q (l ++ r))
      have ihdef : deficit (grams q a) (grams q (l ++ x :: r)) ≤ k0*q := by
        unfold deficit
        omega
      have hmul : (k0+1)*q = k0*q + q := by simp [Nat.add_mul]
      have hres : deficit (grams q a) (grams q (l ++ r)) ≤ (k0+1)*q := by rw [hmul]; omega
      unfold deficit at hres
      omega
  | add l r x h ih =>
      rename_i k0 a
      have hstep := deficit_grams_insert_le q hq l x r
      have hmin : min q (l.length+1) ≤ q := Nat.min_le_left _ _
      have hstepq : deficit (grams q (l ++ x :: r)) (grams q (l ++ r)) ≤ q := Nat.le_trans hstep hmin
      have hstep' : deficit (grams q (l ++ r)) (grams q (l ++ x :: r)) ≤ q := by
        rw [deficit_symm]
        exact hstepq
      have htri := deficit_triangle (grams q a) (grams q (l ++ r)) (grams q (l ++ x :: r))
      have ihdef : deficit (grams q a) (grams q (l ++ r)) ≤ k0*q := by
        unfold deficit
        omega
      have hmul : (k0+1)*q = k0*q + q := by simp [Nat.add_mul]
      have hres : deficit (grams q a) (grams q (l ++ x :: r)) ≤ (k0+1)*q := by rw [hmul]; omega
      unfold deficit at hres
      omega
  | subst l r x y h ih =>
      rename_i k0 a
      have hstep := deficit_grams_subst_le q hq l x y r
      have hmin : min q (l.length+1) ≤ q := Nat.min_le_left _ _
      have hstepq : deficit (grams q (l ++ x :: r)) (grams q (l ++ y :: r)) ≤ q := Nat.le_trans hstep hmin
      have htri := deficit_triangle (grams q a) (grams q (l ++ x :: r)) (grams q (l ++ y :: r))
      have ihdef : deficit (grams q a) (grams q (l ++ x :: r)) ≤ k0*q := by
        unfold deficit
        omega
      have hmul : (k0+1)*q = k0*q + q := by simp [Nat.add_mul]
      have hres : deficit (grams q a) (grams q (l ++ y :: r)) ≤ (k0+1)*q := by rw [hmul]; omega
      unfold deficit at hres
      omega

theorem sound_EditFilter_GramLowerBound : GramLowerBound.Goal := by
  exact main_goal

end TinylambOpt.ResearchProofs.Atlas.Goal_EditFilter_GramLowerBound
