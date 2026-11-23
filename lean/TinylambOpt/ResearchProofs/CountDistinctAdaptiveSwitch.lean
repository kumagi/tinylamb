import TinylambOpt.ResearchGoals.CountDistinctAdaptiveSwitch

namespace TinylambOpt.ResearchProofs.CountDistinctAdaptiveSwitch

open TinylambOpt.ResearchGoals.CountDistinctAdaptiveSwitch

private theorem mem_contains (l : List Int) (a : Int) : a ∈ l ↔ l.contains a = true := by
  rw [List.contains_eq_mem, decide_eq_true_eq]

private theorem nodup_two (v u : Int) (hne : v ≠ u) : [v, u].Nodup := by
  rw [List.nodup_cons, List.mem_singleton]
  refine ⟨hne, ?_⟩
  rw [List.nodup_cons]
  exact ⟨List.not_mem_nil, List.nodup_nil⟩

private theorem insertNonNull_inv (thresh : Nat) :
    ∀ (s : State), StateInvariant thresh s → ∀ v, StateInvariant thresh (insertNonNull thresh s v) := by
  intro s h v
  cases s with
  | empty => trivial
  | single u =>
      by_cases huv : u = v
      · subst huv
        simp [insertNonNull, StateInvariant]
      · have hne : v ≠ u := fun hh => huv hh.symm
        have hb' : ¬ ((u == v) = true) := by simpa using huv
        have hnd : [v, u].Nodup := nodup_two v u hne
        have hstep : insertNonNull thresh (State.single u) v =
            if u == v then State.single u
            else if 2 ≤ thresh then State.small [v, u] else State.large [v, u] := rfl
        rw [hstep, if_neg hb']
        by_cases h2 : (2 : Nat) ≤ thresh
        · rw [if_pos h2]
          exact ⟨h2, hnd⟩
        · rw [if_neg h2]
          exact hnd
  | small xs =>
      obtain ⟨hlen, hnd⟩ := h
      by_cases hc : xs.contains v = true
      · have hstep : insertNonNull thresh (State.small xs) v =
              if xs.contains v then State.small xs
              else if (v :: xs).length ≤ thresh then State.small (v :: xs)
              else State.large (v :: xs) := rfl
        rw [hstep, if_pos hc]
        exact ⟨hlen, hnd⟩
      · have hnot : v ∉ xs := fun hh => hc ((mem_contains xs v).mp hh)
        have hnd2 : (v :: xs).Nodup := by
          rw [List.nodup_cons]
          exact ⟨hnot, hnd⟩
        have hstep : insertNonNull thresh (State.small xs) v =
                if xs.contains v then State.small xs
                else if (v :: xs).length ≤ thresh then State.small (v :: xs)
                else State.large (v :: xs) := rfl
        rw [hstep, if_neg hc]
        by_cases hl : (v :: xs).length ≤ thresh
        · rw [if_pos hl]
          exact ⟨hl, hnd2⟩
        · rw [if_neg hl]
          exact hnd2
  | large xs =>
      have hnd : xs.Nodup := h
      by_cases hc : xs.contains v = true
      · have hstep : insertNonNull thresh (State.large xs) v =
              if xs.contains v then State.large xs else State.large (v :: xs) := rfl
        rw [hstep, if_pos hc]
        exact hnd
      · have hnot : v ∉ xs := fun hh => hc ((mem_contains xs v).mp hh)
        have hstep : insertNonNull thresh (State.large xs) v =
                if xs.contains v then State.large xs else State.large (v :: xs) := rfl
        rw [hstep, if_neg hc]
        show (v :: xs).Nodup
        rw [List.nodup_cons]
        exact ⟨hnot, hnd⟩

private theorem insertNonNull_mem (thresh : Nat) :
    ∀ (s : State) (v w : Int),
      w ∈ entriesOf (insertNonNull thresh s v) ↔ w ∈ entriesOf s ∨ w = v := by
  intro s v w
  cases s with
  | empty => simp [insertNonNull, entriesOf]
  | single u =>
      by_cases huv : u = v
      · subst huv
        simp [insertNonNull, entriesOf]
      · have hb' : ¬ ((u == v) = true) := by simpa using huv
        have hstep : insertNonNull thresh (State.single u) v =
            if u == v then State.single u
            else if 2 ≤ thresh then State.small [v, u] else State.large [v, u] := rfl
        rw [hstep, if_neg hb']
        by_cases h2 : (2 : Nat) ≤ thresh
        · rw [if_pos h2]
          simp only [entriesOf, List.mem_cons, List.mem_singleton]
          exact or_comm
        · rw [if_neg h2]
          simp only [entriesOf, List.mem_cons, List.mem_singleton]
          exact or_comm
  | small xs =>
      by_cases hc : xs.contains v = true
      · have hmem : v ∈ xs := (mem_contains xs v).mpr hc
        have hstep : insertNonNull thresh (State.small xs) v =
                if xs.contains v then State.small xs
                else if (v :: xs).length ≤ thresh then State.small (v :: xs)
                else State.large (v :: xs) := rfl
        rw [hstep, if_pos hc]
        simp only [entriesOf]
        exact ⟨Or.inl, fun hh => hh.elim id fun he => he ▸ hmem⟩
      · have hstep : insertNonNull thresh (State.small xs) v =
              if xs.contains v then State.small xs
              else if (v :: xs).length ≤ thresh then State.small (v :: xs)
              else State.large (v :: xs) := rfl
        rw [hstep, if_neg hc]
        by_cases hl : (v :: xs).length ≤ thresh
        · rw [if_pos hl]
          simp only [entriesOf, List.mem_cons]
          exact or_comm
        · rw [if_neg hl]
          simp only [entriesOf, List.mem_cons]
          exact or_comm
  | large xs =>
      by_cases hc : xs.contains v = true
      · have hmem : v ∈ xs := (mem_contains xs v).mpr hc
        have hstep : insertNonNull thresh (State.large xs) v =
                if xs.contains v then State.large xs else State.large (v :: xs) := rfl
        rw [hstep, if_pos hc]
        simp only [entriesOf]
        exact ⟨Or.inl, fun hh => hh.elim id fun he => he ▸ hmem⟩
      · have hstep : insertNonNull thresh (State.large xs) v =
              if xs.contains v then State.large xs else State.large (v :: xs) := rfl
        rw [hstep, if_neg hc]
        simp only [entriesOf, List.mem_cons]
        exact or_comm

private theorem fold_inv (thresh : Nat) :
    ∀ (xs : List Int) (s : State),
      StateInvariant thresh s → StateInvariant thresh (xs.foldl (insertNonNull thresh) s) := by
  intro xs
  induction xs with
  | nil => intro s h; exact h
  | cons x xs ih =>
      intro s h
      rw [List.foldl_cons]
      exact ih _ (insertNonNull_inv thresh s h x)

private theorem fold_mem (thresh : Nat) :
    ∀ (xs : List Int) (s : State) (w : Int),
      w ∈ entriesOf (xs.foldl (insertNonNull thresh) s) ↔ w ∈ entriesOf s ∨ w ∈ xs := by
  intro xs
  induction xs with
  | nil => intro s w; simp
  | cons x xs ih =>
      intro s w
      rw [List.foldl_cons, ih (insertNonNull thresh s x) w,
        insertNonNull_mem thresh s x w, List.mem_cons]
      rw [or_assoc]

private theorem fold_opt_inv (thresh : Nat) :
    ∀ (c : List (Option Int)) (s : State),
      StateInvariant thresh s → StateInvariant thresh (c.foldl (insertOpt thresh) s) := by
  intro c
  induction c with
  | nil => intro s h; exact h
  | cons o c ih =>
      intro s h
      rw [List.foldl_cons]
      cases o with
      | none =>
          rw [show insertOpt thresh s none = s from rfl]
          exact ih s h
      | some v =>
          rw [show insertOpt thresh s (some v) = insertNonNull thresh s v from rfl]
          exact ih _ (insertNonNull_inv thresh s h v)

private theorem fold_opt_mem (thresh : Nat) :
    ∀ (c : List (Option Int)) (s : State) (w : Int),
      w ∈ entriesOf (c.foldl (insertOpt thresh) s) ↔ w ∈ entriesOf s ∨ some w ∈ c := by
  intro c
  induction c with
  | nil => intro s w; simp
  | cons o c ih =>
      intro s w
      rw [List.foldl_cons]
      cases o with
      | none =>
          rw [show insertOpt thresh s none = s from rfl, ih s w, List.mem_cons]
          simp
      | some v =>
          rw [show insertOpt thresh s (some v) = insertNonNull thresh s v from rfl]
          rw [ih (insertNonNull thresh s v) w, insertNonNull_mem thresh s v w, List.mem_cons]
          rw [show (some w = some v) = (w = v) from by simp]
          rw [or_assoc]

private theorem fold_merge_inv (thresh : Nat) :
    ∀ (ss : List State) (s : State),
      StateInvariant thresh s → StateInvariant thresh (ss.foldl (mergeState thresh) s) := by
  intro ss
  induction ss with
  | nil => intro s h; exact h
  | cons s' ss ih =>
      intro s h
      rw [List.foldl_cons]
      exact ih _ (fold_inv thresh (entriesOf s') s h)

private theorem fold_merge_mem (thresh : Nat) :
    ∀ (ss : List State) (s : State) (w : Int),
      w ∈ entriesOf (ss.foldl (mergeState thresh) s) ↔
        w ∈ entriesOf s ∨ ∃ t ∈ ss, w ∈ entriesOf t := by
  intro ss
  induction ss with
  | nil => intro s w; simp
  | cons s' ss ih =>
      intro s w
      have h2 : w ∈ entriesOf (mergeState thresh s s') ↔ w ∈ entriesOf s ∨ w ∈ entriesOf s' :=
        fold_mem thresh (entriesOf s') s w
      rw [List.foldl_cons, ih (mergeState thresh s s') w, h2]
      constructor
      · rintro ((ha | hb) | hc)
        · exact Or.inl ha
        · exact Or.inr ⟨s', List.mem_cons_self, hb⟩
        · obtain ⟨t, htt, ht⟩ := hc
          exact Or.inr ⟨t, List.mem_cons_of_mem s' htt, ht⟩
      · rintro (ha | ⟨t, htm, ht⟩)
        · exact Or.inl (Or.inl ha)
        · rw [List.mem_cons] at htm
          cases htm with
          | inl heq =>
              subst heq
              exact Or.inl (Or.inr ht)
          | inr hin => exact Or.inr ⟨t, hin, ht⟩

private theorem nodup_eraseDups_aux :
    ∀ (n : Nat) (xs : List Int), xs.length ≤ n → xs.eraseDups.Nodup := by
  intro n
  induction n with
  | zero =>
      intro xs hlen
      cases xs with
      | nil => simp
      | cons x xs =>
          rw [List.length_cons] at hlen
          omega
  | succ n ih =>
      intro xs hlen
      cases xs with
      | nil => simp
      | cons x xs =>
          rw [List.eraseDups_cons, List.nodup_cons]
          refine ⟨fun h => absurd (List.mem_filter.mp (List.mem_eraseDups.mp h)).2 (by simp), ?_⟩
          rw [List.length_cons] at hlen
          refine ih _ (Nat.le_trans (List.length_filter_le _ xs) ?_)
          omega

private theorem nodup_eraseDups (xs : List Int) : xs.eraseDups.Nodup :=
  nodup_eraseDups_aux xs.length xs (Nat.le_refl _)

private theorem exists_append_of_mem (l : List Int) :
    ∀ (a : Int), a ∈ l → ∃ p q, l = p ++ a :: q := by
  induction l with
  | nil => intro a h; exact absurd h List.not_mem_nil
  | cons b bs ih =>
      intro a h
      rw [List.mem_cons] at h
      cases h with
      | inl hb => exact ⟨[], bs, hb ▸ rfl⟩
      | inr hbs =>
          obtain ⟨p, q, hpq⟩ := ih a hbs
          exact ⟨b :: p, q, by rw [hpq, ← List.cons_append]⟩

private theorem nodup_same_length (l : List Int) :
    ∀ (k : List Int), l.Nodup → k.Nodup → (∀ a, a ∈ l ↔ a ∈ k) → l.length = k.length := by
  induction l with
  | nil =>
      intro k _ hk h
      cases k with
      | cons b bs =>
          exact absurd ((h b).mpr List.mem_cons_self) List.not_mem_nil
      | nil => rfl
  | cons a as ih =>
      intro k hl hk h
      rw [List.nodup_cons] at hl
      obtain ⟨hna, has⟩ := hl
      have ham : a ∈ k := (h a).mp List.mem_cons_self
      obtain ⟨k1, k2, hk12⟩ := exists_append_of_mem k a ham
      rw [hk12] at hk
      rw [hk12] at h
      rw [List.nodup_append] at hk
      obtain ⟨hk1, hnak2, hd⟩ := hk
      rw [List.nodup_cons] at hnak2
      obtain ⟨hak2, hk2⟩ := hnak2
      have hak1 : a ∉ k1 := fun ha1 => hd a ha1 a List.mem_cons_self rfl
      have hk12n : (k1 ++ k2).Nodup := by
        rw [List.nodup_append]
        exact ⟨hk1, hk2, fun x hx1 y hy => hd x hx1 y (List.mem_cons_of_mem a hy)⟩
      have hb : ∀ b, b ∈ as ↔ b ∈ k1 ++ k2 := by
        intro b
        have hbw : b ∈ a :: as ↔ b ∈ k1 ++ a :: k2 := h b
        simp only [List.mem_cons, List.mem_append] at hbw ⊢
        constructor
        · intro hb'
          rcases hbw.mp (Or.inr hb') with hb1 | hb0 | hb2
          · exact Or.inl hb1
          · exact absurd (hb0 ▸ hb') hna
          · exact Or.inr hb2
        · rintro (hb1 | hb2)
          · have h2 := hbw.mpr (Or.inl hb1)
            cases h2 with
            | inl hb0 => exact absurd (hb0 ▸ hb1) hak1
            | inr hb' => exact hb'
          · have h2 := hbw.mpr (Or.inr (Or.inr hb2))
            cases h2 with
            | inl hb0 => exact absurd (hb0 ▸ hb2) hak2
            | inr hb' => exact hb'
      have hlen := ih (k1 ++ k2) has hk12n hb
      have h1 : k.length = (k1 ++ k2).length + 1 := by
        rw [hk12, List.length_append, List.length_append, List.length_cons]
        omega
      simp only [List.length_cons]
      omega

private theorem finalize_eq_length (s : State) : finalize s = (entriesOf s).length := by
  cases s <;> rfl

private theorem inv_nodup (thresh : Nat) (s : State) (h : StateInvariant thresh s) :
    (entriesOf s).Nodup := by
  cases s with
  | empty => simp [entriesOf]
  | single v =>
      have h1 : entriesOf (State.single v) = [v] := rfl
      rw [h1, List.nodup_cons]
      exact ⟨List.not_mem_nil, List.nodup_nil⟩
  | small xs => exact h.2
  | large xs => exact h

private theorem spec_of_state (thresh : Nat) (s : State) (xs : List Int)
    (h : StateInvariant thresh s) (hm : ∀ w, w ∈ entriesOf s ↔ w ∈ xs) :
    finalize s = xs.eraseDups.length := by
  have hs : (entriesOf s).Nodup := inv_nodup thresh s h
  rw [finalize_eq_length]
  refine nodup_same_length (entriesOf s) xs.eraseDups hs (nodup_eraseDups xs) ?_
  intro w
  rw [hm, List.mem_eraseDups]

theorem sound : TinylambOpt.ResearchGoals.CountDistinctAdaptiveSwitch.Goal := by
  intro thresh chunks ht
  refine ⟨?_, ?_, ?_⟩
  · intro s hs
    rw [List.mem_map] at hs
    obtain ⟨c, hc, rfl⟩ := hs
    exact fold_opt_inv thresh c State.empty trivial
  · exact fold_merge_inv thresh
      (List.map (fun c => List.foldl (insertOpt thresh) State.empty c) chunks)
      State.empty trivial
  · refine spec_of_state thresh
        (List.foldl (mergeState thresh) State.empty
          (List.map (fun c => List.foldl (insertOpt thresh) State.empty c) chunks))
        (List.filterMap id (List.flatten chunks)) ?_ ?_
    · exact fold_merge_inv thresh
        (List.map (fun c => List.foldl (insertOpt thresh) State.empty c) chunks)
        State.empty trivial
    · intro w
      have h0 : ¬ (w ∈ entriesOf State.empty) := by
        have h1 : entriesOf State.empty = [] := rfl
        rw [h1]
        exact List.not_mem_nil
      rw [fold_merge_mem thresh
        (List.map (fun c => List.foldl (insertOpt thresh) State.empty c) chunks) State.empty w]
      constructor
      · rintro (hE | ⟨s, hs, hm⟩)
        · exact absurd hE h0
        · rw [List.mem_map] at hs
          obtain ⟨c, hdc, rfl⟩ := hs
          have h2 := (fold_opt_mem thresh c State.empty w).mp hm
          cases h2 with
          | inl he => exact absurd he h0
          | inr hmem =>
              rw [List.mem_filterMap]
              exact ⟨some w, List.mem_flatten.mpr ⟨c, hdc, hmem⟩, rfl⟩
      · intro h
        rw [List.mem_filterMap] at h
        obtain ⟨a, ham, hida⟩ := h
        have ha : a = some w := by simpa using hida
        rw [List.mem_flatten] at ham
        subst ha
        obtain ⟨c, hdc, hac⟩ := ham
        exact Or.inr ⟨List.foldl (insertOpt thresh) State.empty c,
          List.mem_map.mpr ⟨c, hdc, rfl⟩,
          (fold_opt_mem thresh c State.empty w).mpr (Or.inr hac)⟩

end TinylambOpt.ResearchProofs.CountDistinctAdaptiveSwitch
