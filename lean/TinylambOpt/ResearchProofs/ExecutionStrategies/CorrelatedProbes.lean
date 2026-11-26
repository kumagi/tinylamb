import TinylambOpt.ResearchGoals.ExecutionStrategies.CorrelatedProbes

/-!
These contracts describe the state or ordering needed for alternative SQL execution
strategies.

Proved (8 claims, including the following)

- `TinylambOpt.ResearchGoals.CorrelatedProbes.CountDistinctStop` — `∀ (xs : List Cell)
  k, enough k {} xs = decide (k ≤ (xs.filterMap id).eraseDups.length)`
- `TinylambOpt.ResearchGoals.CorrelatedProbes.Division` — Relational division: all
  required tags occur. Repeated requirements and duplicate supplied tags contribute
  once; SQL equality cannot witness a required NULL.
- `TinylambOpt.ResearchGoals.CorrelatedProbes.ExcludingMinimum` — `∀ xs i, ((firstTwo
  xs).find? fun e => e.2 != i).map Prod.fst = (excluding xs i).min?`
-/


namespace TinylambOpt.ResearchProofs.ExecutionStrategies.CorrelatedProbes


open TinylambOpt.ResearchGoals.CorrelatedProbes


private theorem D_nodup_eraseDups (l : List Int) : l.eraseDups.Nodup := by
  let P : Nat → Prop := fun n => ∀ l : List Int, l.length = n → l.eraseDups.Nodup
  have h : ∀ n, P n := fun n => Nat.strongRecOn n (fun n ih l hl => by
    cases l with
    | nil => simp
    | cons a as =>
        rw [List.eraseDups_cons]
        refine List.nodup_cons.mpr ⟨?_, ?_⟩
        · intro hm
          have hma : a ∈ as.filter (fun b => ! b == a) := (List.mem_eraseDups).mp hm
          have hf := (List.mem_filter.mp hma).2
          simp at hf
        · apply ih (as.filter (fun b => ! b == a)).length
          · have hl' : as.length + 1 = n := by simpa using hl
            have : as.length < n := by omega
            exact Nat.lt_of_le_of_lt (List.length_filter_le _ _) this
          · rfl)
  exact h l.length l rfl

private theorem D_nodup_length_eq_of_mem_iff {α : Type} [BEq α] [LawfulBEq α] :
    ∀ {l₁ : List α}, l₁.Nodup → ∀ l₂ : List α, l₂.Nodup →
      (∀ a, a ∈ l₁ ↔ a ∈ l₂) → l₁.length = l₂.length := by
  intro l₁ d₁
  induction l₁ with
  | nil =>
      intro l₂ d₂ h
      cases l₂ with
      | nil => rfl
      | cons b bs => exact absurd ((h b).mpr List.mem_cons_self) List.not_mem_nil
  | cons a r ih =>
      intro l₂ d₂ h
      obtain ⟨hna, hnd⟩ := List.nodup_cons.mp d₁
      have ha : a ∈ l₂ := (h a).mp (@List.mem_cons_self _ a r)
      have hd : (l₂.erase a).Nodup := d₂.erase a
      have hiff : ∀ c, c ∈ r ↔ c ∈ l₂.erase a := by
        intro c
        rw [List.Nodup.mem_erase_iff d₂]
        constructor
        · intro hr
          exact ⟨fun hca => hna (hca ▸ hr), (h c).mp (List.mem_cons_of_mem a hr)⟩
        · rintro ⟨hca, hc⟩
          rcases List.mem_cons.mp ((h c).mpr hc) with rfl | hr
          · exact absurd rfl hca
          · exact hr
      have hpos : 0 < l₂.length := by
        cases l₂ with
        | nil => exact absurd ha List.not_mem_nil
        | cons b bs => exact Nat.succ_pos _
      have hlen : (l₂.erase a).length + 1 = l₂.length := by
        rw [List.length_erase_of_mem ha]
        exact Nat.sub_add_cancel (Nat.succ_le_of_lt hpos)
      rw [List.length_cons, ih hnd (l₂.erase a) hd hiff, hlen]

private theorem D_nodup_filter {α : Type} (p : α → Bool) {l : List α} (h : l.Nodup) :
    (l.filter p).Nodup := by
  induction l with
  | nil => simp
  | cons a l ih =>
      obtain ⟨hna, hnd⟩ := List.nodup_cons.mp h
      have ht := ih hnd
      by_cases hp : p a = true
      · have hna' : ¬ a ∈ l.filter p := fun hm => hna (List.mem_filter.mp hm).1
        simpa [List.filter_cons, hp] using (List.nodup_cons.mpr ⟨hna', ht⟩)
      · simpa [List.filter_cons, hp] using ht

private theorem D_mem_filterMap_some {xs : List Cell} {v : Int} :
    v ∈ (xs.filterMap id) ↔ some v ∈ xs := by
  constructor
  · intro h
    rcases List.mem_filterMap.mp h with ⟨a, ha, hfa⟩
    have : a = some v := by simpa using hfa
    rwa [this] at ha
  · intro h
    exact List.mem_filterMap.mpr ⟨some v, h, rfl⟩

private theorem D_contains_iff_mem_general {α : Type} [BEq α] [LawfulBEq α]
    {xs : List α} {c : α} : xs.contains c = true ↔ c ∈ xs := by
  rw [List.contains_eq_mem, decide_eq_true_eq]

private theorem D_contains_iff_mem {xs : List Cell} {c : Cell} : xs.contains c = true ↔ c ∈ xs := by
  rw [List.contains_eq_mem, decide_eq_true_eq]

private theorem D_mem_supplied {supplied : List Cell} {v : Int} :
    v ∈ (supplied.filterMap id).eraseDups ↔ some v ∈ supplied := by
  rw [List.mem_eraseDups, D_mem_filterMap_some]

private theorem D_mem_need {need : List Cell} {v : Int} :
    v ∈ (need.filterMap id).eraseDups ↔ some v ∈ need := by
  rw [List.mem_eraseDups, D_mem_filterMap_some]

private theorem D_match_subset {need supplied : List Cell} {v : Int} :
    v ∈ ((supplied.filterMap id).eraseDups.filter (fun h => need.contains (some h))) →
      v ∈ (need.filterMap id).eraseDups := by
  intro h
  have hf := List.mem_filter.mp h
  exact D_mem_need.mpr ((D_contains_iff_mem.mp hf.2))

private theorem D_match_mem {need supplied : List Cell} {v : Int} :
    v ∈ ((supplied.filterMap id).eraseDups.filter (fun h => need.contains (some h))) ↔
      some v ∈ supplied ∧ some v ∈ need := by
  constructor
  · intro h
    have hf := List.mem_filter.mp h
    exact ⟨D_mem_supplied.mp hf.1, D_contains_iff_mem.mp hf.2⟩
  · rintro ⟨hs, hn⟩
    apply List.mem_filter.mpr
    exact ⟨D_mem_supplied.mpr hs, D_contains_iff_mem.mpr hn⟩

private theorem D_nodup_length_of_subset {α : Type} [BEq α] [LawfulBEq α]
    {l₁ l₂ : List α} (h1 : l₁.Nodup) (h2 : l₂.Nodup)
    (hsub : ∀ a, a ∈ l₁ → a ∈ l₂) (hlen : l₁.length = l₂.length) :
    ∀ a, a ∈ l₂ → a ∈ l₁ := by
  let f : List α := l₂.filter (fun x => l₁.contains x)
  have hfnd : f.Nodup := D_nodup_filter _ h2
  have hfsub : f.Sublist l₂ := List.filter_sublist
  have hfmem : ∀ a, a ∈ f ↔ a ∈ l₁ := by
    intro a
    constructor
    · intro ha
      exact D_contains_iff_mem_general.mp (List.mem_filter.mp ha).2
    · intro ha
      exact List.mem_filter.mpr ⟨hsub a ha, D_contains_iff_mem_general.mpr ha⟩
  have hflen : f.length = l₁.length := by
    exact D_nodup_length_eq_of_mem_iff hfnd l₁ h1 hfmem
  have : f.length = l₂.length := by omega
  have hfeq : f = l₂ := hfsub.eq_of_length this
  intro a ha
  have : a ∈ f := by simpa [hfeq] using ha
  exact (hfmem a).mp this

private theorem D_all_true_iff {need supplied : List Cell}
    (hno : need.contains none = false) :
    (need.all fun n => supplied.any fun h => h.isSome && h == n) = true ↔
      ∀ v ∈ (need.filterMap id).eraseDups,
        v ∈ ((supplied.filterMap id).eraseDups.filter (fun h => need.contains (some h))) := by
  constructor
  · intro hall v hv
    have hn : some v ∈ need := D_mem_need.mp hv
    have hall' := (List.all_eq_true.mp hall) (some v) hn
    rcases (List.any_eq_true.mp hall') with ⟨h, hh, hhh⟩
    cases h with
    | none => simp at hhh
    | some w =>
        have hw : w = v := by simpa using hhh
        subst w
        exact D_match_mem.mpr ⟨hh, hn⟩
  · intro hsub
    apply List.all_eq_true.mpr
    intro n hn
    cases n with
    | none =>
        have hc : need.contains none = true := D_contains_iff_mem.mpr hn
        rw [hno] at hc
        contradiction
    | some v =>
        have hv : v ∈ (need.filterMap id).eraseDups := D_mem_need.mpr hn
        have hm := hsub v hv
        have hs := (D_match_mem.mp hm).1
        apply List.any_eq_true.mpr
        exact ⟨some v, hs, by simp⟩

theorem Division_sound : TinylambOpt.ResearchGoals.CorrelatedProbes.Division.Goal := by
  intro need supplied
  apply Bool.eq_iff_iff.mpr
  constructor
  · intro hlhs
    have hall : (need.all fun n => supplied.any fun h => h.isSome && h == n) = true := hlhs
    by_cases hnone : need.contains none = true
    · have : False := by
        have hnm : none ∈ need := D_contains_iff_mem.mp hnone
        have hf := (List.all_eq_true.mp hall) none hnm
        simp at hf
      contradiction
    · have hno' : need.contains none = false := by cases h : need.contains none <;> simp_all
      have hsub := (D_all_true_iff hno').mp hall
      have hlenNat :
          ((supplied.filterMap id).eraseDups.filter (fun h => need.contains (some h))).length =
            (need.filterMap id).eraseDups.length := by
        exact D_nodup_length_eq_of_mem_iff
          (D_nodup_filter _ (D_nodup_eraseDups (supplied.filterMap id)))
          ((need.filterMap id).eraseDups)
          (D_nodup_eraseDups (need.filterMap id))
          (fun v => ⟨D_match_subset, hsub v⟩)
      have hlen_eq : (((supplied.filterMap id).eraseDups.filter (fun h => need.contains (some h))).length ==
          (need.filterMap id).eraseDups.length) = true := by
        simpa [hlenNat]
      simpa only [hno', Bool.not_false, Bool.true_and] using hlen_eq
  · intro hrhs
    by_cases hnone : need.contains none = true
    · have : False := by
        have htmp := hrhs
        simp only [hnone, Bool.not_true, Bool.false_and] at htmp
        cases htmp
      exact False.elim this
    · have hno' : need.contains none = false := by cases h : need.contains none <;> simp_all
      have hlen_eq_bool : (((supplied.filterMap id).eraseDups.filter (fun h => need.contains (some h))).length ==
        (need.filterMap id).eraseDups.length) = true := by
        simpa only [hno', Bool.not_false, Bool.true_and] using hrhs
      have hlen_eq : (((supplied.filterMap id).eraseDups.filter (fun h => need.contains (some h))).length) =
          (need.filterMap id).eraseDups.length := by
        simpa using hlen_eq_bool
      have hsub : ∀ v, v ∈ ((supplied.filterMap id).eraseDups.filter (fun h => need.contains (some h))) →
          v ∈ (need.filterMap id).eraseDups := by intro v hv; exact D_match_subset hv
      have hsuper : ∀ v, v ∈ (need.filterMap id).eraseDups →
          v ∈ ((supplied.filterMap id).eraseDups.filter (fun h => need.contains (some h))) :=
        D_nodup_length_of_subset
          (D_nodup_filter _ (D_nodup_eraseDups (supplied.filterMap id)))
          (D_nodup_eraseDups (need.filterMap id)) hsub hlen_eq
      exact (D_all_true_iff hno').mpr hsuper


private def O_capStep (bound : Nat) (m : Std.HashMap Nat (Array Cell)) (r : Nat × Cell) :=
  let a := m.getD r.1 #[]
  if a.size < bound then m.insert r.1 (a.push r.2) else m

private theorem O_take_eq_self_of_le {α : Type} (l : List α) {n : Nat} (h : l.length ≤ n) :
    l.take n = l := by
  induction l generalizing n with
  | nil => cases n <;> rfl
  | cons a t ih =>
      cases n with
      | zero =>
          simp only [List.length_cons] at h
          omega
      | succ n =>
          simp only [List.length_cons] at h
          change a :: t.take n = a :: t
          rw [ih (by omega)]

private theorem O_capStep_cap {bound : Nat} {m : Std.HashMap Nat (Array Cell)}
    (hcap : ∀ j, (m.getD j #[]).size ≤ bound) (r : Nat × Cell) :
    ∀ j, ((O_capStep bound m r).getD j #[]).size ≤ bound := by
  intro j
  unfold O_capStep
  by_cases hr : (m.getD r.1 #[]).size < bound
  · rw [if_pos hr, Std.HashMap.getD_insert]
    by_cases hjr : (r.1 == j) = true
    · rw [if_pos hjr]
      simp [Array.size_push]
      have := hcap r.1
      omega
    · rw [if_neg hjr]
      exact hcap j
  · rw [if_neg hr]
    exact hcap j

private theorem O_fold_getD_toList (rows : List (Nat × Cell))
    (bound k : Nat) (m : Std.HashMap Nat (Array Cell))
    (hcap : ∀ j, (m.getD j #[]).size ≤ bound) :
    ((rows.foldl (O_capStep bound) m).getD k #[]).toList =
      (((m.getD k #[]).toList) ++ ((rows.filter fun r => r.1 == k).map Prod.snd)).take bound := by
  induction rows generalizing m with
  | nil =>
      rw [List.foldl_nil, List.filter_nil, List.map_nil, List.append_nil]
      exact (O_take_eq_self_of_le ((m.getD k #[]).toList) (by simpa using hcap k)).symm
  | cons r rows ih =>
      change ((rows.foldl (O_capStep bound) (O_capStep bound m r)).getD k #[]).toList =
        ((m.getD k #[]).toList ++ ((r :: rows).filter fun q => q.1 == k).map Prod.snd).take bound
      have hcap' : ∀ j, ((O_capStep bound m r).getD j #[]).size ≤ bound := O_capStep_cap hcap r
      by_cases hr : (r.1 == k) = true
      · have hreq : r.1 = k := by simpa using hr
        subst k
        have hget : ((O_capStep bound m r).getD r.1 #[]).toList =
            if (m.getD r.1 #[]).size < bound then (m.getD r.1 #[]).toList ++ [r.2]
            else (m.getD r.1 #[]).toList := by
          unfold O_capStep
          by_cases hh : (m.getD r.1 #[]).size < bound
          · rw [if_pos hh, Std.HashMap.getD_insert]
            simp [hh, Array.toList_push]
          · simp [hh]
        rw [ih (O_capStep bound m r) hcap', hget]
        by_cases hh : (m.getD r.1 #[]).size < bound
        · rw [if_pos hh]
          simp [hr]
        · rw [if_neg hh]
          simp [hr]
          have hsize_eq : (m.getD r.1 #[]).size = bound := by
            have hle := hcap r.1
            omega
          have hlen : (m.getD r.1 #[]).toList.length = bound := by simpa using hsize_eq
          calc
            ((m.getD r.1 #[]).toList ++ List.map Prod.snd (List.filter (fun q => q.1 == r.1) rows)).take bound
                = ((m.getD r.1 #[]).toList).take bound := by
                    rw [List.take_append_of_le_length (by omega : bound ≤ ((m.getD r.1 #[]).toList).length)]
            _ = (m.getD r.1 #[]).toList := O_take_eq_self_of_le ((m.getD r.1 #[]).toList) (by omega : ((m.getD r.1 #[]).toList).length ≤ bound)
            _ = ((m.getD r.1 #[]).toList ++ r.2 :: List.map Prod.snd (List.filter (fun q => q.1 == r.1) rows)).take bound := by
                    rw [List.take_append_of_le_length (by omega : bound ≤ ((m.getD r.1 #[]).toList).length)]
                    exact (O_take_eq_self_of_le ((m.getD r.1 #[]).toList) (by omega : ((m.getD r.1 #[]).toList).length ≤ bound)).symm
      · have hget : ((O_capStep bound m r).getD k #[]) = (m.getD k #[]) := by
          unfold O_capStep
          by_cases hh : (m.getD r.1 #[]).size < bound
          · rw [if_pos hh, Std.HashMap.getD_insert]
            rw [if_neg hr]
          · simp [hh]
        rw [ih (O_capStep bound m r) hcap']
        rw [hget]
        simp [hr]

private theorem O_scalar_many_of_len (xs : List Cell) (h : 2 ≤ xs.length) : scalar xs = .many := by
  cases xs with
  | nil => simp at h
  | cons a xs =>
      cases xs with
      | nil => simp at h
      | cons b xs => rfl

private theorem O_hcap_empty (bound : Nat) : ∀ j, ((({} : Std.HashMap Nat (Array Cell)).getD j #[]).size ≤ bound) := by
  intro j
  simp

theorem OffsetScalar_sound : TinylambOpt.ResearchGoals.CorrelatedProbes.OffsetScalar.Goal := by
  intro rows d k
  let L := ((rows.filter fun r => r.1 == k).map Prod.snd)
  have hT : ((capped rows (d+2)).getD k #[]).toList = L.take (d+2) := by
    dsimp [capped]
    let m : Std.HashMap Nat (Array Cell) := {}
    change ((rows.foldl (O_capStep (d+2)) m).getD k #[]).toList = L.take (d+2)
    rw [O_fold_getD_toList rows (d+2) k m (O_hcap_empty (d+2))]
    simp [m, L]
  by_cases hsmall : L.length ≤ d + 1
  · have htake : L.take (d+2) = L := O_take_eq_self_of_le L (by omega)
    simp [scalar, hT, htake, L]
  · have hbig : d + 2 ≤ L.length := by omega
    have hlenT : ((L.take (d+2)).drop d).length = 2 := by
      rw [List.length_drop, List.length_take]
      rw [Nat.min_eq_left hbig]
      omega
    have hlenL : 2 ≤ (L.drop d).length := by
      rw [List.length_drop]
      omega
    rw [hT]
    rw [O_scalar_many_of_len _ (by omega : 2 ≤ ((L.take (d+2)).drop d).length)]
    rw [O_scalar_many_of_len _ hlenL]


private theorem C_nodup_eraseDups (l : List Int) : l.eraseDups.Nodup := by
  let P : Nat → Prop := fun n => ∀ l : List Int, l.length = n → l.eraseDups.Nodup
  have h : ∀ n, P n := fun n => Nat.strongRecOn n (fun n ih l hl => by
    cases l with
    | nil => simp
    | cons a as =>
        rw [List.eraseDups_cons]
        refine List.nodup_cons.mpr ⟨?_, ?_⟩
        · intro hm
          have hma : a ∈ as.filter (fun b => ! b == a) := (List.mem_eraseDups).mp hm
          have hf := (List.mem_filter.mp hma).2
          simp at hf
        · apply ih (as.filter (fun b => ! b == a)).length
          · have hl' : as.length + 1 = n := by simpa using hl
            have : as.length < n := by omega
            exact Nat.lt_of_le_of_lt (List.length_filter_le _ _) this
          · rfl)
  exact h l.length l rfl

private theorem C_nodup_length_eq_of_mem_iff {α : Type} [BEq α] [LawfulBEq α] :
    ∀ {l₁ : List α}, l₁.Nodup → ∀ l₂ : List α, l₂.Nodup →
      (∀ a, a ∈ l₁ ↔ a ∈ l₂) → l₁.length = l₂.length := by
  intro l₁ d₁
  induction l₁ with
  | nil =>
      intro l₂ d₂ h
      cases l₂ with
      | nil => rfl
      | cons b bs => exact absurd ((h b).mpr List.mem_cons_self) List.not_mem_nil
  | cons a r ih =>
      intro l₂ d₂ h
      obtain ⟨hna, hnd⟩ := List.nodup_cons.mp d₁
      have ha : a ∈ l₂ := (h a).mp (@List.mem_cons_self _ a r)
      have hd : (l₂.erase a).Nodup := d₂.erase a
      have hiff : ∀ c, c ∈ r ↔ c ∈ l₂.erase a := by
        intro c
        rw [List.Nodup.mem_erase_iff d₂]
        constructor
        · intro hr
          exact ⟨fun hca => hna (hca ▸ hr), (h c).mp (List.mem_cons_of_mem a hr)⟩
        · rintro ⟨hca, hc⟩
          rcases List.mem_cons.mp ((h c).mpr hc) with rfl | hr
          · exact absurd rfl hca
          · exact hr
      have hpos : 0 < l₂.length := by
        cases l₂ with
        | nil => exact absurd ha List.not_mem_nil
        | cons b bs => exact Nat.succ_pos _
      have hlen : (l₂.erase a).length + 1 = l₂.length := by
        rw [List.length_erase_of_mem ha]
        exact Nat.sub_add_cancel (Nat.succ_le_of_lt hpos)
      rw [List.length_cons, ih hnd (l₂.erase a) hd hiff, hlen]

private theorem C_cons_eraseDups_length (v : Int) (L : List Int) :
    (v :: L).eraseDups.length = 1 + (L.filter fun w => w ≠ v).eraseDups.length := by
  have hnd : (v :: (L.filter fun w => w ≠ v).eraseDups).Nodup := by
    refine List.nodup_cons.mpr ⟨?_, C_nodup_eraseDups _⟩
    intro hm
    have hm' := (List.mem_eraseDups.mp hm)
    simp only [List.mem_filter] at hm'
    have hne : v ≠ v := of_decide_eq_true hm'.2
    exact hne rfl
  have hlen := C_nodup_length_eq_of_mem_iff (C_nodup_eraseDups (v :: L))
      (v :: (L.filter fun w => w ≠ v).eraseDups) hnd (fun z => by
    constructor
    · intro hz
      have hz' : z = v ∨ z ∈ L := by
        simpa only [List.mem_eraseDups, List.mem_cons] using hz
      rcases hz' with rfl | hzL
      · simp
      · by_cases hzv : z = v
        · simp [hzv]
        · right
          apply List.mem_eraseDups.mpr
          exact List.mem_filter.mpr ⟨hzL, decide_eq_true hzv⟩
    · intro hz
      have hz' : z = v ∨ z ∈ (L.filter fun w => w ≠ v).eraseDups := by
        simpa only [List.mem_cons] using hz
      rcases hz' with rfl | hzF
      · apply List.mem_eraseDups.mpr
        simp
      · have hzL : z ∈ L := (List.mem_filter.mp (List.mem_eraseDups.mp hzF)).1
        apply List.mem_eraseDups.mpr
        exact List.mem_cons_of_mem v hzL)
  simpa [Nat.add_comm] using hlen

private theorem C_size_mono_insert (s : Std.HashSet Int) (v : Int) : s.size ≤ (s.insert v).size := by
  by_cases h : v ∈ s
  · have : (s.insert v).size = s.size := by simp [Std.HashSet.size_insert, h]
    omega
  · have : (s.insert v).size = s.size + 1 := by simp [Std.HashSet.size_insert, h]
    omega

private theorem C_fold_insert_mono (s : Std.HashSet Int) (vs : List Int) :
    s.size ≤ (vs.foldl (fun acc v => acc.insert v) s).size := by
  induction vs generalizing s with
  | nil => simpa using (Nat.le_refl s.size)
  | cons v vs ih =>
      exact Nat.le_trans (C_size_mono_insert s v) (ih (s.insert v))

private theorem C_fold_insert_size (s : Std.HashSet Int) (vs : List Int) :
    (vs.foldl (fun acc v => acc.insert v) s).size =
      s.size + (vs.filter fun v => !(s.contains v)).eraseDups.length := by
  induction vs generalizing s with
  | nil => simp
  | cons v vs ih =>
      simp only [List.foldl_cons]
      by_cases hv : s.contains v = true
      · have hmemv : v ∈ s := Std.HashSet.mem_iff_contains.mp hv
        have hs' : (s.insert v).size = s.size := by simp [Std.HashSet.size_insert, hmemv]
        have hcontains : ∀ w, (s.insert v).contains w = s.contains w := by
          intro w
          rw [Std.HashSet.contains_insert]
          by_cases hvw : (v == w) = true
          · have hveq : v = w := by simpa using hvw
            subst w
            simp [hv]
          · simp [hvw]
        have hfilter : (vs.filter fun w => !(s.insert v).contains w) =
            (vs.filter fun w => !s.contains w) := by
          apply List.filter_congr
          intro w hw
          simp [hcontains w]
        rw [ih (s.insert v), hs', hfilter]
        simp [hv]
      · have hnotmem : v ∉ s := by
          intro hm
          exact hv (Std.HashSet.mem_iff_contains.mpr hm)
        have hs' : (s.insert v).size = s.size + 1 := by
          simp [Std.HashSet.size_insert, hnotmem]
        have hfilter : (vs.filter fun w => !(s.insert v).contains w) =
            ((vs.filter fun w => !s.contains w).filter fun w => w ≠ v) := by
          rw [List.filter_filter]
          apply List.filter_congr
          intro w hw
          rw [Std.HashSet.contains_insert]
          by_cases hvw : (v == w) = true
          · have hveq : v = w := by simpa using hvw
            subst w
            simp
          · have hwne : w ≠ v := by intro h; apply hvw; simp [h]
            simp [hvw, hwne]
        rw [ih (s.insert v), hs', hfilter]
        simp [hv]
        have hff : (List.filter (fun a => !decide (a = v) && !s.contains a) vs) =
            ((List.filter (fun w => !s.contains w) vs).filter fun w => w ≠ v) := by
          rw [List.filter_filter]
          apply List.filter_congr
          intro w hw
          simp [Bool.and_comm]
        rw [hff]
        rw [C_cons_eraseDups_length]
        omega

private theorem C_enough_spec (k : Nat) (s : Std.HashSet Int) :
    ∀ xs : List Cell,
      enough k s xs = decide (k ≤ ((xs.filterMap id).foldl (fun acc v => acc.insert v) s).size) := by
  intro xs
  induction xs generalizing s with
  | nil => rfl
  | cons x xs ih =>
      by_cases hs : s.size ≥ k
      · have hmono : s.size ≤ (((x :: xs).filterMap id).foldl (fun acc v => acc.insert v) s).size :=
          C_fold_insert_mono s ((x :: xs).filterMap id)
        have hks : k ≤ s.size := by
          have hd : decide (k ≤ s.size) = true := by simpa using hs
          exact of_decide_eq_true hd
        have hk' : k ≤ (((x :: xs).filterMap id).foldl (fun acc v => acc.insert v) s).size := by omega
        simp [enough, hs, decide_eq_true hk']
      · cases x with
        | none =>
            simpa [enough, hs] using ih s
        | some n =>
            simpa [enough, hs] using ih (s.insert n)

theorem CountDistinctStop_sound : TinylambOpt.ResearchGoals.CorrelatedProbes.CountDistinctStop.Goal := by
  intro xs k
  rw [C_enough_spec k {} xs]
  have hsize : (((xs.filterMap id).foldl (fun acc v => acc.insert v) ({} : Std.HashSet Int)).size) =
      (xs.filterMap id).eraseDups.length := by
    rw [C_fold_insert_size]
    have hf : (xs.filterMap id).filter (fun v => !(({} : Std.HashSet Int).contains v)) =
        xs.filterMap id := by
      apply List.filter_eq_self.mpr
      intro v hv
      simp
    rw [hf]
    simp
  rw [hsize]


private theorem Or_zipIdx_lower_bound {α : Type} (xs : List α) (n : Nat) :
    ∀ p ∈ xs.zipIdx n, n ≤ p.2 := by
  induction xs generalizing n with
  | nil => simp
  | cons x xs ih =>
      intro p hp
      rw [List.zipIdx_cons] at hp
      simp at hp
      rcases hp with rfl | hp
      · simp
      · have := ih (n:=n+1) p hp
        omega
termination_by xs.length

private theorem Or_zipIdx_pairwise {α : Type} (xs : List α) (n : Nat) :
    (xs.zipIdx n).Pairwise (fun a b => a.2 < b.2) := by
  induction xs generalizing n with
  | nil => simp
  | cons x xs ih =>
      rw [List.zipIdx_cons]
      apply List.Pairwise.cons
      · intro p hp
        have hle := Or_zipIdx_lower_bound xs (n+1) p hp
        omega
      · exact ih (n:=n+1)
termination_by xs.length

private theorem Or_merge_filter_compl {α : Type} (ts : List (α × Nat))
    (hpair : ts.Pairwise (fun a b => a.2 < b.2)) (p q : α → Bool) :
    ts.filter (fun t => p t.1 || q t.1) =
      List.merge (ts.filter fun t => p t.1)
        (ts.filter fun t => !p t.1 && q t.1) (fun u v => u.2 ≤ v.2) := by
  induction ts with
  | nil => simp
  | cons t ts ih =>
      have ht_all : ∀ v ∈ ts, t.2 < v.2 := (List.pairwise_cons.mp hpair).1
      have htail : ts.Pairwise (fun a b => a.2 < b.2) := List.Pairwise.of_cons hpair
      have ihts := ih htail
      by_cases hp : p t.1 = true
      · cases h2 : (ts.filter fun t => !p t.1 && q t.1) with
        | nil => simp [hp, h2, ihts]
        | cons v vs =>
            have hvf : v ∈ ts.filter (fun t => !p t.1 && q t.1) := by simp [h2]
            have hv_mem : v ∈ ts := (List.mem_filter.mp hvf).1
            have hle : decide (t.2 ≤ v.2) = true := by
              apply decide_eq_true
              have := ht_all v hv_mem
              omega
            simp [hp, h2, hle, ihts]
      · by_cases hq : q t.1 = true
        · cases h1 : (ts.filter fun t => p t.1) with
          | nil => simp [hp, hq, h1, ihts]
          | cons u us =>
              have huf : u ∈ ts.filter (fun t => p t.1) := by simp [h1]
              have hu_mem : u ∈ ts := (List.mem_filter.mp huf).1
              have hle : decide (u.2 ≤ t.2) = false := by
                apply decide_eq_false
                have := ht_all u hu_mem
                omega
              simp [hp, hq, h1, hle, ihts]
        · simp [hp, hq, ihts]

theorem OrExists_sound : TinylambOpt.ResearchGoals.CorrelatedProbes.OrExists.Goal := by
  intro xs a b
  exact Or_merge_filter_compl (xs.zipIdx) (Or_zipIdx_pairwise xs 0)
    (fun r : Nat × Nat => a.contains r.1) (fun r : Nat × Nat => b.contains r.2)


private theorem P_countP_map_pair {α : Type} (ys : List (Option α)) (x : Option α)
    (p : Option α → Bool) (q : Option α → Bool) :
    (ys.map (fun y => (x,y))).countP (fun z => p z.1 && q z.2) =
      if p x then ys.countP q else 0 := by
  induction ys with
  | nil => cases h : p x <;> simp [h]
  | cons y ys ih =>
      cases hpx : p x
      · simp [hpx, ih]
      · cases hqy : q y <;> simp [hpx, hqy, ih]

private theorem P_countP_flatMap_pairs {α : Type} (xs : List (Option α)) (ys : List (Option α))
    (p : Option α → Bool) (q : Option α → Bool) :
    (xs.flatMap (fun x => ys.map fun y => (x,y))).countP (fun z => p z.1 && q z.2) =
      xs.countP p * ys.countP q := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
      rw [List.flatMap_cons, List.countP_append, ih, P_countP_map_pair]
      cases h : p x <;> simp [h, List.countP_cons, Nat.succ_mul] <;> omega

theorem OptionalSiblings_sound : TinylambOpt.ResearchGoals.CorrelatedProbes.OptionalSiblings.Goal := by
  intro a b p q
  exact P_countP_flatMap_pairs (pad a) (pad b) p q


private theorem Pre_bool_prefix {p s : List UInt8} : p.isPrefixOf s = true ↔ p <+: s := by simp

private theorem Pre_mem_minimal {p : List UInt8} {ps : List (List UInt8)}
    (h : p ∈ minimal ps) : p ∈ ps := by
  unfold minimal at h
  exact List.mem_eraseDups.mp (List.mem_filter.mp h).1

private theorem Pre_not_minimal_cover {p : List UInt8} {ps : List (List UInt8)}
    (hp : p ∈ ps) (hn : p ∉ minimal ps) :
    ∃ q ∈ ps, q.length < p.length ∧ q <+: p := by
  unfold minimal at hn
  by_cases hd : p ∈ ps.eraseDups
  · have hf : ¬ (!(ps.any fun q => q.length < p.length && q.isPrefixOf p)) = true := by
      intro hh
      have : p ∈ ps.eraseDups.filter (fun p => !(ps.any fun q => q.length < p.length && q.isPrefixOf p)) :=
        List.mem_filter.mpr ⟨hd, hh⟩
      exact hn this
    have hf' : (ps.any fun q => q.length < p.length && q.isPrefixOf p) = true := by
      cases h : ps.any (fun q => q.length < p.length && q.isPrefixOf p) <;> simp_all
    rcases List.any_eq_true.mp hf' with ⟨q, hqps, hq⟩
    have hq' : (decide (q.length < p.length) = true) ∧ q.isPrefixOf p = true := by
      simpa only [Bool.and_eq_true] using hq
    exact ⟨q, hqps, of_decide_eq_true hq'.1, (Pre_bool_prefix.mp hq'.2)⟩
  · exfalso
    apply hd
    apply List.mem_eraseDups.mpr
    exact hp

private theorem Pre_exists_minimal_prefix (p : List UInt8) (hps : p <+: s)
    (ps : List (List UInt8)) (hp : p ∈ ps) : ∃ m ∈ minimal ps, m <+: s := by
  by_cases hm : p ∈ minimal ps
  · exact ⟨p, hm, hps⟩
  · rcases Pre_not_minimal_cover hp hm with ⟨q, hqps, hlt, hqp⟩
    exact Pre_exists_minimal_prefix q (hqp.trans hps) ps hqps
termination_by p.length

theorem PrefixAntichain_sound : TinylambOpt.ResearchGoals.CorrelatedProbes.PrefixAntichain.Goal := by
  intro ps xs
  induction xs with
  | nil => rfl
  | cons x xs ih =>
      simp only [List.filter_cons]
      have hx : (x.any (fun s => ps.any fun p => p.isPrefixOf s)) =
          (x.any (fun s => (minimal ps).any fun p => p.isPrefixOf s)) := by
        cases x with
        | none => simp
        | some s =>
            apply Bool.eq_iff_iff.mpr
            constructor
            · intro h
              have h' : (ps.any fun p => p.isPrefixOf s) = true := by simpa using h
              rcases List.any_eq_true.mp h' with ⟨p, hpps, hprefix⟩
              rcases Pre_exists_minimal_prefix p (Pre_bool_prefix.mp hprefix) ps hpps with ⟨m, hm, hms⟩
              apply List.any_eq_true.mpr
              exact ⟨m, hm, Pre_bool_prefix.mpr hms⟩
            · intro h
              have h' : ((minimal ps).any fun p => p.isPrefixOf s) = true := by simpa using h
              rcases List.any_eq_true.mp h' with ⟨p, hpm, hprefix⟩
              apply List.any_eq_true.mpr
              exact ⟨p, Pre_mem_minimal hpm, hprefix⟩
      rw [hx, ih]


private def FT_step (best : List (Int × Nat)) (e : Cell × Nat) : List (Int × Nat) :=
  match e.1 with
  | none => best
  | some v =>
    match best with
    | [] => [(v, e.2)]
    | a::rest =>
      if v < a.1 then [(v, e.2), a]
      else match rest with
        | [] => [a, (v, e.2)]
        | b::_ => if v < b.1 then [a, (v, e.2)] else best


private def FT_occ (xs : List Cell) (v : Int) (p : Nat) : Prop := xs[p]? = some (some v)

private theorem FT_occ_append (pre : List Cell) (c : Cell) (v : Int) (p : Nat) :
    FT_occ (pre ++ [c]) v p ↔ FT_occ pre v p ∨ (p = pre.length ∧ c = some v) := by
  unfold FT_occ
  rw [List.getElem?_append]
  by_cases hp : p < pre.length
  · have hpne : p ≠ pre.length := by omega
    simp [hp, hpne]
  · by_cases hpeq : p = pre.length
    · subst p
      simp
    · have hpgt : pre.length < p := by omega
      have hpre_none : pre[p]? = none := List.getElem?_eq_none (by omega)
      have htail_none : [c][p - pre.length]? = none := by
        apply List.getElem?_eq_none
        simp
        omega
      simp [hp, hpre_none, htail_none, hpeq]

private theorem FT_occ_lt_len {pre : List Cell} {v : Int} {p : Nat} (h : FT_occ pre v p) :
    p < pre.length := by
  unfold FT_occ at h
  rcases List.getElem?_eq_some_iff.mp h with ⟨hp, _⟩
  exact hp

private theorem FT_occ_unique {pre : List Cell} {v w : Int} {p : Nat}
    (hv : FT_occ pre v p) (hw : FT_occ pre w p) : v = w := by
  unfold FT_occ at hv hw
  have heq : some (some v) = some (some w) := hv.symm.trans hw
  simpa using heq

private theorem mem_iff_occ (xs : List Cell) (w : Int) : some w ∈ xs ↔ ∃ p, FT_occ xs w p := by
  unfold FT_occ
  exact List.mem_iff_getElem?

private def FT_lt (a b : Int × Nat) : Prop := a.1 < b.1 ∨ (a.1 = b.1 ∧ a.2 < b.2)
private def FT_le (a b : Int × Nat) : Prop := a.1 < b.1 ∨ (a.1 = b.1 ∧ a.2 ≤ b.2)

private theorem FT_le_refl (a : Int × Nat) : FT_le a a := by right; constructor <;> omega

private theorem FT_of_lt_le {a b c : Int × Nat} (hab : FT_lt a b) (hbc : FT_le b c) : FT_le a c := by
  rcases hab with hab | ⟨haeq, hab2⟩
  · rcases hbc with hbc | ⟨hbeq, hbc2⟩
    · exact Or.inl (by omega)
    · exact Or.inl (by omega)
  · rcases hbc with hbc | ⟨hbeq, hbc2⟩
    · exact Or.inl (by omega)
    · exact Or.inr ⟨by omega, by omega⟩

private def FT_inv (pre : List Cell) (best : List (Int × Nat)) : Prop :=
  match best with
  | [] => ∀ v p, ¬ FT_occ pre v p
  | [a] => FT_occ pre a.1 a.2 ∧ ∀ v p, FT_occ pre v p → (v, p) = a
  | [a, b] => FT_occ pre a.1 a.2 ∧ FT_occ pre b.1 b.2 ∧ FT_lt a b ∧
      (∀ v p, FT_occ pre v p → FT_le a (v, p)) ∧
      (∀ v p, FT_occ pre v p → (v, p) ≠ a → FT_le b (v, p))
  | _ => False

private theorem FT_step_inv (pre : List Cell) (best : List (Int × Nat)) (c : Cell)
    (h : FT_inv pre best) :
    FT_inv (pre ++ [c]) (FT_step best (c, pre.length)) := by
  cases hbest : best with
  | nil =>
      simp [FT_inv, hbest] at h
      cases c with
      | none =>
          intro v p hv
          rcases (FT_occ_append pre none v p).mp hv with hold | hnew
          · exact h v p hold
          · simp at hnew
      | some m =>
          constructor
          · exact (FT_occ_append pre (some m) m pre.length).mpr (Or.inr ⟨rfl, rfl⟩)
          · intro v p hv
            rcases (FT_occ_append pre (some m) v p).mp hv with hold | hnew
            · exact False.elim (h v p hold)
            · ext <;> simp_all
  | cons a rest =>
      cases hrest : rest with
      | nil =>
          simp [FT_inv, hbest, hrest] at h
          rcases h with ⟨haocc, hall⟩
          cases c with
          | none =>
              constructor
              · exact (FT_occ_append pre none a.1 a.2).mpr (Or.inl haocc)
              · intro v p hv
                rcases (FT_occ_append pre none v p).mp hv with hold | hnew
                · exact hall v p hold
                · simp at hnew
          | some m =>
              by_cases hlt : m < a.1
              · simp [FT_step, hbest, hrest, hlt]
                constructor
                · exact (FT_occ_append pre (some m) m pre.length).mpr (Or.inr ⟨rfl, rfl⟩)
                constructor
                · exact (FT_occ_append pre (some m) a.1 a.2).mpr (Or.inl haocc)
                constructor
                · exact Or.inl hlt
                constructor
                · intro v p hv
                  rcases (FT_occ_append pre (some m) v p).mp hv with hold | hnew
                  · have hva := hall v p hold
                    have hv1 : v = a.1 := congrArg Prod.fst hva
                    have hp1 : p = a.2 := congrArg Prod.snd hva
                    subst v; subst p
                    exact Or.inl hlt
                  · rcases hnew with ⟨hp, hv⟩
                    simp at hv
                    subst v; subst p
                    exact FT_le_refl _
                · intro v p hv hne
                  rcases (FT_occ_append pre (some m) v p).mp hv with hold | hnew
                  · have hva := hall v p hold
                    have hv1 : v = a.1 := congrArg Prod.fst hva
                    have hp1 : p = a.2 := congrArg Prod.snd hva
                    subst v; subst p
                    exact FT_le_refl a
                  · rcases hnew with ⟨hp, hv⟩
                    simp at hv
                    subst v; subst p
                    exfalso
                    apply hne
                    rfl
              · simp [FT_step, hbest, hrest, hlt]
                have hage : a.2 < pre.length := FT_occ_lt_len haocc
                constructor
                · exact (FT_occ_append pre (some m) a.1 a.2).mpr (Or.inl haocc)
                constructor
                · exact (FT_occ_append pre (some m) m pre.length).mpr (Or.inr ⟨rfl, rfl⟩)
                constructor
                · by_cases heq : a.1 = m
                  · exact Or.inr ⟨heq, by omega⟩
                  · exact Or.inl (by omega)
                constructor
                · intro v p hv
                  rcases (FT_occ_append pre (some m) v p).mp hv with hold | hnew
                  · have hva := hall v p hold
                    have hv1 : v = a.1 := congrArg Prod.fst hva
                    have hp1 : p = a.2 := congrArg Prod.snd hva
                    subst v; subst p
                    exact FT_le_refl a
                  · rcases hnew with ⟨hp, hv⟩
                    simp at hv
                    subst v; subst p
                    by_cases heq : a.1 = m
                    · exact Or.inr ⟨heq, by omega⟩
                    · exact Or.inl (by omega)
                · intro v p hv hne
                  rcases (FT_occ_append pre (some m) v p).mp hv with hold | hnew
                  · have hva := hall v p hold
                    have hv1 : v = a.1 := congrArg Prod.fst hva
                    have hp1 : p = a.2 := congrArg Prod.snd hva
                    subst v; subst p
                    exfalso
                    apply hne
                    rfl
                  · rcases hnew with ⟨hp, hv⟩
                    simp at hv
                    subst v; subst p
                    exact FT_le_refl _
      | cons b rest' =>
          cases hrest' : rest' with
          | nil =>
              simp [FT_inv, hbest, hrest, hrest'] at h
              rcases h with ⟨haocc, hbocc, hab, hmin, hsecond⟩
              cases c with
              | none =>
                  constructor
                  · exact (FT_occ_append pre none a.1 a.2).mpr (Or.inl haocc)
                  constructor
                  · exact (FT_occ_append pre none b.1 b.2).mpr (Or.inl hbocc)
                  constructor
                  · exact hab
                  constructor
                  · intro v p hv
                    rcases (FT_occ_append pre none v p).mp hv with hold | hnew
                    · exact hmin v p hold
                    · simp at hnew
                  · intro v p hv hne
                    rcases (FT_occ_append pre none v p).mp hv with hold | hnew
                    · exact hsecond v p hold hne
                    · simp at hnew
              | some m =>
                  have hage : a.2 < pre.length := FT_occ_lt_len haocc
                  have hbge : b.2 < pre.length := FT_occ_lt_len hbocc
                  by_cases hmalt : m < a.1
                  · simp [FT_step, hbest, hrest, hrest', hmalt]
                    constructor
                    · exact (FT_occ_append pre (some m) m pre.length).mpr (Or.inr ⟨rfl, rfl⟩)
                    constructor
                    · exact (FT_occ_append pre (some m) a.1 a.2).mpr (Or.inl haocc)
                    constructor
                    · exact Or.inl hmalt
                    constructor
                    · intro v p hv
                      rcases (FT_occ_append pre (some m) v p).mp hv with hold | hnew
                      · exact FT_of_lt_le (Or.inl hmalt) (hmin v p hold)
                      · rcases hnew with ⟨hp, hv⟩
                        simp at hv
                        subst v; subst p
                        exact FT_le_refl _
                    · intro v p hv hne
                      rcases (FT_occ_append pre (some m) v p).mp hv with hold | hnew
                      · exact hmin v p hold
                      · rcases hnew with ⟨hp, hv⟩
                        simp at hv
                        subst v; subst p
                        exfalso
                        apply hne
                        rfl
                  · by_cases hmblt : m < b.1
                    · simp [FT_step, hbest, hrest, hrest', hmalt, hmblt]
                      constructor
                      · exact (FT_occ_append pre (some m) a.1 a.2).mpr (Or.inl haocc)
                      constructor
                      · exact (FT_occ_append pre (some m) m pre.length).mpr (Or.inr ⟨rfl, rfl⟩)
                      constructor
                      · by_cases heq : a.1 = m
                        · exact Or.inr ⟨heq, by omega⟩
                        · exact Or.inl (by omega)
                      constructor
                      · intro v p hv
                        rcases (FT_occ_append pre (some m) v p).mp hv with hold | hnew
                        · exact hmin v p hold
                        · rcases hnew with ⟨hp, hv⟩
                          simp at hv
                          subst v; subst p
                          by_cases heq : a.1 = m
                          · exact Or.inr ⟨heq, by omega⟩
                          · exact Or.inl (by omega)
                      · intro v p hv hne
                        rcases (FT_occ_append pre (some m) v p).mp hv with hold | hnew
                        · have hs := hsecond v p hold hne
                          exact FT_of_lt_le (Or.inl hmblt) hs
                        · rcases hnew with ⟨hp, hv⟩
                          simp at hv
                          subst v; subst p
                          exact FT_le_refl _
                    · simp [FT_step, hbest, hrest, hrest', hmalt, hmblt]
                      constructor
                      · exact (FT_occ_append pre (some m) a.1 a.2).mpr (Or.inl haocc)
                      constructor
                      · exact (FT_occ_append pre (some m) b.1 b.2).mpr (Or.inl hbocc)
                      constructor
                      · exact hab
                      constructor
                      · intro v p hv
                        rcases (FT_occ_append pre (some m) v p).mp hv with hold | hnew
                        · exact hmin v p hold
                        · rcases hnew with ⟨hp, hv⟩
                          simp at hv
                          subst v; subst p
                          by_cases heq : a.1 = m
                          · exact Or.inr ⟨heq, by omega⟩
                          · exact Or.inl (by omega)
                      · intro v p hv hne
                        rcases (FT_occ_append pre (some m) v p).mp hv with hold | hnew
                        · exact hsecond v p hold hne
                        · rcases hnew with ⟨hp, hv⟩
                          simp at hv
                          subst v; subst p
                          by_cases heq : b.1 = m
                          · exact Or.inr ⟨heq, by omega⟩
                          · exact Or.inl (by omega)
          | cons d rest'' =>
              simp [FT_inv, hbest, hrest, hrest'] at h



private def FT_fold (xs : List Cell) (n : Nat) (acc : List (Int × Nat)) :=
  (xs.zipIdx n).foldl FT_step acc


private theorem FT_fold_inv (xs : List Cell) (n : Nat) (acc : List (Int × Nat)) (pre : List Cell)
    (hlen : pre.length = n) (h : FT_inv pre acc) :
    FT_inv (pre ++ xs) (FT_fold xs n acc) := by
  induction xs generalizing n acc pre with
  | nil =>
      simpa [FT_fold] using h
  | cons c xs ih =>
      have hstep := FT_step_inv pre acc c h
      have hstep' : FT_inv (pre ++ [c]) (FT_step acc (c, n)) := by simpa [hlen] using hstep
      have hih := ih (n+1) (FT_step acc (c,n)) (pre++[c]) (by simp [hlen]) hstep'
      simpa [FT_fold, List.zipIdx_cons, List.append_assoc] using hih

private theorem FT_firstTwo_inv (xs : List Cell) : FT_inv xs (firstTwo xs) := by
  exact FT_fold_inv xs 0 [] [] rfl (by intro v p; simp [FT_occ, FT_inv])

private theorem mem_excluding (xs : List Cell) (i : Nat) (w : Int) :
    w ∈ excluding xs i ↔ ∃ p, FT_occ xs w p ∧ p ≠ i := by
  induction xs generalizing i with
  | nil => simp [excluding, FT_occ]
  | cons c cs ih =>
      cases i with
      | zero =>
          constructor
          · intro hw
            have hsm : some w ∈ cs := by
              simpa [excluding] using hw
            rcases (mem_iff_occ cs w).mp hsm with ⟨p, hp⟩
            refine ⟨p+1, ?_, by omega⟩
            simpa [FT_occ] using hp
          · rintro ⟨p, hp, hp0⟩
            have hp_pos : 0 < p := Nat.pos_of_ne_zero hp0
            rcases p with _ | q
            · contradiction
            · have hcs : FT_occ cs w q := by simpa [FT_occ] using hp
              have hsm := (mem_iff_occ cs w).mpr ⟨q, hcs⟩
              simpa [excluding] using hsm
      | succ i =>
          have ih' := ih i
          constructor
          · intro hw
            have hw' : some w ∈ c :: (cs.take i ++ cs.drop (i+1)) := by
              simpa [excluding] using hw
            rcases List.mem_cons.mp hw' with hcw | hwmem
            · refine ⟨0, ?_, by omega⟩
              simpa [FT_occ] using hcw.symm
            · have hw_excl : w ∈ excluding cs i := by
                simpa [excluding] using hwmem
              rcases ih'.mp hw_excl with ⟨p, hp, hpne⟩
              refine ⟨p+1, ?_, by omega⟩
              simpa [FT_occ] using hp
          · rintro ⟨p, hp, hpne⟩
            rcases p with _ | q
            · have hcw : c = some w := by simpa [FT_occ] using hp
              have hw' : some w ∈ c :: (cs.take i ++ cs.drop (i+1)) := by simp [hcw]
              simpa [excluding] using hw'
            · have hcs : FT_occ cs w q := by simpa [FT_occ] using hp
              have hqne : q ≠ i := by omega
              have hw_excl : w ∈ excluding cs i := ih'.mpr ⟨q, hcs, hqne⟩
              have hw' : some w ∈ c :: (cs.take i ++ cs.drop (i+1)) := by
                apply List.mem_cons_of_mem
                simpa [excluding] using hw_excl
              simpa [excluding] using hw'

private theorem foldl_min_is_min (l : List Int) (a : Int) :
    l.foldl min a ∈ a :: l ∧ ∀ b ∈ a :: l, l.foldl min a ≤ b := by
  induction l generalizing a with
  | nil => simp
  | cons c cs ih =>
      rw [List.foldl_cons]
      have h := ih (min a c)
      rcases h with ⟨hmem, hle⟩
      constructor
      · rcases List.mem_cons.mp hmem with heq | hmem
        · have hmc : min a c = a ∨ min a c = c := by omega
          rcases hmc with hmc | hmc
          · have heq' : List.foldl min (min a c) cs = a := heq.trans hmc
            simp [heq']
          · have heq' : List.foldl min (min a c) cs = c := heq.trans hmc
            simp [heq']
        · exact List.mem_cons_of_mem a (List.mem_cons_of_mem c hmem)
      · intro b hb
        have hacc : List.foldl min (min a c) cs ≤ min a c := hle (min a c) (by simp)
        rcases List.mem_cons.mp hb with hba | hb
        · subst b
          omega
        · rcases List.mem_cons.mp hb with hbc | hb
          · subst b
            omega
          · exact hle b (List.mem_cons_of_mem _ hb)

private theorem min?_eq_some_of_mem_le {l : List Int} {m : Int}
    (hm : m ∈ l) (hle : ∀ w ∈ l, m ≤ w) : l.min? = some m := by
  cases l with
  | nil => simp at hm
  | cons a as =>
      simp only [List.min?]
      have h := foldl_min_is_min as a
      have hm_le : m ≤ as.foldl min a := hle _ h.1
      have hle_m : as.foldl min a ≤ m := h.2 m hm
      have heq : as.foldl min a = m := by omega
      rw [heq]

private theorem excl_min_of_value (xs : List Cell) (i : Nat) (v : Int)
    (hvin : v ∈ excluding xs i) (hle : ∀ w ∈ excluding xs i, v ≤ w) :
    (excluding xs i).min? = some v := min?_eq_some_of_mem_le hvin hle

theorem ExcludingMinimum_sound : TinylambOpt.ResearchGoals.CorrelatedProbes.ExcludingMinimum.Goal := by
  intro xs i
  have h := FT_firstTwo_inv xs
  cases hb : firstTwo xs with
  | nil =>
      have hinv : FT_inv xs [] := by simpa [hb] using h
      simp [FT_inv] at hinv
      have hexcl : excluding xs i = [] := by
        apply List.eq_nil_iff_forall_not_mem.mpr
        intro w hw
        rcases (mem_excluding xs i w).mp hw with ⟨p, hp, _⟩
        exact hinv w p hp
      simp [hb, hexcl]
  | cons a rest =>
      cases hrest : rest with
      | nil =>
          have hinv : FT_inv xs [a] := by simpa [hb, hrest] using h
          simp [FT_inv] at hinv
          rcases hinv with ⟨haocc, hall⟩
          by_cases hp : a.2 ≠ i
          · have hpbool : (a.2 != i) = true := by simpa using hp
            simp [hb, hrest, hpbool]
            symm
            apply excl_min_of_value
            · apply (mem_excluding xs i a.1).mpr
              exact ⟨a.2, haocc, hp⟩
            · intro w hw
              rcases (mem_excluding xs i w).mp hw with ⟨p, hpocc, hpne⟩
              have hva := hall w p hpocc
              have hv1 : w = a.1 := congrArg Prod.fst hva
              have hpp : p = a.2 := congrArg Prod.snd hva
              omega
          · have hpbool : (a.2 != i) = false := by simpa using hp
            simp [hb, hrest, hpbool]
            have hexcl : excluding xs i = [] := by
              apply List.eq_nil_iff_forall_not_mem.mpr
              intro w hw
              rcases (mem_excluding xs i w).mp hw with ⟨p, hpocc, hpne⟩
              have hva := hall w p hpocc
              have hpp : p = a.2 := congrArg Prod.snd hva
              exact hpne (by omega)
            simp [hexcl]
      | cons c rest' =>
          cases hrest' : rest' with
          | nil =>
              have hinv : FT_inv xs [a, c] := by simpa [hb, hrest, hrest'] using h
              simp [FT_inv] at hinv
              rcases hinv with ⟨haocc, hcocc, hab, hmin, hsecond⟩
              have hcpos : c.2 ≠ a.2 := by
                intro heq
                have hval : c.1 = a.1 := FT_occ_unique hcocc (by simpa [heq] using haocc)
                rcases hab with hlt | ⟨heqv, hltp⟩
                · omega
                · omega
              by_cases hp : a.2 ≠ i
              · have hpbool : (a.2 != i) = true := by simpa using hp
                simp [hb, hrest, hrest', hpbool]
                symm
                apply excl_min_of_value
                · apply (mem_excluding xs i a.1).mpr
                  exact ⟨a.2, haocc, hp⟩
                · intro w hw
                  rcases (mem_excluding xs i w).mp hw with ⟨p, hpocc, hpne⟩
                  have hle := hmin w p hpocc
                  rcases hle with hlt | ⟨heqv, _⟩
                  · omega
                  · omega
              · have hpbool : (a.2 != i) = false := by simpa using hp
                have hci : c.2 ≠ i := by
                  intro hcqi
                  have hai : a.2 = i := by omega
                  exact hcpos (by omega)
                have hpboolc : (c.2 != i) = true := by simpa using hci
                simp [hb, hrest, hrest', hpbool, hpboolc]
                symm
                apply excl_min_of_value
                · apply (mem_excluding xs i c.1).mpr
                  exact ⟨c.2, hcocc, hci⟩
                · intro w hw
                  rcases (mem_excluding xs i w).mp hw with ⟨p, hpocc, hpne⟩
                  have hne : (w, p) ≠ a := by
                    intro heq
                    have hpp : p = a.2 := congrArg Prod.snd heq
                    exact hpne (by omega)
                  have hle := hsecond w p hpocc hne
                  rcases hle with hlt | ⟨heqv, _⟩
                  · omega
                  · omega
          | cons d rest'' =>
              have hinv : FT_inv xs (a :: c :: d :: rest'') := by simpa [hb, hrest, hrest'] using h
              simp [FT_inv] at hinv


private theorem any_false {α : Type} (l : List α) : l.any (fun _ => false) = false := by
  induction l with
  | nil => rfl
  | cons a l ih => simp [ih]

private def W_stepB (acc : List Int × Bool) (v : Cell) : List Int × Bool :=
  match v with
  | none => (acc.1, true)
  | some n => if decide (acc.1.length < 2) && !acc.1.contains n then (n::acc.1, acc.2) else acc

private def W_step (acc : List Int × Bool) (v : Cell) : List Int × Bool :=
  match v with
  | none => (acc.1, true)
  | some n => if acc.1.length < 2 ∧ n ∉ acc.1 then (n::acc.1, acc.2) else acc

private theorem W_cond_eq (vs : List Int) (n : Int) :
    (decide (vs.length < 2) && !vs.contains n) = true ↔ (vs.length < 2 ∧ n ∉ vs) := by
  constructor
  · intro h
    have h' : (decide (vs.length < 2) = true) ∧ (!vs.contains n = true) := by
      rw [← Bool.and_eq_true]
      exact h
    refine ⟨of_decide_eq_true h'.1, ?_⟩
    intro hm
    have hc : vs.contains n = true := D_contains_iff_mem_general.mpr hm
    rw [hc] at h'
    cases h'.2
  · intro h
    have hcn : vs.contains n = false := by
      cases hcc : vs.contains n with
      | false => rfl
      | true => exact False.elim (h.2 (D_contains_iff_mem_general.mp hcc))
    rw [Bool.and_eq_true]
    exact ⟨decide_eq_true h.1, by simpa using h.2⟩

private theorem W_step_eq (acc : List Int × Bool) (v : Cell) : W_step acc v = W_stepB acc v := by
  cases v with
  | none => rfl
  | some n =>
      change (if acc.1.length < 2 ∧ n ∉ acc.1 then (n::acc.1, acc.2) else acc) =
        (if decide (acc.1.length < 2) && !acc.1.contains n then (n::acc.1, acc.2) else acc)
      by_cases h : acc.1.length < 2 ∧ n ∉ acc.1
      · rw [if_pos h]
        rw [if_pos ((W_cond_eq acc.1 n).mpr h)]
      · rw [if_neg h]
        rw [if_neg (by
          intro hc
          exact h ((W_cond_eq acc.1 n).mp hc))]

private theorem W_fold_eq_acc (ys : List Cell) (acc : List Int × Bool) :
    ys.foldl W_stepB acc = ys.foldl W_step acc := by
  induction ys generalizing acc with
  | nil => rfl
  | cons v ys ih =>
      rw [List.foldl_cons, List.foldl_cons, W_step_eq]
      exact ih _

private theorem W_fold_eq (ys : List Cell) :
    ys.foldl W_stepB ([], false) = ys.foldl W_step ([], false) := W_fold_eq_acc ys ([], false)

private theorem witnesses_eq (ys : List Cell) :
    witnesses ys = (ys.foldl W_step ([], false)).1.map some ++
      (if (ys.foldl W_step ([], false)).2 then [none] else []) := by
  unfold witnesses
  change (((ys.foldl W_stepB ([], false)).1.map some ++
      (if (ys.foldl W_stepB ([], false)).2 then [none] else [])) = _)
  rw [W_fold_eq]

private theorem W_summary_anyNe (acc : List Int × Bool) (pre : List Cell) (x : Cell)
    (hnd : acc.1.Nodup)
    (hsub : ∀ n ∈ acc.1, some n ∈ pre)
    (hfull : ∀ n, some n ∈ pre → n ∈ acc.1 ∨ acc.1.length = 2)
    (hnull : acc.2 = true ↔ none ∈ pre) :
    anyNe x (acc.1.map some ++ (if acc.2 then [none] else [])) = anyNe x pre := by
  have hS_none : none ∈ (acc.1.map some ++ (if acc.2 then [none] else [])) ↔ none ∈ pre := by
    constructor
    · intro h
      have hm := List.mem_append.mp h
      have hnone_tail : (none : Cell) ∈ (if acc.2 then [none] else []) := by
        rcases hm with hhead | htail
        · rcases List.mem_map.mp hhead with ⟨n, hn, heq⟩
          simp at heq
        · exact htail
      have hb : acc.2 = true := by cases hacc2 : acc.2 <;> simp [hacc2] at hnone_tail ⊢
      exact hnull.mp hb
    · intro h
      have hb := hnull.mpr h
      simp [hb]
  have hS_nil : (acc.1.map some ++ (if acc.2 then [none] else [])) = [] ↔ pre = [] := by
    constructor
    · intro hS
      have hacc1 : acc.1 = [] := by
        cases hacc : acc.1 with
        | nil => rfl
        | cons n t =>
            have : some n ∈ acc.1.map some ++ (if acc.2 then [none] else []) := by simp [hacc]
            simp [hS] at this
      have hb : acc.2 = false := by
        cases hacc2 : acc.2 <;> simp [hacc1, hacc2] at hS ⊢
      have hnoSome : ∀ n, some n ∉ pre := by
        intro n hn
        rcases hfull n hn with hnT | hlen2
        · exact (by simp [hacc1] at hnT)
        · exact (by simp [hacc1] at hlen2)
      have hnoNone : none ∉ pre := by
        intro hn
        have hb' : acc.2 = true := hnull.mpr hn
        rw [hb] at hb'
        cases hb'
      cases hpre : pre with
      | nil => rfl
      | cons z zs =>
          cases z with
          | none => exact absurd (by simp [hpre]) hnoNone
          | some n => exact absurd (by simp [hpre]) (hnoSome n)
    · intro hpre
      have hacc1 : acc.1 = [] := by
        cases hacc : acc.1 with
        | nil => rfl
        | cons n t =>
            have hn := hsub n (by simp [hacc])
            simp [hpre] at hn
      have hb : acc.2 = false := by
        cases hacc2 : acc.2 with
        | false => rfl
        | true =>
            exfalso
            have hn : none ∈ pre := hnull.mp hacc2
            simpa [hpre] using hn
      simp [hacc1, hb]
  have hmemS_some : ∀ n, some n ∈ (acc.1.map some ++ (if acc.2 then [none] else [])) ↔ n ∈ acc.1 := by
    intro n
    simp
  have hAny : (acc.1.map some ++ (if acc.2 then [none] else [])).any (fun y => x.isSome && y.isSome && x != y) =
      pre.any (fun y => x.isSome && y.isSome && x != y) := by
    cases x with
    | none =>
        have hpred : (fun y : Cell => (none : Cell).isSome && y.isSome && (none : Cell) != y) =
            (fun _ : Cell => false) := by funext y; simp
        rw [hpred, any_false, any_false]
    | some v =>
        simp only [Option.isSome_some, Bool.true_and]
        apply Bool.eq_iff_iff.mpr
        constructor
        · intro h
          rcases List.any_eq_true.mp h with ⟨y, hyS, hycond⟩
          have hycond' : y.isSome = true ∧ (some v != y) = true := by
            simpa only [Bool.and_eq_true] using hycond
          rcases hycond' with ⟨hys, hvny⟩
          rcases y with _ | n
          · simp at hys
          · have hnT : n ∈ acc.1 := (hmemS_some n).mp hyS
            have hvn : v ≠ n := by simpa using hvny
            apply List.any_eq_true.mpr
            refine ⟨some n, hsub n hnT, ?_⟩
            simp [hvn]
        · intro h
          rcases List.any_eq_true.mp h with ⟨y, hypre, hycond⟩
          have hycond' : y.isSome = true ∧ (some v != y) = true := by
            simpa only [Bool.and_eq_true] using hycond
          rcases hycond' with ⟨hys, hvny⟩
          rcases y with _ | n
          · simp at hys
          · have hvn : v ≠ n := by simpa using hvny
            rcases hfull n (by simpa using hypre) with hnT | hlen2
            · apply List.any_eq_true.mpr
              refine ⟨some n, (hmemS_some n).mpr hnT, ?_⟩
              simp [hvn]
            · have htex : ∃ w ∈ acc.1, w ≠ v := by
                cases hacc : acc.1 with
                | nil => simp [hacc] at hlen2
                | cons a t =>
                    cases ht : t with
                    | nil => simp [hacc, ht] at hlen2
                    | cons b t' =>
                        cases ht' : t' with
                        | nil =>
                            have hab : a ≠ b := by
                              have hnod := hnd
                              simpa [hacc, ht, ht'] using hnod
                            by_cases hav : a = v
                            · refine ⟨b, by simp [hacc, ht, ht'], ?_⟩
                              intro hbv
                              apply hab
                              rw [hav, hbv]
                            · exact ⟨a, by simp [hacc, ht, ht'], hav⟩
                        | cons c t'' =>
                            simp [hacc, ht, ht'] at hlen2
              rcases htex with ⟨w, hwT, hwv⟩
              have hvw : v ≠ w := by intro h'; exact hwv h'.symm
              apply List.any_eq_true.mpr
              refine ⟨some w, (hmemS_some w).mpr hwT, ?_⟩
              simp [hvw]
  have hEmpty : (acc.1.map some ++ (if acc.2 then [none] else [])).isEmpty = pre.isEmpty := by
    apply Bool.eq_iff_iff.mpr
    rw [List.isEmpty_iff, List.isEmpty_iff]
    exact hS_nil
  have hCont : (acc.1.map some ++ (if acc.2 then [none] else [])).contains none = pre.contains none := by
    apply Bool.eq_iff_iff.mpr
    constructor
    · intro h
      exact D_contains_iff_mem_general.mpr (hS_none.mp (D_contains_iff_mem_general.mp h))
    · intro h
      exact D_contains_iff_mem_general.mpr (hS_none.mpr (D_contains_iff_mem_general.mp h))
  unfold anyNe
  rw [hAny, hEmpty, hCont]

private theorem W_step_inv (acc : List Int × Bool) (pre : List Cell) (v : Cell)
    (hnd : acc.1.Nodup) (hlen : acc.1.length ≤ 2)
    (hsub : ∀ n ∈ acc.1, some n ∈ pre)
    (hfull : ∀ n, some n ∈ pre → n ∈ acc.1 ∨ acc.1.length = 2)
    (hnull : acc.2 = true ↔ none ∈ pre) :
    (W_step acc v).1.Nodup ∧ (W_step acc v).1.length ≤ 2 ∧
      (∀ n ∈ (W_step acc v).1, some n ∈ pre ++ [v]) ∧
      (∀ n, some n ∈ pre ++ [v] → n ∈ (W_step acc v).1 ∨ (W_step acc v).1.length = 2) ∧
      ((W_step acc v).2 = true ↔ none ∈ pre ++ [v]) := by
  cases v with
  | none =>
      dsimp [W_step]
      change acc.1.Nodup ∧ acc.1.length ≤ 2 ∧
        (∀ n ∈ acc.1, some n ∈ pre ++ [none]) ∧
        (∀ n, some n ∈ pre ++ [none] → n ∈ acc.1 ∨ acc.1.length = 2) ∧
        (true = true ↔ none ∈ pre ++ [none])
      refine ⟨hnd, hlen, ?_, ?_, ?_⟩
      · intro n hn; exact List.mem_append.mpr (Or.inl (hsub n hn))
      · intro n hn
        have hn' : some n ∈ pre := by
          rcases List.mem_append.mp hn with hnpre | hnlast
          · exact hnpre
          · simp at hnlast
        exact hfull n hn'
      · constructor
        · intro _; simp
        · intro _; rfl
  | some m =>
      simp only [W_step]
      by_cases h : acc.1.length < 2 ∧ m ∉ acc.1
      · rw [if_pos h]
        refine ⟨?_, ?_, ?_, ?_, ?_⟩
        · exact List.nodup_cons.mpr ⟨h.2, hnd⟩
        · simp
          omega
        · intro n hn
          simp only [List.mem_cons] at hn
          rcases hn with rfl | hn
          · simp
          · exact List.mem_append.mpr (Or.inl (hsub n hn))
        · intro n hn
          by_cases hnm : n = m
          · subst n
            simp
          · have hn' : some n ∈ pre := by
              rcases List.mem_append.mp hn with hnpre | hnlast
              · exact hnpre
              · simp at hnlast
                exact False.elim (hnm hnlast)
            have hnacc : n ∈ acc.1 := by
              rcases hfull n hn' with hacc | hlen2
              · exact hacc
              · omega
            left
            simp [hnacc]
        · simpa using hnull
      · rw [if_neg h]
        have hmfull : m ∈ acc.1 ∨ acc.1.length = 2 := by
          by_cases hlt : acc.1.length < 2
          · left
            by_cases hm : m ∈ acc.1
            · exact hm
            · exact False.elim (h ⟨hlt, hm⟩)
          · right
            omega
        refine ⟨hnd, hlen, ?_, ?_, ?_⟩
        · intro n hn
          exact List.mem_append.mpr (Or.inl (hsub n hn))
        · intro n hn
          by_cases hnm : n = m
          · subst n
            exact hmfull
          · have hn' : some n ∈ pre := by
              rcases List.mem_append.mp hn with hnpre | hnlast
              · exact hnpre
              · simp at hnlast
                exact False.elim (hnm hnlast)
            rcases hfull n hn' with hacc | hlen2
            · exact Or.inl hacc
            · exact Or.inr hlen2
        · simpa using hnull

private theorem W_fold_len (ys : List Cell) (acc : List Int × Bool) (h : acc.1.length ≤ 2) :
    (ys.foldl W_step acc).1.length ≤ 2 := by
  induction ys generalizing acc with
  | nil => simpa using h
  | cons v ys ih =>
      rw [List.foldl_cons]
      apply ih
      cases v with
      | none => simpa [W_step] using h
      | some m =>
          simp only [W_step]
          by_cases hh : acc.1.length < 2 ∧ m ∉ acc.1
          · rw [if_pos hh]
            simp
            omega
          · rw [if_neg hh]
            exact h

private theorem W_fold_anyNe (acc : List Int × Bool) (pre ys : List Cell) (x : Cell)
    (hnd : acc.1.Nodup) (hlen : acc.1.length ≤ 2)
    (hsub : ∀ n ∈ acc.1, some n ∈ pre)
    (hfull : ∀ n, some n ∈ pre → n ∈ acc.1 ∨ acc.1.length = 2)
    (hnull : acc.2 = true ↔ none ∈ pre) :
    anyNe x (((ys.foldl W_step acc).1.map some) ++ (if (ys.foldl W_step acc).2 then [none] else []))
      = anyNe x (pre ++ ys) := by
  induction ys generalizing acc pre with
  | nil =>
      rw [List.foldl_nil, List.append_nil]
      exact W_summary_anyNe acc pre x hnd hsub hfull hnull
  | cons v ys ih =>
      rw [List.foldl_cons]
      have hinv := W_step_inv acc pre v hnd hlen hsub hfull hnull
      rcases hinv with ⟨hnd', hlen', hsub', hfull', hnull'⟩
      have hih := ih (W_step acc v) (pre ++ [v]) hnd' hlen' hsub' hfull' hnull'
      simpa [List.append_assoc] using hih

theorem NeWitnesses_sound : TinylambOpt.ResearchGoals.CorrelatedProbes.NeWitnesses.Goal := by
  intro ys x
  constructor
  · have hlen := W_fold_len ys ([], false) (by simp)
    rw [witnesses_eq]
    cases hb : (ys.foldl W_step ([], false)).2 <;> simp [hb] <;> omega
  · rw [witnesses_eq]
    simpa using (W_fold_anyNe ([], false) [] ys x
      (by simp) (by simp) (by simp) (by simp) (by simp))


end TinylambOpt.ResearchProofs.ExecutionStrategies.CorrelatedProbes

-- C++ references to this model or its proof.
-- executor/detail/execution_strategies.hpp
-- executor/detail/execution_strategies_test.cpp
