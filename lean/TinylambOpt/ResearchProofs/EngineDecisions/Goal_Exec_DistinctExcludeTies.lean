import TinylambOpt.ResearchGoals.EngineDecisions.Executor

/-!
These models compare concrete implementation choices in planning, execution,
concurrency, or recovery against their stated observable contracts.

Named results include `nodup_filter`, `nodup_eraseDups`, `nodup_append_of_disjoint`.
-/
set_option maxHeartbeats 0
set_option maxRecDepth 10000
set_option linter.unusedSimpArgs false
set_option linter.unusedVariables false
namespace TinylambOpt.ResearchProofs.EngineDecisions.Exec_DistinctExcludeTies

open TinylambOpt.ResearchGoals.EngineDecisions.Executor.DistinctExcludeTies

theorem nodup_filter {α : Type} (p : α → Bool) {l : List α} (h : l.Nodup) :
    (l.filter p).Nodup := by
  induction l with
  | nil => simp
  | cons a as ih =>
      by_cases hp : p a = true
      · have h' := List.nodup_cons.mp h
        have hnot : ¬ a ∈ List.filter p as := by
          intro hm
          exact h'.1 (List.mem_filter.mp hm).1
        simpa [List.filter_cons_of_pos hp, List.nodup_cons] using And.intro hnot (ih h'.2)
      · simpa [List.filter_cons_of_neg hp] using ih (List.nodup_cons.mp h).2

theorem nodup_eraseDups {α : Type} [BEq α] [LawfulBEq α] : ∀ l : List α, l.eraseDups.Nodup
  | [] => by simp
  | a :: as => by
      rw [List.eraseDups_cons, List.nodup_cons]
      constructor
      · intro ha
        rw [List.mem_eraseDups] at ha
        simp at ha
      · exact nodup_eraseDups (as.filter fun b => ! b == a)
termination_by l => l.length
decreasing_by
  simp only [List.length_cons]
  have hle := List.length_filter_le (fun b => ! b == a) as
  omega

theorem nodup_append_of_disjoint {α : Type} {l₁ l₂ : List α}
    (h₁ : l₁.Nodup) (h₂ : l₂.Nodup) (hd : ∀ x, x ∈ l₁ → x ∈ l₂ → False) :
    (l₁ ++ l₂).Nodup := by
  induction l₁ with
  | nil => simpa using h₂
  | cons a as ih =>
      change (a :: (as ++ l₂)).Nodup
      have hn' := List.nodup_cons.mp h₁
      rw [List.nodup_cons]
      constructor
      · intro ha2
        have ha2' : a ∈ l₂ := by
          rcases List.mem_append.mp ha2 with ha | ha
          · exact False.elim (hn'.1 ha)
          · exact ha
        exact hd a (by simp) ha2'
      · apply ih hn'.2
        intro x hx hx2
        exact hd x (by simp [hx]) hx2

theorem not_mem_erase_self_of_nodup {α : Type} [BEq α] [LawfulBEq α] {a : α} {l : List α}
    (hn : l.Nodup) : a ∉ l.erase a := by
  induction l with
  | nil => simp
  | cons b bs ih =>
      by_cases hba : b == a
      · have hb : b = a := beq_iff_eq.mp hba
        subst b
        have hn' := List.nodup_cons.mp hn
        simpa [List.erase_cons_head] using hn'.1
      · have hne : a ≠ b := by
          intro h
          subst b
          simp at hba
        have hn' := List.nodup_cons.mp hn
        rw [List.erase_cons_tail hba]
        intro hmem
        rcases List.mem_cons.mp hmem with hab | hmem
        · exact hne hab
        · exact ih hn'.2 hmem

theorem nodup_erase {α : Type} [BEq α] [LawfulBEq α] {a : α} {l : List α}
    (hn : l.Nodup) : (l.erase a).Nodup := by
  induction l with
  | nil => simp
  | cons b bs ih =>
      by_cases hba : b == a
      · have hb : b = a := beq_iff_eq.mp hba
        subst b
        have hn' := List.nodup_cons.mp hn
        simpa [List.erase_cons_head] using hn'.2
      · have hn' := List.nodup_cons.mp hn
        rw [List.erase_cons_tail hba, List.nodup_cons]
        constructor
        · intro hbmem
          exact hn'.1 (List.mem_of_mem_erase hbmem)
        · exact ih hn'.2

theorem nodup_perm_ext {α : Type} [BEq α] [LawfulBEq α] (l₁ l₂ : List α)
    (hnd₁ : l₁.Nodup) (hnd₂ : l₂.Nodup) (hmem : ∀ x, x ∈ l₁ ↔ x ∈ l₂) : l₁.Perm l₂ := by
  induction l₁ generalizing l₂ with
  | nil =>
      have hl2 : l₂ = [] := by
        cases l₂ with
        | nil => rfl
        | cons b bs =>
            have : b ∈ b :: bs := by simp
            have : b ∈ ([] : List α) := (hmem b).mpr this
            simp at this
      subst l₂
      exact List.Perm.nil
  | cons a as ih =>
      rw [List.cons_perm_iff_perm_erase]
      constructor
      · exact (hmem a).mp (by simp)
      · have hnd' := List.nodup_cons.mp hnd₁
        apply ih (l₂.erase a) hnd'.2
        · exact nodup_erase hnd₂
        · intro x
          constructor
          · intro hx
            have hxa : x ≠ a := by
              intro h; subst x; exact hnd'.1 hx
            have hx2 : x ∈ l₂ := (hmem x).mp (by simp [hx])
            simpa [List.mem_erase_of_ne hxa] using hx2
          · intro hx
            have hxa : x ≠ a := by
              intro h; subst x
              exact not_mem_erase_self_of_nodup hnd₂ hx
            have hx2 : x ∈ l₂ := List.mem_of_mem_erase hx
            have hx1 : x ∈ a :: as := (hmem x).mpr hx2
            rcases List.mem_cons.mp hx1 with rfl | hxas
            · contradiction
            · exact hxas

theorem eraseDups_append_perm {α : Type} [BEq α] [LawfulBEq α] (P N : List α) :
    (P ++ N).eraseDups.Perm (P.eraseDups.filter (fun x => x ∉ N) ++ N.eraseDups) := by
  have hnd2 : (P.eraseDups.filter (fun x => x ∉ N) ++ N.eraseDups).Nodup := by
    apply nodup_append_of_disjoint
    · exact nodup_filter (fun x => x ∉ N) (nodup_eraseDups P)
    · exact nodup_eraseDups N
    · intro x hxP hxN
      have hnot : x ∉ N := by simpa using (List.mem_filter.mp hxP).2
      exact hnot (List.mem_eraseDups.mp hxN)
  apply nodup_perm_ext (P ++ N).eraseDups (P.eraseDups.filter (fun x => x ∉ N) ++ N.eraseDups)
    (nodup_eraseDups (P ++ N)) hnd2
  intro x
  simp [List.mem_eraseDups]
  constructor
  · intro h
    rcases h with h | h
    · by_cases hxN : x ∈ N
      · exact Or.inr hxN
      · exact Or.inl ⟨h, hxN⟩
    · exact Or.inr h
  · intro h
    rcases h with h | h
    · exact Or.inl h.1
    · exact Or.inr h

theorem eraseDups_length_perm {α : Type} [BEq α] [LawfulBEq α] {l₁ l₂ : List α}
    (h : l₁.Perm l₂) : l₁.eraseDups.length = l₂.eraseDups.length := by
  have hnd1 : l₁.eraseDups.Nodup := nodup_eraseDups l₁
  have hm : ∀ x, x ∈ l₁.eraseDups ↔ x ∈ l₂.eraseDups := by
    intro x
    simp [List.mem_eraseDups]
    constructor
    · intro hx
      exact h.subset hx
    · intro hx
      exact h.symm.subset hx
  exact (nodup_perm_ext l₁.eraseDups l₂.eraseDups hnd1 (nodup_eraseDups l₂) hm).length_eq

theorem eraseDups_append_length {α : Type} [BEq α] [LawfulBEq α] (P N : List α) :
    (P ++ N).eraseDups.length = (P.eraseDups.filter (fun x => x ∉ N)).length + N.eraseDups.length := by
  simpa using (eraseDups_append_perm P N).length_eq

theorem perm_append_comm {α : Type} (l₁ l₂ : List α) : (l₁ ++ l₂).Perm (l₂ ++ l₁) := by
  induction l₁ with
  | nil => simp
  | cons a as ih =>
      exact (ih.cons a).trans (List.perm_middle (a := a) (l₁ := l₂) (l₂ := as)).symm

theorem length_filter_ne_add_count {α : Type} [BEq α] (a : α) (l : List α) :
    (l.filter (fun x => ! x == a)).length + l.count a = l.length := by
  induction l with
  | nil => simp
  | cons b bs ih =>
      by_cases hba : b == a <;> simp [List.count_cons, hba] <;> omega

theorem filter_split_perm {α : Type} (p : α → Bool) (l : List α) :
    l.Perm (l.filter p ++ l.filter (fun x => ! p x)) := by
  induction l with
  | nil => simp
  | cons a as ih =>
      by_cases hp : p a = true
      · simpa [hp] using ih.cons a
      · have hp' : p a = false := by cases hpa : p a <;> simp_all
        simpa [hp'] using (List.perm_cons_append_cons a ih)

theorem perm_singleton_of_nodup_all_eq {α : Type} [BEq α] [LawfulBEq α] {l : List α} {v : α}
    (hn : l.Nodup) (hm : ∃ x, x ∈ l) (hall : ∀ x ∈ l, x = v) : l.Perm [v] := by
  apply nodup_perm_ext l [v] hn (by simp)
  intro x
  constructor
  · intro hx
    have hxv := hall x hx
    simp [hxv]
  · intro hx
    have hxv : x = v := by simpa using hx
    obtain ⟨y, hy⟩ := hm
    have hyv := hall y hy
    subst x
    simpa [hyv] using hy

theorem filter_not_mem_length_of_perm_singleton {α : Type} [BEq α] [LawfulBEq α]
    {l : List α} {v : α} {N : List α} (hp : l.eraseDups.Perm [v]) :
    (l.eraseDups.filter (fun x => ! decide (x ∈ N))).length = if v ∈ N then 0 else 1 := by
  have hlen := (hp.filter (fun x => ! decide (x ∈ N))).length_eq
  by_cases hvN : v ∈ N
  · have hf : (List.filter (fun x => x ∉ N) [v]) = [] := by simp [hvN]
    simpa [hf, hvN] using hlen
  · have hf : (List.filter (fun x => x ∉ N) [v]) = [v] := by simp [hvN]
    simpa [hf, hvN] using hlen

theorem sound : Goal := by
  intro frame current huniq
  let peers := frame.filter (fun r => r.peer == current.peer)
  let nonpeers := frame.filter (fun r => ! r.peer == current.peer)
  let P := peers.filterMap R.value
  let N := nonpeers.filterMap R.value
  let A := frame.filterMap R.value
  let E := P.eraseDups.filter (fun v => P.count v == A.count v)
  have hs : frame.Perm (peers ++ nonpeers) := by
    dsimp [peers, nonpeers]
    exact filter_split_perm (fun r : R => r.peer == current.peer) frame
  have hAperm : A.Perm (P ++ N) := by
    dsimp [A, P, N]
    simpa [List.filterMap_append] using hs.filterMap R.value
  have hE : E = P.eraseDups.filter (fun v => v ∉ N) := by
    dsimp [E]
    apply List.filter_congr
    intro v hv
    have hc : A.count v = P.count v + N.count v := by
      rw [hAperm.count v, List.count_append]
    by_cases hvN : v ∈ N
    · have hn0 : N.count v ≠ 0 := by
        intro hz
        exact (List.count_eq_zero.mp hz) hvN
      have hne : P.count v ≠ A.count v := by omega
      simp [hne, hvN]
    · have hn0 : N.count v = 0 := List.count_eq_zero.mpr hvN
      simp [hc, hn0, hvN]
  have hAcard : A.eraseDups.length = E.length + N.eraseDups.length := by
    rw [hE]
    calc
      A.eraseDups.length = (P ++ N).eraseDups.length := eraseDups_length_perm hAperm
      _ = (P.eraseDups.filter (fun v => v ∉ N)).length + N.eraseDups.length :=
        eraseDups_append_length P N
  let kept := frame.filter (fun r => r.peer != current.peer || r.id == current.id)
  let qpeers := peers.filter (fun r => r.id == current.id)
  let V := qpeers.filterMap R.value
  let keep := fun r : R => r.peer != current.peer || r.id == current.id
  have hpeers : peers.filter keep = qpeers := by
    change peers.filter keep = peers.filter (fun r => r.id == current.id)
    apply List.filter_congr
    intro r hr
    have hp : (r.peer == current.peer) = true := (List.mem_filter.mp hr).2
    have hpeq : r.peer = current.peer := Nat.eq_of_beq_eq_true hp
    by_cases hid : (r.id == current.id) = true
    · simp [keep, hpeq, hid]
    · simp [keep, hpeq, hid]
  have hnon : nonpeers.filter keep = nonpeers := by
    dsimp [nonpeers]
    apply List.filter_eq_self.mpr
    intro r hr
    have hp : (! r.peer == current.peer) = true := (List.mem_filter.mp hr).2
    have hpne : (r.peer != current.peer) = true := by simpa [Bool.not_eq_true] using hp
    simp [keep, hpne]
  have hk : kept.Perm (nonpeers ++ qpeers) := by
    have hs' := hs.filter keep
    have hs'' : kept.Perm (qpeers ++ nonpeers) := by
      simpa [kept, keep, List.filter_append, hpeers, hnon] using hs'
    exact hs''.trans (perm_append_comm qpeers nonpeers)
  have hkcard : (kept.filterMap R.value).eraseDups.length = (N ++ V).eraseDups.length := by
    dsimp [N, V]
    exact eraseDups_length_perm (by simpa [List.filterMap_append] using hk.filterMap R.value)
  have hqeq : ∀ r ∈ qpeers, r = current := by
    intro r hr
    have hrp : r ∈ peers := (List.mem_filter.mp hr).1
    have hri : (r.id == current.id) = true := (List.mem_filter.mp hr).2
    have hrip : r.id = current.id := Nat.eq_of_beq_eq_true hri
    exact huniq r (List.mem_filter.mp hrp).1 hrip
  have hVmem : ∀ x ∈ V, current.value = some x := by
    intro x hx
    have hx' : x ∈ qpeers.filterMap R.value := by simpa [V] using hx
    obtain ⟨r, hrq, hrv⟩ := List.mem_filterMap.mp hx'
    have hr := hqeq r hrq
    rw [hr] at hrv
    exact hrv
  have hVall_of_some : ∀ {v}, current.value = some v → ∀ x ∈ V, x = v := by
    intro v hcv x hx
    have hxv := hVmem x hx
    have hxv' : v = x := by simpa [hcv] using hxv
    exact hxv'.symm
  have hVnil_of_none : current.value = none → V = [] := by
    intro hcv
    cases hV : V with
    | nil => rfl
    | cons x xs =>
        have hxmem : x ∈ V := by simp [hV]
        have hcvx := hVmem x hxmem
        rw [hcv] at hcvx
        contradiction
  have hVnil_of_no_current : ¬ (peers.any (fun r => r.id == current.id)) = true → V = [] := by
    intro hc
    cases hV : V with
    | nil => rfl
    | cons x xs =>
        have hxmem : x ∈ V := by simp [hV]
        have hx' : x ∈ qpeers.filterMap R.value := by simpa [V] using hxmem
        obtain ⟨r, hrq, hrv⟩ := List.mem_filterMap.mp hx'
        have hrp : r ∈ peers := (List.mem_filter.mp hrq).1
        have hri : (r.id == current.id) = true := (List.mem_filter.mp hrq).2
        have hany : (peers.any (fun r => r.id == current.id)) = true := by
          rw [List.any_eq_true]
          exact ⟨r, hrp, hri⟩
        exact False.elim (hc hany)
  have hVperm_of_some : ∀ {v}, current.value = some v →
      (peers.any (fun r => r.id == current.id)) = true → V.eraseDups.Perm [v] := by
    intro v hcv hc
    apply perm_singleton_of_nodup_all_eq (l := V.eraseDups) (v := v) (nodup_eraseDups V)
    · have hmemv : v ∈ V := by
        dsimp [V, qpeers]
        apply List.mem_filterMap.mpr
        refine ⟨current, ?_, ?_⟩
        · rw [List.mem_filter]
          constructor
          · rw [List.any_eq_true] at hc
            obtain ⟨r, hrp, hri⟩ := hc
            have hrpframe : r ∈ frame := (List.mem_filter.mp hrp).1
            have hrip : r.id = current.id := Nat.eq_of_beq_eq_true hri
            have hr : r = current := huniq r hrpframe hrip
            dsimp [peers]
            rw [List.mem_filter]
            constructor
            · simpa [hr] using hrpframe
            · simp
          · simp
        · simp [hcv]
      exact ⟨v, List.mem_eraseDups.mpr hmemv⟩
    · intro x hx
      exact hVall_of_some hcv x (List.mem_eraseDups.mp hx)
  have hkeep_eq : ((peers.any (fun r => r.id == current.id) &&
        current.value.any E.contains).toNat) =
      (V.eraseDups.filter (fun x => ! decide (x ∈ N))).length := by
    by_cases hc : (peers.any (fun r => r.id == current.id)) = true
    · cases hcv : current.value with
      | none =>
          have hVnil := hVnil_of_none hcv
          simp [hc, hcv, hVnil]
      | some v =>
          have hpv : v ∈ P := by
            dsimp [P, peers]
            apply List.mem_filterMap.mpr
            refine ⟨current, ?_, ?_⟩
            · rw [List.mem_filter]
              constructor
              · rw [List.any_eq_true] at hc
                obtain ⟨r, hrp, hri⟩ := hc
                have hrpframe : r ∈ frame := (List.mem_filter.mp hrp).1
                have hrip : r.id = current.id := Nat.eq_of_beq_eq_true hri
                have hr : r = current := huniq r hrpframe hrip
                simpa [hr] using hrpframe
              · simp
            · simp [hcv]
          have hEv : v ∈ E ↔ v ∉ N := by
            rw [hE]
            simp [List.mem_eraseDups, hpv]
          have hlen := filter_not_mem_length_of_perm_singleton (N := N) (hVperm_of_some hcv hc)
          by_cases hvN : v ∈ N
          · have hnotE : ¬ v ∈ E := by simpa [hvN] using hEv
            simp [hc, hcv, hvN, hnotE, hlen]
          · have hE' : v ∈ E := by simpa [hvN] using hEv
            simp [hc, hcv, hvN, hE', hlen]
    · have hVnil := hVnil_of_no_current hc
      simp [hc, hVnil]
  have happ : (N ++ V).eraseDups.length =
      (V.eraseDups.filter (fun x => ! decide (x ∈ N))).length + N.eraseDups.length := by
    calc
      (N ++ V).eraseDups.length = (V ++ N).eraseDups.length :=
        eraseDups_length_perm (perm_append_comm N V)
      _ = (V.eraseDups.filter (fun x => x ∉ N)).length + N.eraseDups.length :=
        eraseDups_append_length V N
  dsimp [estimate, distinctVals]
  change (A.eraseDups.length - E.length +
      ((peers.any (fun r => r.id == current.id) && current.value.any E.contains).toNat)) =
    (kept.filterMap R.value).eraseDups.length
  rw [hAcard, hkcard, happ, ← hkeep_eq]
  omega

end TinylambOpt.ResearchProofs.EngineDecisions.Exec_DistinctExcludeTies

-- C++ references to this model or its proof.
-- executor/detail/window_eval.cpp
